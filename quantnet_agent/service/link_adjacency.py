import asyncio
import json
import logging
import time
import uuid
from datetime import datetime

from quantnet_mq.rpcclient import RPCClient
from quantnet_agent.common.constants import Constants
from quantnet_agent.hal.hwclasses import LightSrc, SignalMeasurement
from quantnet_agent.hal.link_state_table import (
    LinkStateTable, LinkEntry, link_key,
    LINK_DOWN, LINK_INIT, LINK_CONTROL_UP, LINK_QUANTUM_UP,
)

LINK_TOPIC = Constants.LINK_TOPIC_PREFIX

# RPC client handler definitions (cmd, callback, classpath)
_LINK_RPC_HANDLERS = [
    ("link.hello", None, "quantnet_mq.schema.models.link_adjacency.linkHello"),
    ("link.probe", None, "quantnet_mq.schema.models.link_adjacency.linkProbe"),
    ("link.switchPortCheck", None, "quantnet_mq.schema.models.link_adjacency.linkSwitchPortCheck"),
]

log = logging.getLogger(__name__)


class LinkAdjacencyManager:
    def __init__(
        self,
        cid: str,
        node_config: str,
        mqhost: str,
        mqport: int,
        link_state_table: LinkStateTable,
        hello_interval: int = 10,
        hold_time: int = 30,
        node_status_fn=None,
        hal=None,
    ):
        self._cid = cid
        self._node_config = node_config
        self._mqhost = mqhost
        self._mqport = mqport
        self._table = link_state_table
        self.hello_interval = hello_interval
        self.hold_time = hold_time
        self.is_started = False
        self.registered = False
        self._node_status_fn = node_status_fn
        self._hal = hal
        if hal is not None and node_status_fn is not None:
            hal.node_status_fn = node_status_fn
        self._client = RPCClient(f"{cid}-link", host=mqhost, port=mqport)
        self._neighbors: list[dict] = []

    def _is_local_ready(self) -> bool:
        """Return True if local node is IN_SPEC (or no status fn configured)."""
        if self._node_status_fn is None:
            return True
        from quantnet_agent.hal.local_task_manager import NodeState
        try:
            status = self._node_status_fn()
        except Exception:
            return False
        return status is NodeState.in_spec

    def _get_lightsource(self):
        if self._hal is None:
            return None
        devs = getattr(self._hal, "devs", None)
        if not isinstance(devs, dict):
            return None
        dev = devs.get("lightsource")
        return dev if isinstance(dev, LightSrc) else None

    def _get_signalmeasurement(self):
        if self._hal is None:
            return None
        devs = getattr(self._hal, "devs", None)
        if not isinstance(devs, dict):
            return None
        dev = devs.get("signalmeasurement")
        return dev if isinstance(dev, SignalMeasurement) else None

    def set_hello_interval(self, n: int) -> None:
        self.hello_interval = n

    def set_hold_time(self, n: int) -> None:
        self.hold_time = n
        # propagate to all existing entries
        for entry in self._table.all().values():
            entry.hold_time = n

    def _load_neighbors(self) -> list[dict]:
        """Parse node config JSON and return one entry per channel (in and out)."""
        with open(self._node_config) as f:
            data = json.load(f)

        raw_channels: list[dict] = []
        for iface in data.get("matterLightInterfaceSettings", []):
            for ch in iface.get("channels", []):
                raw_channels.append(ch)
        for ch in data.get("channels", []):
            raw_channels.append(ch)

        neighbors = []
        for ch in raw_channels:
            n = ch.get("neighbor", {})
            ref = n.get("systemRef") or n.get("idRef")
            if not ref:
                continue
            ch_type = ch.get("type", "quantum").lower()
            neighbors.append({
                "neighbor_cid": ref,
                "channel_id": ch.get("ID", ""),
                "neighbor_channel_id": n.get("channelRef", ""),
                "switch_in_path": False,
                "switch_cid": None,
                "is_quantum": ch_type == "quantum",
                "direction": ch.get("direction", "out"),
            })
        return neighbors

    def _init_table(self) -> None:
        default_neighbor_ready = self._node_status_fn is None
        for nb in self._neighbors:
            key = link_key(nb["neighbor_cid"], nb["channel_id"])
            if self._table.get(key) is None:
                self._table.add(
                    key,
                    LinkEntry(
                        state=LINK_DOWN,
                        last_hello_sent=None,
                        last_hello_received=None,
                        hold_time=self.hold_time,
                        neighbor_cid=nb["neighbor_cid"],
                        channel_id=nb["channel_id"],
                        neighbor_channel_id=nb["neighbor_channel_id"],
                        switch_in_path=nb["switch_in_path"],
                        switch_cid=nb["switch_cid"],
                        is_quantum=nb["is_quantum"],
                        direction=nb["direction"],
                        history=[],
                        neighbor_ready=default_neighbor_ready,
                    ),
                )

    def _check_hold_timers(self) -> None:
        now = datetime.utcnow()
        for key, entry in self._table.all().items():
            if entry.direction == "in":
                continue  # Inbound channels are passive — no hold timer
            if entry.state == LINK_DOWN:
                continue
            # Outbound entries: check last_neighbor_hello (liveness from any inbound)
            if entry.last_neighbor_hello is not None:
                elapsed = (now - entry.last_neighbor_hello).total_seconds()
            elif entry.init_since is not None:
                # INIT with no neighbor hello: measure from when we entered INIT
                elapsed = (now - entry.init_since).total_seconds()
            else:
                continue
            if elapsed > entry.hold_time:
                log.warning(
                    "Hold timer expired for %s (%.1fs > %ds) → DOWN",
                    key, elapsed, entry.hold_time,
                )
                self._table.record_transition(key, LINK_DOWN)
                self._table.update(
                    key,
                    init_since=None,
                    neighbor_ready=(self._node_status_fn is None),
                )
                self._publish_state_update(key, entry, LINK_DOWN)

    def _publish_state_update(
        self,
        key: str,
        entry: LinkEntry,
        state: str,
        force: bool = False,
        state_changed: bool | None = None,
    ) -> None:
        # Suppress duplicate state updates (e.g., repeated DOWN from INIT timeout cycles)
        if not force and state == entry.last_published_state:
            return
        if state != entry.last_published_state:
            state_changed = True
        elif state_changed is None:
            state_changed = False
        # Fire-and-forget publish to monitor topic using MonitorEvent format
        try:
            from quantnet_mq.schema.models import monitor
            now_iso = datetime.utcnow().isoformat()
            probe_info = dict(entry.last_probe_info) if entry.last_probe_info else {}
            rx_count = probe_info.get("rx_count", entry.probe_rx_count or entry.probe_ok_count)
            probe_history = [
                {
                    "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                    "src_cid": self._cid,
                    "dst_cid": entry.neighbor_cid,
                    "channel_id": entry.channel_id,
                    **(info if isinstance(info, dict) else {}),
                }
                for ts, info in (entry.probe_history or [])[-15:]
            ]
            value = {
                "src_cid": self._cid,
                "dst_cid": entry.neighbor_cid,
                "channel_id": entry.channel_id,
                "neighbor_channel_id": entry.neighbor_channel_id,
                "direction": entry.direction,
                "is_quantum": entry.is_quantum,
                "state": state,
                "state_changed": state_changed,
                "timestamp": now_iso,
                "last_probe_sent": entry.last_probe_sent.isoformat() if entry.last_probe_sent else None,
                "last_probe_received": entry.last_probe_received.isoformat() if entry.last_probe_received else None,
                "last_probe_status": entry.last_probe_status,
                "last_probe_id": entry.last_probe_id,
                "probe_tx_count": entry.probe_tx_count,
                "probe_rx_count": rx_count,
                "probe_ok_count": entry.probe_ok_count,
                "probe_fail_count": entry.probe_fail_count,
                "last_probe_info": probe_info,
                "probe_history": probe_history,
            }
            event = monitor.MonitorEvent(
                rid=self._cid,
                ts=datetime.utcnow().timestamp(),
                eventType="linkStateUpdate",
                value=value,
            )
            mqtt = self._client._mqttclient
            if mqtt:
                mqtt.publish("monitor", event.serialize(), qos=1)
            # Update last published state after successful publish attempt
            entry.last_published_state = state
        except Exception as e:
            log.debug("Could not publish link_state_update: %s", e)

    def _publish_pending_updates(self) -> None:
        """Scan table for state changes that haven't been published yet."""
        for key, entry in self._table.all().items():
            if entry.direction == "in":
                continue
            if entry.state != entry.last_published_state:
                self._publish_state_update(key, entry, entry.state)

    async def _send_hello(self, key: str) -> None:
        entry = self._table.get(key)
        if entry is None:
            return
        if entry.direction == "in":
            return  # Inbound channels are passive — never send hellos
        seen = [
            k for k, e in self._table.all().items()
            if e.last_hello_received is not None
        ]
        try:
            payload = {
                "src_cid": self._cid,
                "dst_cid": entry.neighbor_cid,
                "channel_id": entry.channel_id,
                "neighbor_channel_id": entry.neighbor_channel_id,
                "seen_neighbors": seen,
                "hello_interval": self.hello_interval,
                "hold_time": self.hold_time,
                "timestamp": datetime.utcnow().isoformat(),
                "ready": self._is_local_ready(),
            }
            t0 = time.monotonic()
            resp = await self._client.call(
                "link.hello", payload, topic=f"{LINK_TOPIC}/{entry.neighbor_cid}"
            )
            rtt_ms = round((time.monotonic() - t0) * 1000.0, 2)
            resp = json.loads(resp) if isinstance(resp, (str, bytes)) else resp
            now = datetime.utcnow()
            self._table.update(key, last_hello_sent=now)
            if entry.state == LINK_DOWN:
                self._table.record_transition(key, LINK_INIT)
                self._table.update(key, init_since=now)
            if isinstance(resp, dict) and resp.get("status") == "ok":
                remote_ready = resp.get("ready")
                if isinstance(remote_ready, bool):
                    for k, e in self._table.all().items():
                        if e.neighbor_cid == entry.neighbor_cid and e.direction == "out":
                            self._table.update(k, neighbor_ready=remote_ready)
                has_inbound = any(
                    e.neighbor_cid == entry.neighbor_cid and e.direction == "in"
                    for e in self._table.all().values()
                )
                if not has_inbound:
                    self._table.update(key, last_neighbor_hello=now)
                    if entry.state == LINK_INIT:
                        log.info(
                            "Unidirectional hello ACK confirmed %s → CONTROL_UP",
                            key,
                        )
                        self._table.record_transition(key, LINK_CONTROL_UP)
                        self._table.update(key, init_since=None)
                if not entry.is_quantum and entry.state == LINK_CONTROL_UP:
                    hello_id = str(uuid.uuid4())
                    info = {
                        "direction": "out",
                        "kind": "hello",
                        "key": key,
                        "probe_id": hello_id,
                        "status": "ok",
                        "rtt_ms": rtt_ms,
                        "rx_count": (entry.probe_ok_count or 0) + 1,
                        "state": LINK_CONTROL_UP,
                        "vlink": (
                            f"{self._cid}:{entry.channel_id}"
                            f"->{entry.neighbor_cid}:{entry.neighbor_channel_id}"
                        ),
                    }
                    self._table.record_probe(key, info)
                    self._publish_state_update(
                        key, entry, LINK_CONTROL_UP, force=True, state_changed=False
                    )
        except Exception as e:
            log.warning("Hello to %s failed: %s", key, e)
            if not entry.is_quantum and entry.state == LINK_CONTROL_UP:
                hello_id = str(uuid.uuid4())
                info = {
                    "direction": "out",
                    "kind": "hello",
                    "key": key,
                    "probe_id": hello_id,
                    "status": "rpc_error",
                    "reason": str(e),
                    "rx_count": entry.probe_ok_count or 0,
                    "state": entry.state,
                    "vlink": (
                        f"{self._cid}:{entry.channel_id}"
                        f"->{entry.neighbor_cid}:{entry.neighbor_channel_id}"
                    ),
                }
                self._table.record_probe(key, info)
                self._publish_state_update(
                    key, entry, entry.state, force=True, state_changed=False
                )

    async def _probe_if_ready(self) -> None:
        """After each hello loop tick, probe any outbound quantum link in CONTROL_UP or QUANTUM_UP.

        Quantum probes are only attempted when BOTH the local agent and the
        neighbor agent are IN_SPEC, meaning all calibration tasks have passed
        on both endpoints.
        """
        if not self._is_local_ready():
            log.debug("Skipping quantum probe — local agent not IN_SPEC")
            for key, entry in self._table.all().items():
                if entry.direction == "out" and entry.is_quantum and entry.state == LINK_QUANTUM_UP:
                    self._table.record_transition(key, LINK_CONTROL_UP)
                    self._publish_state_update(key, entry, LINK_CONTROL_UP)
            return
        for key, entry in self._table.all().items():
            if entry.direction == "in":
                continue  # Inbound channels are passive
            if entry.state not in (LINK_CONTROL_UP, LINK_QUANTUM_UP):
                continue
            # Classical links stay at CONTROL_UP; only quantum links attempt probe
            if not entry.is_quantum:
                log.debug("Skipping quantum probe for classical link %s", key)
                continue
            if not entry.neighbor_ready:
                log.debug(
                    "Skipping quantum probe for %s — neighbor %s not ready (not IN_SPEC)",
                    key,
                    entry.neighbor_cid,
                )
                if entry.state == LINK_QUANTUM_UP:
                    self._table.record_transition(key, LINK_CONTROL_UP)
                    self._publish_state_update(key, entry, LINK_CONTROL_UP)
                continue
            if entry.switch_in_path and entry.switch_cid:
                ok = await self._check_switch(entry.switch_cid, entry.channel_id, entry.neighbor_channel_id)
                if not ok:
                    log.info("Switch port check failed for %s, staying CONTROL_UP", key)
                    if entry.state == LINK_QUANTUM_UP:
                        self._table.record_transition(key, LINK_CONTROL_UP)
                        self._publish_state_update(key, entry, LINK_CONTROL_UP)
                    continue
            await self._send_probe(key, entry)

    async def _check_switch(self, switch_cid: str, src_ch: str, dst_ch: str) -> bool:
        try:
            payload = {
                "src_channel": src_ch,
                "dst_channel": dst_ch,
            }
            resp = await self._client.call(
                "link.switchPortCheck", payload, topic=f"{LINK_TOPIC}/{switch_cid}"
            )
            resp = json.loads(resp) if isinstance(resp, (str, bytes)) else resp
            return resp.get("status") == "ok"
        except Exception as e:
            log.warning("Switch check failed: %s", e)
            return False

    async def _send_probe(self, key: str, entry: LinkEntry) -> dict:
        if entry.direction != "out":
            log.warning("Cannot send probe on non-outbound channel %s (direction=%s)", key, entry.direction)
            return {
                "status": "skipped",
                "reason": f"non-outbound channel (direction={entry.direction})",
                "key": key,
                "direction": entry.direction,
            }
        if not entry.is_quantum:
            log.debug("Skipping quantum probe on non-quantum channel %s", key)
            return {
                "status": "skipped",
                "reason": "non-quantum channel",
                "key": key,
                "direction": entry.direction,
            }
        if not self._is_local_ready():
            log.debug("Skipping quantum probe on %s — local node not ready (not IN_SPEC)", key)
            return {
                "status": "skipped",
                "reason": "local_node_not_ready",
                "key": key,
                "direction": entry.direction,
            }
        if not entry.neighbor_ready:
            log.debug(
                "Skipping quantum probe on %s — neighbor %s not ready (not IN_SPEC)",
                key,
                entry.neighbor_cid,
            )
            return {
                "status": "skipped",
                "reason": "neighbor_node_not_ready",
                "key": key,
                "direction": entry.direction,
            }
        vlink_info = {}
        probe_id = str(uuid.uuid4())
        try:
            lightsource = self._get_lightsource()
            if lightsource is not None:
                if hasattr(lightsource, "get_outbound_vlink"):
                    vlink = lightsource.get_outbound_vlink(entry.channel_id, dst_cid=entry.neighbor_cid)
                    if vlink is not None:
                        vlink_info = {
                            "vlink": vlink.vlink_id,
                            "peer_ip": vlink.peer_ip,
                            "port": vlink.port,
                            "wavelength": vlink.wavelength,
                            "power": vlink.power,
                        }
                rc = await lightsource.generate(
                    "H",
                    channel_id=entry.channel_id,
                    dst_cid=entry.neighbor_cid,
                    probe_id=probe_id,
                )
                if rc != 0:
                    log.warning("LightSrc failed to emit probe for %s", key)
                    state_changed = entry.state == LINK_QUANTUM_UP
                    if state_changed:
                        self._table.record_transition(key, LINK_CONTROL_UP)
                    info = {
                        "direction": "out",
                        "key": key,
                        "probe_id": probe_id,
                        "status": "tx_failed",
                        "reason": "lightsource_generate_failed",
                        "state": entry.state,
                        **vlink_info,
                    }
                    self._table.record_probe(key, info)
                    self._publish_state_update(
                        key, entry, entry.state, force=True, state_changed=state_changed
                    )
                    return info

            payload = {
                "src_cid": self._cid,
                "dst_cid": entry.neighbor_cid,
                "channel_id": entry.channel_id,
                "probe_id": probe_id,
            }
            resp = await self._client.call(
                "link.probe", payload, topic=f"{LINK_TOPIC}/{entry.neighbor_cid}"
            )
            resp = json.loads(resp) if isinstance(resp, (str, bytes)) else resp
            remote_data = (
                resp.get("data")
                if isinstance(resp, dict) and isinstance(resp.get("data"), dict)
                else {}
            )
            if resp and resp.get("status") == "ok":
                state_changed = entry.state != LINK_QUANTUM_UP
                if state_changed:
                    log.info("Probe passed for %s → QUANTUM_UP", key)
                    self._table.record_transition(key, LINK_QUANTUM_UP)
                else:
                    log.debug("Periodic probe passed for %s (QUANTUM_UP)", key)
                info = {
                    "direction": "out",
                    "key": key,
                    "probe_id": probe_id,
                    "status": "ok",
                    "state": LINK_QUANTUM_UP,
                    **vlink_info,
                }
                for rk in ("detected", "counts", "tof", "rx_count"):
                    if rk in remote_data and remote_data[rk] is not None:
                        info[rk] = remote_data[rk]
                if not info.get("vlink") and remote_data.get("vlink"):
                    info["vlink"] = remote_data["vlink"]
                self._table.record_probe(key, info)
                self._publish_state_update(
                    key, entry, LINK_QUANTUM_UP, force=True, state_changed=state_changed
                )
                return info

            remote_status = resp.get("status", "error") if isinstance(resp, dict) else "error"
            if remote_status == "not_ready":
                log.info("Neighbor %s not ready for quantum probe on %s — waiting", entry.neighbor_cid, key)
                self._table.update(key, neighbor_ready=False)
                if entry.state == LINK_QUANTUM_UP:
                    self._table.record_transition(key, LINK_CONTROL_UP)
                    self._publish_state_update(key, entry, LINK_CONTROL_UP)
                return {
                    "status": "skipped",
                    "reason": remote_data.get("reason", "neighbor_not_ready"),
                    "key": key,
                    "direction": entry.direction,
                }
            state_changed = entry.state == LINK_QUANTUM_UP
            if state_changed:
                log.warning("Probe failed for %s → demoting to CONTROL_UP", key)
                self._table.record_transition(key, LINK_CONTROL_UP)
            info = {
                "direction": "out",
                "key": key,
                "probe_id": probe_id,
                "status": remote_status,
                "reason": remote_data.get("reason", "remote_rejected"),
                "state": entry.state,
                **vlink_info,
            }
            if "rx_count" in remote_data and remote_data["rx_count"] is not None:
                info["rx_count"] = remote_data["rx_count"]
            self._table.record_probe(key, info)
            self._publish_state_update(
                key, entry, entry.state, force=True, state_changed=state_changed
            )
            return info
        except Exception as e:
            log.warning("Probe to %s failed: %s", key, e)
            state_changed = entry.state == LINK_QUANTUM_UP
            if state_changed:
                self._table.record_transition(key, LINK_CONTROL_UP)
            info = {
                "direction": "out",
                "key": key,
                "probe_id": probe_id,
                "status": "rpc_error",
                "reason": str(e),
                "state": entry.state,
                **vlink_info,
            }
            self._table.record_probe(key, info)
            self._publish_state_update(
                key, entry, entry.state, force=True, state_changed=state_changed
            )
            return info

    async def _hello_loop(self) -> None:
        while self.is_started:
            keys = list(self._table.all().keys())
            for k in keys:
                await self._send_hello(k)
            self._check_hold_timers()
            await self._probe_if_ready()
            self._publish_pending_updates()
            await asyncio.sleep(self.hello_interval)

    async def start(self) -> None:
        self._neighbors = self._load_neighbors()
        self._init_table()
        for cmd, cb, classpath in _LINK_RPC_HANDLERS:
            self._client.set_handler(cmd, cb, classpath)
        await self._client.start()
        # Wait for registration before sending first hello
        while not self.registered:
            await asyncio.sleep(1)
        self.is_started = True
        log.info("LinkAdjacencyManager started, %d link(s)", len(self._neighbors))
        asyncio.create_task(self._hello_loop())

    async def stop(self) -> None:
        self.is_started = False
        log.info("LinkAdjacencyManager stopped")

    async def connect(self, key: str) -> None:
        """Manually trigger adjacency on one link."""
        entry = self._table.get(key)
        if entry and entry.state == LINK_DOWN:
            self._table.record_transition(key, LINK_INIT)
            self._table.update(key, init_since=datetime.utcnow())
        await self._send_hello(key)

    async def disconnect(self, key: str) -> None:
        """Manually bring one link to DOWN."""
        entry = self._table.get(key)
        if entry:
            self._table.record_transition(key, LINK_DOWN)
            self._table.update(key, init_since=None)
            self._publish_state_update(key, entry, LINK_DOWN)
