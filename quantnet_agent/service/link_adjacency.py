import asyncio
import json
import logging
from datetime import datetime

from quantnet_mq.rpcclient import RPCClient
from quantnet_agent.hal.link_state_table import (
    LinkStateTable, LinkEntry,
    LINK_DOWN, LINK_INIT, LINK_CONTROL_UP, LINK_QUANTUM_UP,
)

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
        self._client = RPCClient(cid, host=mqhost, port=mqport)
        self._neighbors: list[dict] = []

    def set_hello_interval(self, n: int) -> None:
        self.hello_interval = n

    def set_hold_time(self, n: int) -> None:
        self.hold_time = n
        # propagate to all existing entries
        for entry in self._table.all().values():
            entry.hold_time = n

    def _load_neighbors(self) -> list[dict]:
        """Parse node config JSON and return list of neighbor dicts."""
        with open(self._node_config) as f:
            data = json.load(f)
        neighbors = []
        # QNode / BSMNode / MNode: channels under matterLightInterfaceSettings
        for iface in data.get("matterLightInterfaceSettings", []):
            for ch in iface.get("channels", []):
                n = ch.get("neighbor", {})
                ref = n.get("idRef") or n.get("systemRef")
                if ref and ch.get("direction") == "out":
                    neighbors.append({
                        "neighbor_cid": ref,
                        "channel_id": ch.get("ID", ""),
                        "neighbor_channel_id": n.get("channelRef", ""),
                        "switch_in_path": False,
                        "switch_cid": None,
                    })
        # OpticalSwitch: flat channels array
        for ch in data.get("channels", []):
            n = ch.get("neighbor", {})
            ref = n.get("idRef") or n.get("systemRef")
            if ref and ch.get("direction") == "out":
                neighbors.append({
                    "neighbor_cid": ref,
                    "channel_id": ch.get("ID", ""),
                    "neighbor_channel_id": n.get("channelRef", ""),
                    "switch_in_path": False,
                    "switch_cid": None,
                })
        return neighbors

    def _init_table(self) -> None:
        for nb in self._neighbors:
            if self._table.get(nb["neighbor_cid"]) is None:
                self._table.add(
                    nb["neighbor_cid"],
                    LinkEntry(
                        state=LINK_DOWN,
                        last_hello_sent=None,
                        last_hello_received=None,
                        hold_time=self.hold_time,
                        channel_id=nb["channel_id"],
                        neighbor_channel_id=nb["neighbor_channel_id"],
                        switch_in_path=nb["switch_in_path"],
                        switch_cid=nb["switch_cid"],
                        history=[],
                    ),
                )

    def _check_hold_timers(self) -> None:
        now = datetime.utcnow()
        for neighbor_cid, entry in self._table.all().items():
            if entry.state == LINK_DOWN:
                continue
            if entry.last_hello_received is None:
                continue
            elapsed = (now - entry.last_hello_received).total_seconds()
            if elapsed > entry.hold_time:
                log.warning(
                    "Hold timer expired for %s (%.1fs > %ds) → DOWN",
                    neighbor_cid, elapsed, entry.hold_time,
                )
                self._table.record_transition(neighbor_cid, LINK_DOWN)
                self._publish_state_update(neighbor_cid, LINK_DOWN)

    def _publish_state_update(self, neighbor_cid: str, state: str) -> None:
        # Fire-and-forget publish to monitor topic
        try:
            from quantnet_mq.schema import models
            event = models.link_adjacency.linkStateUpdate(
                eventType="link_state_update",
                src_cid=self._cid,
                dst_cid=neighbor_cid,
                state=state,
                timestamp=datetime.utcnow().isoformat(),
            )
            asyncio.create_task(
                self._client._msgclient.publish("monitor", event.serialize())
            )
        except Exception as e:
            log.debug("Could not publish link_state_update: %s", e)

    async def _send_hello(self, neighbor_cid: str) -> None:
        entry = self._table.get(neighbor_cid)
        if entry is None:
            return
        seen = [
            cid for cid, e in self._table.all().items()
            if e.last_hello_received is not None
        ]
        try:
            from quantnet_mq.schema import models
            msg = models.link_adjacency.linkHello(
                cmd="link.hello",
                src_cid=self._cid,
                dst_cid=neighbor_cid,
                channel_id=entry.channel_id,
                neighbor_channel_id=entry.neighbor_channel_id,
                seen_neighbors=seen,
                hello_interval=self.hello_interval,
                hold_time=self.hold_time,
                timestamp=datetime.utcnow().isoformat(),
            )
            await self._client.call("link.hello", msg.serialize(), target=f"rpc/{neighbor_cid}")
            self._table.update(neighbor_cid, last_hello_sent=datetime.utcnow())
            if entry.state == LINK_DOWN:
                self._table.record_transition(neighbor_cid, LINK_INIT)
        except Exception as e:
            log.debug("Hello to %s failed: %s", neighbor_cid, e)

    async def _probe_if_ready(self) -> None:
        """After each hello loop tick, check if any link is CONTROL_UP and probe it."""
        for neighbor_cid, entry in self._table.all().items():
            if entry.state != LINK_CONTROL_UP:
                continue
            if entry.switch_in_path and entry.switch_cid:
                ok = await self._check_switch(entry.switch_cid, entry.channel_id, entry.neighbor_channel_id)
                if not ok:
                    log.info("Switch port check failed for %s, staying CONTROL_UP", neighbor_cid)
                    continue
            await self._send_probe(neighbor_cid, entry)

    async def _check_switch(self, switch_cid: str, src_ch: str, dst_ch: str) -> bool:
        try:
            from quantnet_mq.schema import models
            msg = models.link_adjacency.linkSwitchPortCheck(
                cmd="link.switchPortCheck",
                src_channel=src_ch,
                dst_channel=dst_ch,
            )
            resp = await self._client.call(
                "link.switchPortCheck", msg.serialize(), target=f"rpc/{switch_cid}"
            )
            return resp.get("status") == "ok"
        except Exception as e:
            log.debug("Switch check failed: %s", e)
            return False

    async def _send_probe(self, neighbor_cid: str, entry: LinkEntry) -> None:
        import uuid
        try:
            from quantnet_mq.schema import models
            msg = models.link_adjacency.linkProbe(
                cmd="link.probe",
                src_cid=self._cid,
                dst_cid=neighbor_cid,
                channel_id=entry.channel_id,
                probe_id=str(uuid.uuid4()),
            )
            resp = await self._client.call(
                "link.probe", msg.serialize(), target=f"rpc/{neighbor_cid}"
            )
            if resp and resp.get("status") == "ok":
                log.info("Probe passed for %s → QUANTUM_UP", neighbor_cid)
                self._table.record_transition(neighbor_cid, LINK_QUANTUM_UP)
                self._publish_state_update(neighbor_cid, LINK_QUANTUM_UP)
        except Exception as e:
            log.debug("Probe to %s failed: %s", neighbor_cid, e)

    async def _hello_loop(self) -> None:
        while self.is_started:
            for neighbor_cid in list(self._table.all().keys()):
                await self._send_hello(neighbor_cid)
            self._check_hold_timers()
            await self._probe_if_ready()
            await asyncio.sleep(self.hello_interval)

    async def start(self) -> None:
        self._neighbors = self._load_neighbors()
        self._init_table()
        await self._client.start()
        # Wait for registration before sending first hello
        while not self.registered:
            await asyncio.sleep(1)
        self.is_started = True
        log.info("LinkAdjacencyManager started, %d neighbor(s)", len(self._neighbors))
        asyncio.create_task(self._hello_loop())

    async def stop(self) -> None:
        self.is_started = False
        log.info("LinkAdjacencyManager stopped")

    async def connect(self, neighbor_cid: str) -> None:
        """Manually trigger adjacency on one link."""
        entry = self._table.get(neighbor_cid)
        if entry and entry.state == LINK_DOWN:
            self._table.record_transition(neighbor_cid, LINK_INIT)
        await self._send_hello(neighbor_cid)

    async def disconnect(self, neighbor_cid: str) -> None:
        """Manually bring one link to DOWN."""
        if self._table.get(neighbor_cid):
            self._table.record_transition(neighbor_cid, LINK_DOWN)
            self._publish_state_update(neighbor_cid, LINK_DOWN)
