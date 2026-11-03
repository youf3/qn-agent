from quantnet_agent.hal.hwclasses import (
    LightSrc,
    Filter,
    LightMeasurement,
    SignalMeasurement,
    ExpFramework,
)
from quantnet_mq.schema.models import experiment
from collections import deque
from dataclasses import dataclass
import asyncio
import ipaddress
import json
import logging
import os
import random
import socket
import time
import zlib

log = logging.getLogger(__name__)

DEFAULT_DUMMY_RESULTS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data",
    "dummy_results.json",
)
POL_TRACKING_CHANNELS = ("Bob_H1", "Bob_D2", "Bob_H2", "Alice_H1", "Alice_D2", "Alice_H2")


def _load_dummy_results(path: str = DEFAULT_DUMMY_RESULTS_FILE) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def replay_pol_tracking(msgclient=None, cid: str = "", exp_id: str = "", interval: float = 0.2) -> dict:
    pol_data = _load_dummy_results().get("pol_tracking", {})
    samples = pol_data.get("samples", [])
    dt = float(interval)
    base_ts = time.time()
    step_dt = dt if dt > 0 else 0.05
    series = []

    for idx, sample in enumerate(samples):
        sample_copy = dict(sample)
        ts = time.time() if dt > 0 else round(base_ts + idx * step_dt, 6)
        series.append({"ts": ts, "pol_tracking": sample_copy})
        if msgclient is not None:
            try:
                await msgclient.publish(
                    "experiment_data",
                    {
                        "cid": str(cid or ""),
                        "expid": str(exp_id or ""),
                        "ts": ts,
                        "data": {"pol_tracking": sample_copy},
                    },
                )
            except Exception as exc:
                log.warning("Failed to publish pol_tracking sample to experiment_data: %s", exc)
        if dt > 0 and idx < len(samples) - 1:
            await asyncio.sleep(dt)

    return {
        "status": "done",
        **{k: v for k, v in pol_data.items() if k != "samples"},
        "pol_tracking_series": series,
    }


@dataclass
class VirtualLinkEndpoint:
    """Directional virtual link endpoint created on top of the configured emulation IP."""

    vlink_id: str
    direction: str
    local_ch: str
    src_cid: str
    src_ch: str
    dst_cid: str
    dst_ch: str
    local_ip: str
    peer_ip: str
    port: int
    wavelength: float = 1550.0
    power: float = 0.0
    ch_type: str = "quantum"


def _extract_float(raw, default: float) -> float:
    val = raw.get("value", default) if isinstance(raw, dict) else raw
    if isinstance(val, (int, float, str)):
        try:
            return float(val)
        except ValueError:
            return default
    return default


def _normalize_ip(ip_addr: str) -> str:
    s = str(ip_addr).strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    return s


def _udp_socket_family(ip_addr: str) -> socket.AddressFamily:
    try:
        addr = ipaddress.ip_address(_normalize_ip(ip_addr).split("%", 1)[0])
        return socket.AF_INET6 if addr.version == 6 else socket.AF_INET
    except ValueError:
        return socket.AF_INET6 if ":" in str(ip_addr) else socket.AF_INET


def _parse_peer_ips(raw_peer_ips) -> dict:
    if not raw_peer_ips:
        return {}
    if isinstance(raw_peer_ips, dict):
        return {str(k).strip(): _normalize_ip(v) for k, v in raw_peer_ips.items()}
    peer_map = {}
    items = raw_peer_ips if isinstance(raw_peer_ips, list) else str(raw_peer_ips).split(",")
    for item in items:
        item_str = str(item).strip()
        if not item_str:
            continue
        if "=" in item_str:
            cid, ip_addr = item_str.split("=", 1)
            peer_map[cid.strip()] = _normalize_ip(ip_addr)
        elif ":" in item_str:
            cid, ip_addr = item_str.split(":", 1)
            peer_map[cid.strip()] = _normalize_ip(ip_addr)
    return peer_map


def parse_virtual_links(
    node_file,
    ip: str = "127.0.0.1",
    base_port: int = 15000,
    port_range: int = 10000,
    peer_ips=None,
):
    """Parse a node definition JSON and construct outbound and inbound virtual links on top of `ip`."""
    ip = _normalize_ip(ip)
    if not node_file:
        return "", {}, {}

    if isinstance(node_file, dict):
        data = node_file
    elif isinstance(node_file, str) and os.path.isfile(node_file):
        with open(node_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        return "", {}, {}

    sys_settings = data.get("systemSettings", {})
    local_cid = str(sys_settings.get("ID") or sys_settings.get("cid") or "")
    if not local_cid:
        return "", {}, {}

    peer_ip_map = _parse_peer_ips(peer_ips)
    raw_channels = []
    for mli in data.get("matterLightInterfaceSettings", []):
        if isinstance(mli, dict):
            raw_channels.extend(mli.get("channels", []))
    raw_channels.extend(data.get("channels", []))

    outbound = {}
    inbound = {}
    seen = set()

    for ch in raw_channels:
        if not isinstance(ch, dict):
            continue
        local_ch = str(ch.get("ID") or "")
        direction = str(ch.get("direction") or "")
        ch_type = str(ch.get("type") or "quantum")
        neighbor = ch.get("neighbor")
        if not local_ch or direction not in ("out", "in") or not isinstance(neighbor, dict):
            continue

        neighbor_cid = str(neighbor.get("systemRef") or neighbor.get("idRef") or "")
        neighbor_ch = str(neighbor.get("channelRef") or "")
        if not neighbor_cid or not neighbor_ch:
            continue

        dedup_key = (direction, local_ch, neighbor_cid, neighbor_ch)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        if direction == "out":
            src_cid, src_ch = local_cid, local_ch
            dst_cid, dst_ch = neighbor_cid, neighbor_ch
        else:
            src_cid, src_ch = neighbor_cid, neighbor_ch
            dst_cid, dst_ch = local_cid, local_ch

        vlink_id = f"{src_cid}:{src_ch}->{dst_cid}:{dst_ch}"
        port = base_port + (zlib.crc32(vlink_id.encode("utf-8")) % port_range)
        peer_ip = peer_ip_map.get(neighbor_cid, ip)

        endpoint = VirtualLinkEndpoint(
            vlink_id=vlink_id,
            direction=direction,
            local_ch=local_ch,
            src_cid=src_cid,
            src_ch=src_ch,
            dst_cid=dst_cid,
            dst_ch=dst_ch,
            local_ip=ip,
            peer_ip=peer_ip,
            port=port,
            wavelength=_extract_float(ch.get("wavelength"), 1550.0),
            power=_extract_float(ch.get("power"), 0.0),
            ch_type=ch_type,
        )
        if direction == "out":
            outbound[(dst_cid, local_ch)] = endpoint
        else:
            inbound[(src_cid, src_ch)] = endpoint

    return local_cid, outbound, inbound


class DummyLightSrc(LightSrc):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        props = args[0] if len(args) > 0 and isinstance(args[0], dict) else {}
        node_file = args[1] if len(args) > 1 else None
        self._power = 0
        self._wavelength = 0
        self._status = 0
        self._failrate = float(props.get("failrate", 0))
        self._ip = _normalize_ip(props.get("ip", "127.0.0.1"))
        self._base_port = int(props.get("base_port", 15000))
        self._port_range = int(props.get("port_range", 10000))
        self._cid, self._outbound_links, _ = parse_virtual_links(
            node_file,
            ip=self._ip,
            base_port=self._base_port,
            port_range=self._port_range,
            peer_ips=props.get("peer_ips"),
        )
        log.info(
            "Initializing DummyLightSrc (ip=%s, base_port=%d, outbound_vlinks=%d)",
            self._ip,
            self._base_port,
            len(self._outbound_links),
        )

    @property
    def wavelength(self):
        return self._wavelength

    @wavelength.setter
    def wavelength(self, freq):
        log.info(f"Setting wavelength to {freq}")
        self._wavelength = freq

    @property
    def power(self):
        return self._power

    @power.setter
    def power(self, power):
        log.info(f"Setting power to {power}")
        self._power = power

    def get_outbound_vlink(self, channel_id: str, dst_cid: str = None):
        ch_str = str(channel_id)
        if dst_cid is not None:
            vlink = self._outbound_links.get((str(dst_cid), ch_str))
            if vlink is not None:
                return vlink
        for (_, local_ch), vlink in self._outbound_links.items():
            if local_ch == ch_str:
                return vlink
        return None

    async def generate(self, pol="H", channel_id=None, dst_cid=None, probe_id=None):
        if channel_id is None and probe_id is None:
            log.info(f"Generating {pol} light with to power = {self._power}, wavelength = {self._wavelength}")
            if random.random() < self._failrate:
                log.error("Light source failed")
                return 1
            else:
                log.info("Calibrating for 10 sec")
                await asyncio.sleep(10)
                return 0

        vlink = self.get_outbound_vlink(channel_id=str(channel_id), dst_cid=dst_cid)
        if vlink is None:
            log.warning(
                "DummyLightSrc: no outbound ('out') virtual link for channel=%s dst=%s on %s",
                channel_id,
                dst_cid,
                self._cid,
            )
            return 1

        if random.random() < self._failrate:
            log.error("DummyLightSrc failed to emit probe on %s", vlink.vlink_id)
            return 1

        self.wavelength = vlink.wavelength
        self.power = vlink.power
        payload = {
            "vlink": vlink.vlink_id,
            "probe_id": str(probe_id),
            "src_cid": vlink.src_cid,
            "dst_cid": vlink.dst_cid,
            "src_ch": vlink.src_ch,
            "dst_ch": vlink.dst_ch,
            "wavelength": self._wavelength,
            "power": self._power,
            "pol": pol,
            "tx_ts": time.time(),
        }
        data = json.dumps(payload).encode("utf-8")
        sock = socket.socket(_udp_socket_family(vlink.peer_ip), socket.SOCK_DGRAM)
        try:
            sock.setblocking(False)
            sock.sendto(data, (vlink.peer_ip, vlink.port))
            log.debug(
                "DummyLightSrc emitted quantum probe %s over %s (%s:%d)",
                probe_id,
                vlink.vlink_id,
                vlink.peer_ip,
                vlink.port,
            )
            return 0
        except OSError as exc:
            log.warning("DummyLightSrc UDP send failed on %s: %s", vlink.vlink_id, exc)
            return 1
        finally:
            sock.close()

    async def cleanUp(self):
        self._status = 0
        log.info("Cleaning up for 10 sec")
        await asyncio.sleep(10)
        return 0

    @property
    def status(self):
        return self._status


class DummySignalMeasurement(SignalMeasurement):
    """Emulates a single-photon detector + Time-Tagger on inbound ('in') virtual links."""

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        props = args[0] if len(args) > 0 and isinstance(args[0], dict) else {}
        node_file = args[1] if len(args) > 1 else None
        self._status = 0
        self._ip = _normalize_ip(props.get("ip", "127.0.0.1"))
        self._base_port = int(props.get("base_port", 15000))
        self._port_range = int(props.get("port_range", 10000))
        self._window = float(props.get("window", props.get("timeout", 2.0)))
        self._resolution = float(props.get("resolution", 100e-9))
        self._efficiency = float(props.get("efficiency", 1.0))

        self._cid, _, self._inbound_links = parse_virtual_links(
            node_file,
            ip=self._ip,
            base_port=self._base_port,
            port_range=self._port_range,
            peer_ips=props.get("peer_ips"),
        )
        self._sockets = {}
        self._timetags = {}
        self._bind_inbound_sockets()
        log.info(
            "Initializing DummySignalMeasurement time-tagger (ip=%s, base_port=%d, inbound_vlinks=%d, window=%.2fs)",
            self._ip,
            self._base_port,
            len(self._inbound_links),
            self._window,
        )

    def _bind_inbound_sockets(self):
        for key, vlink in self._inbound_links.items():
            self._timetags[key] = deque(maxlen=256)
            sock = socket.socket(_udp_socket_family(vlink.local_ip), socket.SOCK_DGRAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if hasattr(socket, "SO_REUSEPORT"):
                    try:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    except OSError:
                        pass
                sock.setblocking(False)
                sock.bind((vlink.local_ip, vlink.port))
                self._sockets[key] = sock
            except OSError as exc:
                log.warning(
                    "DummySignalMeasurement failed to bind UDP socket for %s on %s:%d: %s",
                    vlink.vlink_id,
                    vlink.local_ip,
                    vlink.port,
                    exc,
                )
                sock.close()

    @property
    def window(self):
        return self._window

    @window.setter
    def window(self, value):
        self._window = float(value)

    @property
    def resolution(self):
        return self._resolution

    def _drain_socket(self, key, vlink: VirtualLinkEndpoint):
        sock = self._sockets.get(key)
        if sock is None:
            return
        buf = self._timetags[key]
        while True:
            try:
                raw, _ = sock.recvfrom(4096)
            except (BlockingIOError, InterruptedError):
                break
            except OSError:
                break

            rx_ts = time.time()
            try:
                msg = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue

            if not isinstance(msg, dict):
                continue
            if msg.get("vlink") != vlink.vlink_id:
                continue
            if str(msg.get("dst_cid")) != vlink.dst_cid:
                continue
            if str(msg.get("src_cid")) != vlink.src_cid or str(msg.get("src_ch")) != vlink.src_ch:
                continue

            tx_ts = float(msg.get("tx_ts", rx_ts))
            buf.append(
                {
                    "probe_id": str(msg.get("probe_id", "")),
                    "vlink": vlink.vlink_id,
                    "src_cid": vlink.src_cid,
                    "dst_cid": vlink.dst_cid,
                    "src_ch": vlink.src_ch,
                    "dst_ch": vlink.dst_ch,
                    "wavelength": float(msg.get("wavelength", vlink.wavelength)),
                    "power": float(msg.get("power", vlink.power)),
                    "pol": msg.get("pol", "H"),
                    "tx_ts": tx_ts,
                    "rx_ts": rx_ts,
                    "tof": max(0.0, rx_ts - tx_ts),
                }
            )

    async def measure(self, src_cid=None, dst_cid=None, channel_id=None, probe_id=None, timeout=None):
        key = (str(src_cid), str(channel_id))
        vlink = self._inbound_links.get(key)
        if vlink is None or (dst_cid is not None and str(dst_cid) != self._cid):
            log.warning(
                "DummySignalMeasurement: no inbound ('in') virtual link for src=%s ch=%s on %s",
                src_cid,
                channel_id,
                self._cid,
            )
            return {
                "detected": False,
                "counts": 0,
                "reason": "invalid_inbound_channel",
            }

        coincidence_window = float(timeout) if timeout is not None else self._window
        deadline = time.monotonic() + coincidence_window
        target_probe = str(probe_id) if probe_id is not None else None
        buf = self._timetags[key]

        while True:
            self._drain_socket(key, vlink)
            now = time.time()
            while buf and (now - buf[0]["rx_ts"]) > max(coincidence_window, self._window):
                buf.popleft()

            for idx, tag in enumerate(buf):
                if target_probe is None or tag["probe_id"] == target_probe:
                    del buf[idx]
                    if random.random() > self._efficiency:
                        return {
                            "detected": False,
                            "counts": 0,
                            "reason": "detector_inefficiency",
                        }
                    return {
                        "detected": True,
                        "counts": 1,
                        "probe_id": tag["probe_id"],
                        "vlink": vlink.vlink_id,
                        "channel_id": vlink.dst_ch,
                        "rx_ts": tag["rx_ts"],
                        "tx_ts": tag["tx_ts"],
                        "tof": tag["tof"],
                        "wavelength": tag["wavelength"],
                        "power": tag["power"],
                    }

            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.02)

        return {
            "detected": False,
            "counts": 0,
            "reason": "timeout",
        }

    async def cleanUp(self):
        self._status = 0
        for sock in self._sockets.values():
            try:
                sock.close()
            except OSError:
                pass
        self._sockets.clear()
        for buf in self._timetags.values():
            buf.clear()
        return 0


class DummyEPC(Filter):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        log.info("EPC initialized")
        return

    async def polarize(self, _):
        log.info("Calibrating link using EPC")
        await asyncio.sleep(5)
        log.info("Link calibrate")
        return 0

    async def attenuate(self, _):
        log.info("Attenuate link using EPC")
        await asyncio.sleep(5)
        log.info("Link attenuated")
        return 0

    async def cleanUp(self):
        self._status = 0
        return 0


class DummyPolarimeter(LightMeasurement):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        props = args[0] if len(args) > 0 and isinstance(args[0], dict) else {}
        node_file = args[1] if len(args) > 1 else None
        self._wavelength = 0
        self._power = 0
        self._status = 0
        self._msgclient = None
        self._hal = None
        cid, _, _ = parse_virtual_links(node_file)
        self._cid = cid or str(props.get("cid", ""))
        self._sample_interval = float(props.get("sample_interval", 0.2))
        self.last_calibration_result = None
        log.info("Initializing DummyPolarimeter with wavelength=%s, cid=%s", self._wavelength, self._cid)

    @property
    def wavelength(self):
        return self._wavelength

    @wavelength.setter
    def wavelength(self, freq):
        log.info(f"Setting wavelength to {freq}")
        self._wavelength = freq

    @property
    def power(self):
        return self._power

    @property
    def status(self):
        return self._status

    def sweep(self):
        pass

    async def measure(self, exp_id: str = "", interval=None):
        dt = self._sample_interval if interval is None else float(interval)
        log.info("Replaying polarization tracking data (cid=%s, exp_id=%s)", self._cid, exp_id)
        self.last_calibration_result = await replay_pol_tracking(
            msgclient=self._msgclient,
            cid=self._cid,
            exp_id=exp_id,
            interval=dt,
        )
        return 0

    async def cleanUp(self):
        self._status = 0
        return 0


class DummyExpFramework(ExpFramework):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        props = args[0] if len(args) > 0 and isinstance(args[0], dict) else {}
        node_file = args[1] if len(args) > 1 else None
        log.info("Initializing ExpFramework")
        self.result_ind = 0
        self._status = 0
        self._msgclient = None
        self._hal = None
        cid, _, _ = parse_virtual_links(node_file)
        self._cid = cid or str(props.get("cid", ""))
        self._sample_interval = float(props.get("sample_interval", 0.2))
        self._dummy_results = _load_dummy_results()
        self._experiments = {}

    def _init_result(self, exp_type: str) -> dict:
        template = dict(self._dummy_results.get(exp_type, {}))
        if exp_type == "local_calibration":
            scans = template.get("results", [])
            template["results"] = scans[self.result_ind % len(scans)] if scans else {}
            self.result_ind += 1
        return {
            "status": "done",
            "exp_type": exp_type,
            "node": self._cid or "unknown",
            "steps": [],
            **template,
        }

    async def _run_calibration_step(self, state, exp_name: str, params: dict):
        res = json.loads(str(state.result))
        if exp_name in ("calibration.srcInit", "calibration.generation"):
            res["role"] = "src"
            state.result = json.dumps(res)
            if exp_name == "calibration.generation" and self._hal and "lightsource" in self._hal.devs:
                if "power" in params:
                    self._hal.devs["lightsource"].power = params["power"]
        elif exp_name == "calibration.dstInit":
            res["role"] = "dst"
            state.result = json.dumps(res)
        elif exp_name == "calibration.calibration":
            res["role"] = "dst"
            state.result = json.dumps(res)
            polarimeter = self._hal.devs.get("polarimeter") if self._hal else None
            if polarimeter is not None:
                polarimeter._msgclient = polarimeter._msgclient or self._msgclient
                polarimeter._cid = polarimeter._cid or self._cid
                await polarimeter.measure(exp_id=str(state.id))
                cal_res = polarimeter.last_calibration_result or {}
            else:
                cal_res = await replay_pol_tracking(
                    self._msgclient, self._cid, str(state.id), self._sample_interval
                )
            res = json.loads(str(state.result))
            res.update(cal_res)
            state.result = json.dumps(res)
            if str(state.phase) == "cleanup":
                state.phase = "done"
                state.end_ts = time.time()
            else:
                state.phase = "measured"
        elif exp_name == "calibration.cleanUp":
            if res.get("role") != "dst" or str(state.phase) == "measured":
                state.phase = "done"
                state.end_ts = time.time()
            else:
                state.phase = "cleanup"

    async def submit(self, exp_id, expName, classname, args=None):
        exp_key = str(getattr(exp_id, "_value", exp_id) or "")
        exp_name = str(expName or "")
        params = dict(args or {})
        exp_type = str(params.pop("exp_type", None) or "generic")
        log.info(f"Submitting exp:{exp_name} with id {exp_key} (type={exp_type}) with args:{params}")

        if not exp_key:
            return 0

        state = self._experiments.get(exp_key)
        if state is None:
            state = experiment.experimentRecord(
                id=exp_key,
                phase="started",
                start_ts=time.time(),
                result=json.dumps(self._init_result(exp_type)),
            )
            self._experiments[exp_key] = state

        res = json.loads(str(state.result))
        if exp_name:
            res["steps"].append(exp_name)
        if params:
            res.setdefault("parameters", {}).update(params)

        if res["exp_type"] == "calibration":
            state.result = json.dumps(res)
            await self._run_calibration_step(state, exp_name, params)
        else:
            if res["exp_type"] == "dqc":
                if exp_name == "entanglement_prefill":
                    res["prefill_blocks_executed"] += 1
                elif exp_name.startswith("Block_"):
                    res["gate_blocks_executed"] += 1
                else:
                    res["entanglement_blocks_executed"] += 1
            state.result = json.dumps(res)
            state.phase = "done"
            state.end_ts = time.time()
        return 0

    async def receive(self, exp_id, *args, **kwargs):
        exp_key = str(getattr(exp_id, "_value", exp_id) or "")
        log.info(f"Receiving result from exp:{exp_key}")
        state = self._experiments.get(exp_key)
        if state is None:
            return {}
        deadline = time.monotonic() + 30.0
        while str(state.phase) != "done":
            if time.monotonic() >= deadline:
                log.warning("Timed out waiting for exp %s to finish all steps", exp_key)
                break
            await asyncio.sleep(0.01)
        return json.loads(str(state.result))

    @property
    def logs(self):
        log.info("Getting log from ExpFramework")
        return "Dummy logs"

    async def cleanUp(self):
        self._status = 0
        return 0
