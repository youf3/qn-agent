from quantnet_agent.hal.hwclasses import (
    LightSrc,
    Filter,
    LightMeasurement,
    SignalMeasurement,
    ExpFramework,
)
from collections import deque
from dataclasses import dataclass
import asyncio
import json
import logging
import os
import random
import socket
import time
import zlib

log = logging.getLogger(__name__)


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


def _extract_wavelength(raw_wl) -> float:
    if isinstance(raw_wl, dict):
        val = raw_wl.get("value", 1550.0)
        return float(val) if val is not None else 1550.0
    if isinstance(raw_wl, (int, float, str)):
        try:
            return float(raw_wl)
        except ValueError:
            return 1550.0
    return 1550.0


def _extract_power(raw_power) -> float:
    if isinstance(raw_power, dict):
        val = raw_power.get("value", 0.0)
        return float(val) if val is not None else 0.0
    if isinstance(raw_power, (int, float, str)):
        try:
            return float(raw_power)
        except ValueError:
            return 0.0
    return 0.0


def _parse_peer_ips(raw_peer_ips) -> dict:
    if not raw_peer_ips:
        return {}
    if isinstance(raw_peer_ips, dict):
        return {str(k): str(v) for k, v in raw_peer_ips.items()}
    peer_map = {}
    items = raw_peer_ips if isinstance(raw_peer_ips, list) else str(raw_peer_ips).split(",")
    for item in items:
        item_str = str(item).strip()
        if ":" in item_str:
            cid, ip_addr = item_str.split(":", 1)
            peer_map[cid.strip()] = ip_addr.strip()
    return peer_map


def parse_virtual_links(
    node_file,
    ip: str = "127.0.0.1",
    base_port: int = 15000,
    port_range: int = 10000,
    peer_ips=None,
):
    """Parse a node definition JSON and construct outbound and inbound virtual links on top of `ip`."""
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
            wavelength=_extract_wavelength(ch.get("wavelength")),
            power=_extract_power(ch.get("power")),
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
        self._ip = str(props.get("ip", "127.0.0.1"))
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
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
        self._ip = str(props.get("ip", "127.0.0.1"))
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
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
        self._wavelength = 0
        log.info(f"Initializing LightMeasurement with wavelength={self._wavelength}")
        return

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

    def sweep(self):
        pass

    async def measure(self):
        log.info("Measuring SOP")
        for i in range(5):
            log.info(f"Generating random SOPs :[{random.random()},{random.random()},{random.random()}]")
            await asyncio.sleep(1)
        log.info("reached threashold SOP :[0.995, 0.995, 0.995]")
        return 0

    async def cleanUp(self):
        self._status = 0
        return 0


class DummyExpFramework(ExpFramework):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        log.info("Initializing ExpFramework")
        self.result_ind = 0
        return

    async def submit(self, exp_id, expName, classname, args=dict()):
        log.info(f"Submitting exp:{expName} with id {id} with args:{args} for 10 sec")
        # await asyncio.sleep(10)
        return 0

    async def receive(self, *args, **kwargs):
        exp_id = args[0]
        log.info(f"Receiving result from exp:{exp_id}")
        results = [
            {
                "current_scan.plots.x": [
                    170000000.0,
                    172068965.5172414,
                    174137931.03448275,
                    176206896.55172414,
                    178275862.06896552,
                    180344827.58620688,
                    182413793.10344827,
                    184482758.62068966,
                    186551724.13793105,
                    188620689.6551724,
                    190689655.1724138,
                    192758620.68965518,
                    194827586.20689654,
                    196896551.72413793,
                    198965517.24137932,
                    201034482.75862068,
                    203103448.27586207,
                    205172413.79310346,
                    207241379.31034482,
                    209310344.8275862,
                    211379310.3448276,
                    213448275.86206895,
                    215517241.37931034,
                    217586206.89655173,
                    219655172.4137931,
                    221724137.93103448,
                    223793103.44827586,
                    225862068.96551722,
                    227931034.4827586,
                    230000000.0,
                ],
                "current_scan.plots.y": [
                    [1896.0],
                    [1715.0],
                    [1913.0],
                    [1844.0],
                    [2061.0],
                    [1977.0],
                    [1936.0],
                    [2039.0],
                    [1937.0],
                    [2319.0],
                    [2458.0],
                    [2743.0],
                    [2943.0],
                    [3469.0],
                    [4059.0],
                    [4817.0],
                    [5377.0],
                    [5807.0],
                    [6039.0],
                    [6113.0],
                    [4882.0],
                    [3784.0],
                    [3607.0],
                    [4132.0],
                    [4200.0],
                    [4517.0],
                    [4693.0],
                    [5064.0],
                    [5146.0],
                    [2963.0],
                ],
            },
            {
                "current_scan.plots.x": [
                    30.0,
                    28.94736842105263,
                    27.894736842105264,
                    26.842105263157894,
                    25.789473684210527,
                    24.736842105263158,
                    23.684210526315788,
                    22.63157894736842,
                    21.578947368421055,
                    20.526315789473685,
                    19.473684210526315,
                    18.42105263157895,
                    17.36842105263158,
                    16.315789473684212,
                    15.263157894736842,
                    14.210526315789474,
                    13.157894736842106,
                    12.105263157894736,
                    11.05263157894737,
                    10.0,
                ],
                "current_scan.plots.y": [
                    [-2.0],
                    [-26.0],
                    [15.0],
                    [4.0],
                    [-31.0],
                    [-14.0],
                    [69.0],
                    [63.0],
                    [149.0],
                    [177.0],
                    [494.0],
                    [576.0],
                    [1112.0],
                    [2253.0],
                    [2947.0],
                    [3776.0],
                    [4876.0],
                    [5963.0],
                    [7261.0],
                    [8522.0],
                ],
            },
            {
                "current_scan.plots.x": [
                    60000000.0,
                    61379310.344827585,
                    62758620.68965517,
                    64137931.03448276,
                    65517241.37931035,
                    66896551.72413793,
                    68275862.06896552,
                    69655172.4137931,
                    71034482.7586207,
                    72413793.10344827,
                    73793103.44827586,
                    75172413.79310346,
                    76551724.13793103,
                    77931034.48275863,
                    79310344.8275862,
                    80689655.1724138,
                    82068965.51724139,
                    83448275.86206897,
                    84827586.20689656,
                    86206896.55172414,
                    87586206.89655173,
                    88965517.24137932,
                    90344827.5862069,
                    91724137.93103449,
                    93103448.27586207,
                    94482758.62068966,
                    95862068.96551725,
                    97241379.31034483,
                    98620689.65517241,
                    100000000.0,
                ],
                "current_scan.plots.y": [
                    [3034.0],
                    [3063.0],
                    [3198.0],
                    [3290.0],
                    [3017.0],
                    [3097.0],
                    [3066.0],
                    [3188.0],
                    [3201.0],
                    [3348.0],
                    [3278.0],
                    [3243.0],
                    [3223.0],
                    [3145.0],
                    [3238.0],
                    [3159.0],
                    [3193.0],
                    [3328.0],
                    [3227.0],
                    [3342.0],
                    [3338.0],
                    [3433.0],
                    [3293.0],
                    [3261.0],
                    [3205.0],
                    [3437.0],
                    [3306.0],
                    [3325.0],
                    [3359.0],
                    [3394.0],
                ],
            },
            {
                "current_scan.plots.x": [
                    30.0,
                    28.94736842105263,
                    27.894736842105264,
                    26.842105263157894,
                    25.789473684210527,
                    24.736842105263158,
                    23.684210526315788,
                    22.63157894736842,
                    21.578947368421055,
                    20.526315789473685,
                    19.473684210526315,
                    18.42105263157895,
                    17.36842105263158,
                    16.315789473684212,
                    15.263157894736842,
                    14.210526315789474,
                    13.157894736842106,
                    12.105263157894736,
                    11.05263157894737,
                    10.0,
                ],
                "current_scan.plots.y": [
                    [3514.0],
                    [3372.0],
                    [3508.0],
                    [3575.0],
                    [3508.0],
                    [3536.0],
                    [3671.0],
                    [3489.0],
                    [3597.0],
                    [3494.0],
                    [3596.0],
                    [3576.0],
                    [3661.0],
                    [3721.0],
                    [3531.0],
                    [3609.0],
                    [3596.0],
                    [3626.0],
                    [3681.0],
                    [3543.0],
                ],
            },
        ]
        index = self.result_ind % 4
        self.result_ind += 1
        return {"dummy status": "done", "results": results[index]}

    @property
    def logs(self):
        log.info("Getting log from ExpFramework")
        return "Dummy logs"

    async def cleanUp(self):
        self._status = 0
        return 0
