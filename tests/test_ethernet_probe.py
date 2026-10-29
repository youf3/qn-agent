"""Unit tests for quantum link probe emulation via DummyLightSrc & DummySignalMeasurement."""
import asyncio
import json
import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from quantnet_agent.hal.hwclasses import LightSrc, SignalMeasurement
from quantnet_agent.hal.driver.dummy import (
    DummyLightSrc,
    DummySignalMeasurement,
    parse_virtual_links,
)
from quantnet_agent.hal.interpreter.link import LinkInterpreter
from quantnet_agent.hal.link_state_table import (
    LinkStateTable,
    LinkEntry,
    link_key,
    LINK_DOWN,
    LINK_CONTROL_UP,
    LINK_QUANTUM_UP,
)
from quantnet_agent.service.link_adjacency import LinkAdjacencyManager
from quantnet_agent.service.repl import AgentREPL, _make_completer
from quantnet_mq.schema.models import Schema

SCHEMA_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "qn-mq",
        "quantnet_mq",
        "schema",
        "rpc",
        "link_adjacency.yaml",
    )
)
if os.path.exists(SCHEMA_PATH):
    Schema.load_schema(SCHEMA_PATH)

TOPOLOGY_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "qn-mq",
        "quantnet_mq",
        "schema",
        "examples",
        "topology",
    )
)
LBNL_Q_CONF = os.path.join(TOPOLOGY_DIR, "conf_lbnl-q.json")
LBNL_SW_CONF = os.path.join(TOPOLOGY_DIR, "conf_lbnl-switch.json")


def _write_pair_configs(tmp_path):
    """Create minimal sender (NodeA) and receiver (NodeB) topology JSON files."""
    node_a = {
        "systemSettings": {"ID": "NodeA", "type": "QNodeSettings"},
        "channels": [
            {
                "ID": "ch_out_1",
                "direction": "out",
                "type": "quantum",
                "wavelength": {"value": 1550.0, "unit": "nm"},
                "power": 10.5,
                "neighbor": {"systemRef": "NodeB", "channelRef": "ch_in_1"},
            },
            {
                "ID": "ch_in_2",
                "direction": "in",
                "type": "quantum",
                "wavelength": {"value": 1310.0, "unit": "nm"},
                "power": -3.0,
                "neighbor": {"systemRef": "NodeB", "channelRef": "ch_out_2"},
            },
        ],
    }
    node_b = {
        "systemSettings": {"ID": "NodeB", "type": "SwitchSettings"},
        "channels": [
            {
                "ID": "ch_in_1",
                "direction": "in",
                "type": "quantum",
                "wavelength": {"value": 1550.0, "unit": "nm"},
                "power": -1.5,
                "neighbor": {"systemRef": "NodeA", "channelRef": "ch_out_1"},
            },
            {
                "ID": "ch_out_2",
                "direction": "out",
                "type": "quantum",
                "wavelength": {"value": 1310.0, "unit": "nm"},
                "power": 8.0,
                "neighbor": {"systemRef": "NodeA", "channelRef": "ch_in_2"},
            },
        ],
    }
    path_a = tmp_path / "node_a.json"
    path_b = tmp_path / "node_b.json"
    path_a.write_text(json.dumps(node_a))
    path_b.write_text(json.dumps(node_b))
    return str(path_a), str(path_b)


def _make_entry(
    state,
    neighbor_cid,
    channel_id,
    neighbor_channel_id,
    direction="out",
    is_quantum=True,
):
    return LinkEntry(
        state=state,
        last_hello_sent=None,
        last_hello_received=None,
        hold_time=30,
        neighbor_cid=neighbor_cid,
        channel_id=channel_id,
        neighbor_channel_id=neighbor_channel_id,
        switch_in_path=False,
        switch_cid=None,
        is_quantum=is_quantum,
        direction=direction,
        history=[],
    )


class TestVirtualLinkCreation:
    """Verify virtual links are created on top of IP following channel directions."""

    def test_parse_virtual_links_real_topology(self):
        cid_q, out_q, in_q = parse_virtual_links(LBNL_Q_CONF, ip="127.0.0.1", base_port=16000)
        cid_sw, out_sw, in_sw = parse_virtual_links(LBNL_SW_CONF, ip="127.0.0.1", base_port=16000)

        assert cid_q == "LBNL-Q"
        assert cid_sw == "LBNL-SWITCH"

        # LBNL-Q channel "1" is direction="out" -> LBNL-SWITCH channel "1" (direction="in")
        vlink_out = out_q.get(("LBNL-SWITCH", "1"))
        vlink_in = in_sw.get(("LBNL-Q", "1"))
        assert vlink_out is not None
        assert vlink_in is not None
        assert vlink_out.vlink_id == "LBNL-Q:1->LBNL-SWITCH:1"
        assert vlink_in.vlink_id == "LBNL-Q:1->LBNL-SWITCH:1"
        assert vlink_out.port == vlink_in.port
        assert vlink_out.direction == "out"
        assert vlink_in.direction == "in"
        assert vlink_out.wavelength == 1550.0

    def test_direction_enforcement_between_lightsrc_and_signalmeasurement(self, tmp_path):
        async def _run():
            path_a, path_b = _write_pair_configs(tmp_path)
            props = {"ip": "127.0.0.1", "base_port": "18000", "window": "0.1"}

            light_a = DummyLightSrc(props, path_a, "localhost", 1883)
            sig_b = DummySignalMeasurement(props, path_b, "localhost", 1883)
            try:
                assert isinstance(light_a, LightSrc)
                assert isinstance(sig_b, SignalMeasurement)

                # NodeA cannot emit probe on its inbound channel ch_in_2
                rc_in = await light_a.generate(
                    "H", channel_id="ch_in_2", dst_cid="NodeB", probe_id="bad-dir"
                )
                assert rc_in == 1

                # NodeB cannot measure probe on its outbound channel ch_out_2
                res_out = await sig_b.measure(
                    src_cid="NodeA", dst_cid="NodeB", channel_id="ch_out_2", probe_id="bad-dir"
                )
                assert res_out["detected"] is False
                assert res_out["reason"] == "invalid_inbound_channel"
            finally:
                await sig_b.cleanUp()

        asyncio.run(_run())


class TestLightSrcAndTimeTaggerEmulation:
    """Verify UDP probe emission by DummyLightSrc and time-tagger detection by DummySignalMeasurement."""

    def test_emit_and_detect_quantum_probe_timetag(self, tmp_path):
        async def _run():
            path_a, path_b = _write_pair_configs(tmp_path)
            props = {"ip": "127.0.0.1", "base_port": "19000", "window": "0.5"}

            light_a = DummyLightSrc(props, path_a, "localhost", 1883)
            sig_b = DummySignalMeasurement(props, path_b, "localhost", 1883)
            try:
                probe_id = str(uuid.uuid4())
                rc = await light_a.generate(
                    "H", channel_id="ch_out_1", dst_cid="NodeB", probe_id=probe_id
                )
                assert rc == 0
                assert light_a.wavelength == 1550.0
                assert light_a.power == 10.5

                result = await sig_b.measure(
                    src_cid="NodeA",
                    dst_cid="NodeB",
                    channel_id="ch_out_1",
                    probe_id=probe_id,
                )
                assert result["detected"] is True
                assert result["counts"] == 1
                assert result["probe_id"] == probe_id
                assert result["channel_id"] == "ch_in_1"
                assert result["wavelength"] == 1550.0
                assert result["power"] == 10.5
                assert result["rx_ts"] >= result["tx_ts"]
            finally:
                await sig_b.cleanUp()

        asyncio.run(_run())

    def test_timetagger_timeout_when_no_pulse_sent(self, tmp_path):
        async def _run():
            _, path_b = _write_pair_configs(tmp_path)
            props = {"ip": "127.0.0.1", "base_port": "19500", "window": "0.08"}

            sig_b = DummySignalMeasurement(props, path_b, "localhost", 1883)
            try:
                result = await sig_b.measure(
                    src_cid="NodeA",
                    dst_cid="NodeB",
                    channel_id="ch_out_1",
                    probe_id="missing-pulse",
                )
                assert result["detected"] is False
                assert result["counts"] == 0
                assert result["reason"] == "timeout"
            finally:
                await sig_b.cleanUp()

        asyncio.run(_run())

    def test_calibration_generate_backward_compatible(self, tmp_path):
        async def _run():
            path_a, _ = _write_pair_configs(tmp_path)
            props = {"ip": "127.0.0.1", "base_port": "19600", "failrate": "0.0"}
            light_a = DummyLightSrc(props, path_a, "localhost", 1883)
            with patch("quantnet_agent.hal.driver.dummy.asyncio.sleep", new_callable=AsyncMock):
                rc = await light_a.generate("V")
                assert rc == 0

        asyncio.run(_run())

    def test_emit_and_detect_ipv6_quantum_probe_timetag(self, tmp_path):
        async def _run():
            path_a, path_b = _write_pair_configs(tmp_path)
            props_a = {
                "ip": "::1",
                "base_port": "19700",
                "peer_ips": "NodeA:2602:fcfb:000d:1::2,NodeB:[::1]",
            }
            props_b = {
                "ip": "[::1]",
                "base_port": "19700",
                "window": "0.5",
                "peer_ips": "NodeA=::1,NodeB=::1",
            }

            light_a = DummyLightSrc(props_a, path_a, "localhost", 1883)
            sig_b = DummySignalMeasurement(props_b, path_b, "localhost", 1883)
            try:
                vlink_out = light_a.get_outbound_vlink("ch_out_1", dst_cid="NodeB")
                assert vlink_out is not None
                assert vlink_out.peer_ip == "::1"

                probe_id = str(uuid.uuid4())
                rc = await light_a.generate(
                    "H", channel_id="ch_out_1", dst_cid="NodeB", probe_id=probe_id
                )
                assert rc == 0

                result = await sig_b.measure(
                    src_cid="NodeA",
                    dst_cid="NodeB",
                    channel_id="ch_out_1",
                    probe_id=probe_id,
                )
                assert result["detected"] is True
                assert result["counts"] == 1
                assert result["probe_id"] == probe_id
                assert result["channel_id"] == "ch_in_1"
            finally:
                await sig_b.cleanUp()

        asyncio.run(_run())


class TestEndToEndLinkProbeProcess:
    """Verify LinkAdjacencyManager and LinkInterpreter use LightSrc and SignalMeasurement."""

    def test_full_probe_handshake_reaches_quantum_up(self, tmp_path):
        async def _run():
            path_a, path_b = _write_pair_configs(tmp_path)
            props = {"ip": "127.0.0.1", "base_port": "20000", "window": "0.5"}

            light_a = DummyLightSrc(props, path_a, "localhost", 1883)
            sig_b = DummySignalMeasurement(props, path_b, "localhost", 1883)
            try:
                # Set up Receiver (NodeB) LinkInterpreter with DummySignalMeasurement
                table_b = LinkStateTable()
                table_b.add(
                    link_key("NodeA", "ch_in_1"),
                    _make_entry(LINK_DOWN, "NodeA", "ch_in_1", "ch_out_1", direction="in"),
                )

                hal_b = MagicMock()
                hal_b._config.cid = "NodeB"
                hal_b._config.node_type = "SwitchSettings"
                hal_b.link_state_table = table_b
                hal_b.devs = {"signalmeasurement": sig_b}
                interp_b = LinkInterpreter(hal_b)

                # Set up Sender (NodeA) LinkAdjacencyManager with DummyLightSrc
                table_a = LinkStateTable()
                key_a = link_key("NodeB", "ch_out_1")
                entry_a = _make_entry(
                    LINK_CONTROL_UP, "NodeB", "ch_out_1", "ch_in_1", direction="out"
                )
                table_a.add(key_a, entry_a)

                hal_a = MagicMock()
                hal_a._config.cid = "NodeA"
                hal_a.link_state_table = table_a
                hal_a.devs = {"lightsource": light_a}

                mgr_a = LinkAdjacencyManager(
                    cid="NodeA",
                    node_config=path_a,
                    mqhost="localhost",
                    mqport=1883,
                    link_state_table=table_a,
                    hal=hal_a,
                )

                async def route_rpc(cmd, payload, topic):
                    assert cmd == "link.probe"
                    assert topic == "link/NodeB"
                    msg = MagicMock()
                    msg.payload.src_cid = payload["src_cid"]
                    msg.payload.dst_cid = payload["dst_cid"]
                    msg.payload.channel_id = payload["channel_id"]
                    msg.payload.probe_id = payload["probe_id"]
                    resp = await interp_b.handle_probe(msg)
                    return {"status": resp.status}

                mgr_a._client.call = AsyncMock(side_effect=route_rpc)

                res = await mgr_a._send_probe(key_a, entry_a)

                assert res["status"] == "ok"
                assert table_a.get(key_a).state == LINK_QUANTUM_UP
                assert table_a.get(key_a).probe_tx_count == 1
                assert table_a.get(key_a).probe_ok_count == 1
                assert table_a.get(key_a).last_probe_status == "ok"
                assert table_a.get(key_a).last_probe_info["wavelength"] == 1550.0

                key_b = link_key("NodeA", "ch_in_1")
                assert table_b.get(key_b).probe_rx_count == 1
                assert table_b.get(key_b).probe_ok_count == 1
                assert table_b.get(key_b).last_probe_status == "ok"
                assert table_b.get(key_b).last_probe_info["detected"] is True
                assert table_b.get(key_b).last_probe_info["counts"] == 1
            finally:
                await sig_b.cleanUp()

        asyncio.run(_run())

    def test_probe_fails_if_optical_pulse_dropped(self, tmp_path):
        async def _run():
            _, path_b = _write_pair_configs(tmp_path)
            props_b = {"ip": "127.0.0.1", "base_port": "20500", "window": "0.08"}
            sig_b = DummySignalMeasurement(props_b, path_b, "localhost", 1883)
            try:
                table_b = LinkStateTable()
                key_b = link_key("NodeA", "ch_in_1")
                table_b.add(
                    key_b,
                    _make_entry(LINK_DOWN, "NodeA", "ch_in_1", "ch_out_1", direction="in"),
                )

                hal_b = MagicMock()
                hal_b._config.cid = "NodeB"
                hal_b._config.node_type = "SwitchSettings"
                hal_b.link_state_table = table_b
                hal_b.devs = {"signalmeasurement": sig_b}
                interp_b = LinkInterpreter(hal_b)

                # Classical RPC arrives at NodeB, but no optical pulse was sent over UDP
                msg = MagicMock()
                msg.payload.src_cid = "NodeA"
                msg.payload.dst_cid = "NodeB"
                msg.payload.channel_id = "ch_out_1"
                msg.payload.probe_id = "dropped-optical-pulse"
                resp = await interp_b.handle_probe(msg)
                assert resp.status == "error"
                assert table_b.get(key_b).probe_rx_count == 1
                assert table_b.get(key_b).probe_fail_count == 1
                assert table_b.get(key_b).last_probe_status == "timeout"
            finally:
                await sig_b.cleanUp()

        asyncio.run(_run())

    def test_sender_skips_rpc_when_lightsrc_fails(self, tmp_path):
        async def _run():
            path_a, _ = _write_pair_configs(tmp_path)
            props_a = {"ip": "127.0.0.1", "base_port": "20800", "failrate": "1.0"}
            light_a = DummyLightSrc(props_a, path_a, "localhost", 1883)

            table_a = LinkStateTable()
            key_a = link_key("NodeB", "ch_out_1")
            entry_a = _make_entry(LINK_CONTROL_UP, "NodeB", "ch_out_1", "ch_in_1", direction="out")
            table_a.add(key_a, entry_a)

            hal_a = MagicMock()
            hal_a._config.cid = "NodeA"
            hal_a.link_state_table = table_a
            hal_a.devs = {"lightsource": light_a}

            mgr_a = LinkAdjacencyManager(
                cid="NodeA",
                node_config=path_a,
                mqhost="localhost",
                mqport=1883,
                link_state_table=table_a,
                hal=hal_a,
            )
            mgr_a._client.call = AsyncMock()

            res = await mgr_a._send_probe(key_a, entry_a)

            assert res["status"] == "tx_failed"
            mgr_a._client.call.assert_not_called()
            assert table_a.get(key_a).state == LINK_CONTROL_UP
            assert table_a.get(key_a).probe_tx_count == 1
            assert table_a.get(key_a).probe_fail_count == 1
            assert table_a.get(key_a).last_probe_status == "tx_failed"

        asyncio.run(_run())


class TestREPLProbeMonitoring:
    """Verify interactive REPL commands for quantum probe monitoring."""

    def test_show_probe_summary_and_detail(self, tmp_path, capsys):
        async def _run():
            path_a, path_b = _write_pair_configs(tmp_path)
            props = {"ip": "127.0.0.1", "base_port": "21200", "window": "0.5"}
            light_a = DummyLightSrc(props, path_a, "localhost", 1883)
            sig_a = DummySignalMeasurement(props, path_a, "localhost", 1883)
            try:
                table_a = LinkStateTable()
                key_out = link_key("NodeB", "ch_out_1")
                key_in = link_key("NodeB", "ch_in_2")
                entry_out = _make_entry(
                    LINK_QUANTUM_UP, "NodeB", "ch_out_1", "ch_in_1", direction="out"
                )
                entry_in = _make_entry(
                    LINK_DOWN, "NodeB", "ch_in_2", "ch_out_2", direction="in"
                )
                table_a.add(key_out, entry_out)
                table_a.add(key_in, entry_in)

                table_a.record_probe(
                    key_out,
                    {
                        "direction": "out",
                        "probe_id": "abc12345",
                        "status": "ok",
                        "vlink": "NodeA:ch_out_1->NodeB:ch_in_1",
                        "peer_ip": "127.0.0.1",
                        "port": 21201,
                        "wavelength": 1550.0,
                        "power": 10.5,
                        "rtt_ms": 2.4,
                        "reason": None,
                    },
                )
                table_a.record_probe(
                    key_in,
                    {
                        "direction": "in",
                        "probe_id": "def67890",
                        "status": "ok",
                        "detected": True,
                        "counts": 1,
                        "tof": 1.2e-03,
                        "wavelength": 1310.0,
                        "power": 8.0,
                        "vlink": "NodeB:ch_out_2->NodeA:ch_in_2",
                        "reason": None,
                    },
                )

                hal_a = MagicMock()
                hal_a._config.cid = "NodeA"
                hal_a.link_state_table = table_a
                hal_a.devs = {"lightsource": light_a, "signalmeasurement": sig_a}

                mgr_a = LinkAdjacencyManager(
                    cid="NodeA",
                    node_config=path_a,
                    mqhost="localhost",
                    mqport=1883,
                    link_state_table=table_a,
                    hal=hal_a,
                )

                repl = AgentREPL("NodeA", mgr_a, table_a)

                # 1. Summary: show probe
                repl._dispatch("show probe")
                out = capsys.readouterr().out
                assert "Hardware Probe Drivers: LightSrc=DummyLightSrc" in out
                assert "SignalMeasurement=DummySignalMeasurement" in out
                assert key_out in out
                assert key_in in out
                assert "TX:1" in out
                assert "RX:1" in out

                # 2. Detail outbound: show probe NodeB:ch_out_1
                repl._dispatch(f"show probe {key_out}")
                out_detail = capsys.readouterr().out
                assert f"Quantum Probe Status: {key_out}" in out_detail
                assert "NodeA:ch_out_1->NodeB:ch_in_1" in out_detail
                assert "wavelength=1550.0 nm, power=10.5 dBm" in out_detail
                assert "abc12345" in out_detail

                # 3. Detail inbound: show probe NodeB:ch_in_2
                repl._dispatch(f"show probe {key_in}")
                in_detail = capsys.readouterr().out
                assert f"Quantum Probe Status: {key_in}" in in_detail
                assert "Photon Counts:     1" in in_detail
                assert "def67890" in in_detail

                # 4. Enriched show link <key>
                repl._dispatch(f"show link {key_out}")
                link_detail = capsys.readouterr().out
                assert "Probe TX / OK / Fail: 1 / 1 / 0" in link_detail
            finally:
                await sig_a.cleanUp()

        asyncio.run(_run())

    def test_interactive_probe_command_and_debug_toggle(self, capsys):
        async def _run():
            table = LinkStateTable()
            key = link_key("NodeB", "ch_out_1")
            entry = _make_entry(LINK_CONTROL_UP, "NodeB", "ch_out_1", "ch_in_1", direction="out")
            table.add(key, entry)

            mgr = MagicMock()
            mgr._send_probe = AsyncMock(
                return_value={
                    "direction": "out",
                    "probe_id": "p1122334",
                    "status": "ok",
                    "vlink": "NodeA:ch_out_1->NodeB:ch_in_1",
                    "wavelength": 1550.0,
                    "power": 10.5,
                    "rtt_ms": 1.8,
                    "reason": None,
                }
            )

            repl = AgentREPL("NodeA", mgr, table)
            await repl._cmd_link_probe(key, ["2"])
            out = capsys.readouterr().out
            assert "[1/2] Running quantum probe" in out
            assert "[2/2] Running quantum probe" in out
            assert "[PASS] probe_id=p1122334" in out
            assert mgr._send_probe.await_count == 2

            repl._dispatch("debug probe")
            assert repl._debug_probe is True
            repl._dispatch("no debug probe")
            assert repl._debug_probe is False

            # Verify tab completer includes show probe & debug probe
            completer = _make_completer(table)
            with patch("readline.get_line_buffer", return_value="show probe "):
                assert completer("NodeB", 0) == key

        asyncio.run(_run())

    def test_render_probe_dashboard_writes_to_curses_window(self):
        table = LinkStateTable()
        key = link_key("NodeB", "ch_out_1")
        entry = _make_entry(LINK_QUANTUM_UP, "NodeB", "ch_out_1", "ch_in_1", direction="out")
        table.add(key, entry)
        table.record_probe(
            key,
            {
                "direction": "out",
                "probe_id": "dash1234",
                "status": "ok",
                "vlink": "NodeA:ch_out_1->NodeB:ch_in_1",
                "wavelength": 1550.0,
                "power": 10.5,
                "rtt_ms": 1.5,
            },
        )
        repl = AgentREPL("NodeA", MagicMock(), table)
        stdscr = MagicMock()
        with patch("curses.color_pair", return_value=0), patch("curses.A_BOLD", 1), patch(
            "curses.A_REVERSE", 2
        ):
            repl._render_probe_dashboard(stdscr, 24, 100, "2026-10-28 15:00:00", {})
        rendered = " ".join(str(call.args[2]) for call in stdscr.addstr.call_args_list if len(call.args) >= 3)
        assert "Quantum Probe Monitor" in rendered
        assert "dash1234" in rendered
