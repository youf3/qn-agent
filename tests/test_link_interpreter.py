import asyncio
import os
from unittest.mock import MagicMock
from quantnet_agent.hal.interpreter.link import LinkInterpreter
from quantnet_agent.hal.link_state_table import (
    LinkStateTable, LinkEntry,
    LINK_DOWN, LINK_INIT, LINK_CONTROL_UP,
)

# Load link_adjacency schema
from quantnet_mq.schema.models import Schema
schema_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "qn-mq/quantnet_mq/schema/rpc/link_adjacency.yaml"
)
if os.path.exists(schema_path):
    Schema.load_schema(schema_path)


def make_hal(node_type="QNode"):
    hal = MagicMock()
    hal.config.node_type = node_type
    hal.link_state_table = LinkStateTable()
    return hal


def make_entry(state=LINK_DOWN):
    return LinkEntry(
        state=state,
        last_hello_sent=None,
        last_hello_received=None,
        hold_time=30,
        channel_id="1",
        neighbor_channel_id="4",
        switch_in_path=False,
        switch_cid=None,
        history=[],
    )


SRC = "urn:quant-net:LBNL-BSM:alice:4"
MY_CID = "urn:quant-net:LBNL-Q:alice:1"


def test_qnode_registers_hello_and_probe():
    interp = LinkInterpreter(make_hal("QNode"))
    cmds = interp.get_commands()
    assert "link.hello" in cmds
    assert "link.probe" in cmds
    assert "link.switchPortCheck" not in cmds


def test_bsmnode_registers_hello_and_probe():
    interp = LinkInterpreter(make_hal("BSMNode"))
    cmds = interp.get_commands()
    assert "link.hello" in cmds
    assert "link.probe" in cmds


def test_mnode_registers_hello_and_probe():
    interp = LinkInterpreter(make_hal("MNode"))
    cmds = interp.get_commands()
    assert "link.hello" in cmds
    assert "link.probe" in cmds


def test_opticalswitch_registers_switch_port_check_only():
    interp = LinkInterpreter(make_hal("OpticalSwitch"))
    cmds = interp.get_commands()
    assert "link.switchPortCheck" in cmds
    assert "link.hello" not in cmds
    assert "link.probe" not in cmds


def test_handle_hello_updates_last_received():
    hal = make_hal("QNode")
    hal.link_state_table.add(SRC, make_entry(LINK_DOWN))
    interp = LinkInterpreter(hal)

    msg = MagicMock()
    msg.src_cid = SRC
    msg.seen_neighbors = []  # our CID not in list yet

    asyncio.run(interp.handle_hello(msg))
    entry = hal.link_state_table.get(SRC)
    assert entry.last_hello_received is not None


def test_handle_hello_transitions_to_control_up_when_seen():
    hal = make_hal("QNode")
    hal.config.cid = MY_CID
    hal.link_state_table.add(SRC, make_entry(LINK_INIT))
    interp = LinkInterpreter(hal)

    msg = MagicMock()
    msg.src_cid = SRC
    msg.seen_neighbors = [MY_CID]  # neighbor sees us

    asyncio.run(interp.handle_hello(msg))
    entry = hal.link_state_table.get(SRC)
    assert entry.state == LINK_CONTROL_UP


def test_handle_hello_does_not_transition_if_not_seen():
    hal = make_hal("QNode")
    hal.config.cid = MY_CID
    hal.link_state_table.add(SRC, make_entry(LINK_INIT))
    interp = LinkInterpreter(hal)

    msg = MagicMock()
    msg.src_cid = SRC
    msg.seen_neighbors = []  # neighbor does NOT see us yet

    asyncio.run(interp.handle_hello(msg))
    entry = hal.link_state_table.get(SRC)
    assert entry.state == LINK_INIT


def test_handle_probe_returns_ok():
    hal = make_hal("QNode")
    interp = LinkInterpreter(hal)
    msg = MagicMock()
    msg.src_cid = SRC
    result = asyncio.run(interp.handle_probe(msg))
    assert result.status == "ok"


def test_handle_switch_port_check_returns_ok():
    hal = make_hal("OpticalSwitch")
    # Mock devs to avoid MagicMock issues
    hal.devs = {}
    interp = LinkInterpreter(hal)
    msg = MagicMock()
    msg.src_channel = "1"
    msg.dst_channel = "4"
    result = asyncio.run(interp.handle_switch_port_check(msg))
    assert result.status in ("ok", "port_down", "not_routed")
