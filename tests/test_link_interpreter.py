import asyncio
import os
from unittest.mock import MagicMock
from quantnet_agent.hal.interpreter.link import LinkInterpreter
from quantnet_agent.hal.link_state_table import (
    LinkStateTable, LinkEntry, link_key,
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
    hal._config.node_type = node_type
    hal.link_state_table = LinkStateTable()
    return hal


def make_entry(state=LINK_DOWN, neighbor_cid="LBNL-BSM", channel_id="1",
               neighbor_channel_id="4"):
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
        history=[],
    )


SRC = "LBNL-BSM"
SRC_CH = "4"
MY_CID = "LBNL-Q"
MY_CH = "1"
KEY = link_key(SRC, MY_CH)  # "LBNL-BSM:1" — keyed by (neighbor, our channel)


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


def test_opticalswitch_registers_switch_and_control():
    interp = LinkInterpreter(make_hal("OpticalSwitch"))
    cmds = interp.get_commands()
    assert "link.switchPortCheck" in cmds
    assert "link.control" in cmds
    assert "link.hello" not in cmds
    assert "link.probe" not in cmds


def test_handle_hello_updates_last_received():
    hal = make_hal("QNode")
    entry = make_entry(LINK_DOWN, neighbor_cid=SRC, channel_id=MY_CH,
                       neighbor_channel_id=SRC_CH)
    hal.link_state_table.add(KEY, entry)
    interp = LinkInterpreter(hal)

    msg = MagicMock()
    msg.payload.src_cid = SRC
    msg.payload.neighbor_channel_id = SRC_CH
    msg.payload.seen_neighbors = []

    asyncio.run(interp.handle_hello(msg))
    assert hal.link_state_table.get(KEY).last_hello_received is not None


def test_handle_hello_transitions_to_control_up_when_seen_by_key():
    """Bilateral confirmation works when seen_neighbors contains per-channel keys."""
    hal = make_hal("QNode")
    hal._config.cid = MY_CID
    entry = make_entry(LINK_INIT, neighbor_cid=SRC, channel_id=MY_CH,
                       neighbor_channel_id=SRC_CH)
    hal.link_state_table.add(KEY, entry)
    interp = LinkInterpreter(hal)

    msg = MagicMock()
    msg.payload.src_cid = SRC
    msg.payload.neighbor_channel_id = SRC_CH
    # Remote's seen list uses per-channel keys: "OUR_CID:THEIR_CH"
    msg.payload.seen_neighbors = [f"{MY_CID}:4"]

    asyncio.run(interp.handle_hello(msg))
    assert hal.link_state_table.get(KEY).state == LINK_CONTROL_UP


def test_handle_hello_transitions_to_control_up_when_seen_by_cid():
    """Backward compat: bilateral confirmation also works with bare CID."""
    hal = make_hal("QNode")
    hal._config.cid = MY_CID
    entry = make_entry(LINK_INIT, neighbor_cid=SRC, channel_id=MY_CH,
                       neighbor_channel_id=SRC_CH)
    hal.link_state_table.add(KEY, entry)
    interp = LinkInterpreter(hal)

    msg = MagicMock()
    msg.payload.src_cid = SRC
    msg.payload.neighbor_channel_id = SRC_CH
    msg.payload.seen_neighbors = [MY_CID]

    asyncio.run(interp.handle_hello(msg))
    assert hal.link_state_table.get(KEY).state == LINK_CONTROL_UP


def test_handle_hello_does_not_transition_if_not_seen():
    hal = make_hal("QNode")
    hal._config.cid = MY_CID
    entry = make_entry(LINK_INIT, neighbor_cid=SRC, channel_id=MY_CH,
                       neighbor_channel_id=SRC_CH)
    hal.link_state_table.add(KEY, entry)
    interp = LinkInterpreter(hal)

    msg = MagicMock()
    msg.payload.src_cid = SRC
    msg.payload.neighbor_channel_id = SRC_CH
    msg.payload.seen_neighbors = []  # neighbor does NOT see us yet

    asyncio.run(interp.handle_hello(msg))
    assert hal.link_state_table.get(KEY).state == LINK_INIT


def test_handle_hello_multiple_channels_independent():
    """Two channels to the same neighbor have independent state machines."""
    hal = make_hal("QNode")
    hal._config.cid = MY_CID
    key1 = link_key(SRC, "1")
    key2 = link_key(SRC, "2")
    hal.link_state_table.add(key1, make_entry(LINK_INIT, neighbor_cid=SRC,
                                              channel_id="1", neighbor_channel_id="4"))
    hal.link_state_table.add(key2, make_entry(LINK_DOWN, neighbor_cid=SRC,
                                              channel_id="2", neighbor_channel_id="5"))
    interp = LinkInterpreter(hal)

    # Hello arrives matching channel 1 (neighbor_channel_id="4")
    msg = MagicMock()
    msg.payload.src_cid = SRC
    msg.payload.neighbor_channel_id = "4"
    msg.payload.seen_neighbors = [MY_CID]

    asyncio.run(interp.handle_hello(msg))
    assert hal.link_state_table.get(key1).state == LINK_CONTROL_UP
    assert hal.link_state_table.get(key2).state == LINK_DOWN  # untouched


def test_handle_probe_returns_ok():
    hal = make_hal("QNode")
    interp = LinkInterpreter(hal)
    msg = MagicMock()
    msg.payload.src_cid = SRC
    msg.payload.channel_id = "1"
    result = asyncio.run(interp.handle_probe(msg))
    assert result.status == "ok"


def test_handle_switch_port_check_returns_ok():
    hal = make_hal("OpticalSwitch")
    # Mock devs to avoid MagicMock issues
    hal.devs = {}
    interp = LinkInterpreter(hal)
    msg = MagicMock()
    msg.payload.src_channel = "1"
    msg.payload.dst_channel = "4"
    result = asyncio.run(interp.handle_switch_port_check(msg))
    assert result.status in ("ok", "port_down", "not_routed")
