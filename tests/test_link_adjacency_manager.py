import asyncio
import json
import tempfile
import os
from datetime import datetime, timedelta
from quantnet_agent.service.link_adjacency import LinkAdjacencyManager
from quantnet_agent.hal.link_state_table import (
    LinkStateTable, LinkEntry,
    LINK_DOWN, LINK_CONTROL_UP, LINK_QUANTUM_UP,
)

# Load link_adjacency schema
from quantnet_mq.schema.models import Schema
schema_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "qn-mq/quantnet_mq/schema/rpc/link_adjacency.yaml"
)
if os.path.exists(schema_path):
    Schema.load_schema(schema_path)


MY_CID = "urn:quant-net:LBNL-Q:alice:1"
NEIGHBOR_CID = "urn:quant-net:LBNL-BSM:alice:4"


def make_node_config(neighbor_cid=NEIGHBOR_CID):
    """Write a minimal node JSON to a temp file and return its path."""
    data = {
        "systemSettings": {
            "cid": MY_CID,
            "type": "QNode",
        },
        "matterLightInterfaceSettings": [
            {
                "channels": [
                    {
                        "ID": "1",
                        "type": "quantum",
                        "direction": "out",
                        "neighbor": {
                            "idRef": neighbor_cid,
                            "systemRef": "LBNL-BSM",
                            "channelRef": "4",
                        }
                    }
                ]
            }
        ]
    }
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(data, f)
    f.close()
    return f.name


def make_manager(hello_interval=10, hold_time=30):
    table = LinkStateTable()
    cfg_path = make_node_config()
    mgr = LinkAdjacencyManager(
        cid=MY_CID,
        node_config=cfg_path,
        mqhost="localhost",
        mqport=1883,
        link_state_table=table,
        hello_interval=hello_interval,
        hold_time=hold_time,
    )
    return mgr, table, cfg_path


def test_manager_instantiates():
    mgr, table, cfg = make_manager()
    os.unlink(cfg)
    assert mgr is not None
    assert not mgr.is_started


def test_set_hello_interval():
    mgr, _, cfg = make_manager()
    os.unlink(cfg)
    mgr.set_hello_interval(5)
    assert mgr.hello_interval == 5


def test_set_hold_time():
    mgr, _, cfg = make_manager()
    os.unlink(cfg)
    mgr.set_hold_time(15)
    assert mgr.hold_time == 15


def test_load_neighbors_from_config():
    mgr, table, cfg = make_manager()
    neighbors = mgr._load_neighbors()
    os.unlink(cfg)
    assert len(neighbors) == 1
    assert neighbors[0]["neighbor_cid"] == NEIGHBOR_CID


def test_hold_timer_expires_transitions_to_down():
    """If last_hello_received is older than hold_time, link should go DOWN."""
    mgr, table, cfg = make_manager(hold_time=30)
    os.unlink(cfg)
    old_time = datetime.utcnow() - timedelta(seconds=60)
    entry = LinkEntry(
        state=LINK_CONTROL_UP,
        last_hello_sent=datetime.utcnow(),
        last_hello_received=old_time,
        hold_time=30,
        channel_id="1",
        neighbor_channel_id="4",
        switch_in_path=False,
        switch_cid=None,
        history=[],
    )
    table.add(NEIGHBOR_CID, entry)
    mgr._check_hold_timers()
    assert table.get(NEIGHBOR_CID).state == LINK_DOWN


def test_hold_timer_does_not_expire_when_recent():
    mgr, table, cfg = make_manager(hold_time=30)
    os.unlink(cfg)
    recent = datetime.utcnow()
    entry = LinkEntry(
        state=LINK_CONTROL_UP,
        last_hello_sent=datetime.utcnow(),
        last_hello_received=recent,
        hold_time=30,
        channel_id="1",
        neighbor_channel_id="4",
        switch_in_path=False,
        switch_cid=None,
        history=[],
    )
    table.add(NEIGHBOR_CID, entry)
    mgr._check_hold_timers()
    assert table.get(NEIGHBOR_CID).state == LINK_CONTROL_UP


def test_disconnect_sets_down():
    mgr, table, cfg = make_manager()
    os.unlink(cfg)
    entry = LinkEntry(
        state=LINK_QUANTUM_UP,
        last_hello_sent=None,
        last_hello_received=None,
        hold_time=30,
        channel_id="1",
        neighbor_channel_id="4",
        switch_in_path=False,
        switch_cid=None,
        history=[],
    )
    table.add(NEIGHBOR_CID, entry)
    asyncio.run(mgr.disconnect(NEIGHBOR_CID))
    assert table.get(NEIGHBOR_CID).state == LINK_DOWN
