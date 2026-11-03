"""Regression tests for graceful agent shutdown and outbound link DOWN publication."""
import asyncio
import json
from types import SimpleNamespace

from quantnet_agent.hal.link_state_table import (
    LINK_CONTROL_UP,
    LINK_DOWN,
    LINK_QUANTUM_UP,
    LinkEntry,
    LinkStateTable,
)
from quantnet_agent.service.link_adjacency import LinkAdjacencyManager


def _make_entry(
    neighbor_cid: str,
    channel_id: str,
    direction: str,
    is_quantum: bool,
    state: str,
) -> LinkEntry:
    return LinkEntry(
        state=state,
        last_hello_sent=None,
        last_hello_received=None,
        hold_time=30,
        neighbor_cid=neighbor_cid,
        channel_id=channel_id,
        neighbor_channel_id=channel_id,
        switch_in_path=False,
        switch_cid=None,
        is_quantum=is_quantum,
        direction=direction,
        last_published_state=state,
    )


def test_link_adjacency_stop_transitions_outbound_links_to_down_and_publishes():
    """When an agent stops, LinkAdjacencyManager.stop() must transition all outbound
    classical and quantum links to DOWN and publish linkStateUpdate(state='DOWN')."""
    table = LinkStateTable()
    table.add("QPU-1:1", _make_entry("QPU-1", "1", "out", False, LINK_CONTROL_UP))
    table.add("QPU-1:2", _make_entry("QPU-1", "2", "out", True, LINK_QUANTUM_UP))
    table.add("QPU-2:3", _make_entry("QPU-2", "3", "in", False, LINK_CONTROL_UP))

    published = []

    def _fake_publish(topic, payload, qos=0):
        published.append((topic, json.loads(payload), qos))

    mgr = LinkAdjacencyManager.__new__(LinkAdjacencyManager)
    mgr._cid = "BSM-1_2"
    mgr._table = table
    mgr._client = SimpleNamespace(_mqttclient=SimpleNamespace(publish=_fake_publish))
    mgr.is_started = True

    asyncio.run(mgr.stop())

    assert mgr.is_started is False
    assert table.get("QPU-1:1").state == LINK_DOWN
    assert table.get("QPU-1:2").state == LINK_DOWN
    # Inbound entry is not owned as an outbound publisher
    assert len(published) == 2
    states_by_ch = {p["value"]["channel_id"]: p["value"]["state"] for _, p, _ in published}
    assert states_by_ch == {"1": "DOWN", "2": "DOWN"}
