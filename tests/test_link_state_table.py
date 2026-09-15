from datetime import datetime
from quantnet_agent.hal.link_state_table import (
    LinkEntry, LinkStateTable,
    LINK_DOWN, LINK_INIT, LINK_CONTROL_UP, LINK_QUANTUM_UP,
)

NEIGHBOR = "urn:quant-net:LBNL-BSM:alice:4"


def make_entry(**kwargs):
    defaults = dict(
        state=LINK_DOWN,
        last_hello_sent=None,
        last_hello_received=None,
        hold_time=30,
        channel_id="1",
        neighbor_channel_id="4",
        switch_in_path=False,
        switch_cid=None,
        history=[],
    )
    defaults.update(kwargs)
    return LinkEntry(**defaults)


def test_add_and_get():
    t = LinkStateTable()
    entry = make_entry()
    t.add(NEIGHBOR, entry)
    assert t.get(NEIGHBOR) is entry


def test_get_missing_returns_none():
    t = LinkStateTable()
    assert t.get("unknown") is None


def test_update_state():
    t = LinkStateTable()
    t.add(NEIGHBOR, make_entry())
    t.update(NEIGHBOR, state=LINK_INIT)
    assert t.get(NEIGHBOR).state == LINK_INIT


def test_update_multiple_fields():
    t = LinkStateTable()
    t.add(NEIGHBOR, make_entry())
    now = datetime.utcnow()
    t.update(NEIGHBOR, state=LINK_CONTROL_UP, last_hello_received=now)
    e = t.get(NEIGHBOR)
    assert e.state == LINK_CONTROL_UP
    assert e.last_hello_received == now


def test_all_returns_all_entries():
    t = LinkStateTable()
    t.add("n1", make_entry())
    t.add("n2", make_entry())
    assert set(t.all().keys()) == {"n1", "n2"}


def test_record_transition_updates_state_and_history():
    t = LinkStateTable()
    t.add(NEIGHBOR, make_entry(state=LINK_DOWN))
    t.record_transition(NEIGHBOR, LINK_INIT)
    e = t.get(NEIGHBOR)
    assert e.state == LINK_INIT
    assert len(e.history) == 1
    ts, state = e.history[0]
    assert state == LINK_INIT
    assert isinstance(ts, datetime)


def test_record_transition_accumulates_history():
    t = LinkStateTable()
    t.add(NEIGHBOR, make_entry(state=LINK_DOWN))
    t.record_transition(NEIGHBOR, LINK_INIT)
    t.record_transition(NEIGHBOR, LINK_CONTROL_UP)
    t.record_transition(NEIGHBOR, LINK_QUANTUM_UP)
    e = t.get(NEIGHBOR)
    assert e.state == LINK_QUANTUM_UP
    assert [s for _, s in e.history] == [LINK_INIT, LINK_CONTROL_UP, LINK_QUANTUM_UP]
