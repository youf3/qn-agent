from dataclasses import dataclass, field
from datetime import datetime

LINK_DOWN = "DOWN"
LINK_INIT = "INIT"
LINK_CONTROL_UP = "CONTROL_UP"
LINK_QUANTUM_UP = "QUANTUM_UP"


@dataclass
class LinkEntry:
    state: str
    last_hello_sent: datetime | None
    last_hello_received: datetime | None
    hold_time: int
    channel_id: str
    neighbor_channel_id: str
    switch_in_path: bool
    switch_cid: str | None
    history: list = field(default_factory=list)  # list[tuple[datetime, str]]


class LinkStateTable:
    def __init__(self):
        self._entries: dict[str, LinkEntry] = {}

    def add(self, neighbor_cid: str, entry: LinkEntry) -> None:
        self._entries[neighbor_cid] = entry

    def get(self, neighbor_cid: str) -> LinkEntry | None:
        return self._entries.get(neighbor_cid)

    def all(self) -> dict[str, LinkEntry]:
        return dict(self._entries)

    def update(self, neighbor_cid: str, **kwargs) -> None:
        entry = self._entries[neighbor_cid]
        for k, v in kwargs.items():
            setattr(entry, k, v)

    def record_transition(self, neighbor_cid: str, new_state: str) -> None:
        entry = self._entries[neighbor_cid]
        entry.state = new_state
        entry.history.append((datetime.utcnow(), new_state))
