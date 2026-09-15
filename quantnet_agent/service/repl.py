import asyncio
import curses
import logging
import readline
from datetime import datetime

from quantnet_agent.hal.link_state_table import (
    LINK_DOWN, LINK_INIT, LINK_CONTROL_UP, LINK_QUANTUM_UP,
)

log = logging.getLogger(__name__)

COMMANDS = [
    "link show", "link connect", "link disconnect", "link probe",
    "link set hello-interval", "link set hold-time",
    "link switch check", "link debug on", "link debug off",
    "link monitor", "help", "exit", "quit",
]


def _make_completer(neighbor_cids, commands):
    def completer(text, state):
        options = [c for c in commands + neighbor_cids if c.startswith(text)]
        return options[state] if state < len(options) else None
    return completer


class AgentREPL:
    def __init__(self, cid: str, link_mgr, link_state_table):
        self._cid = cid
        self._link_mgr = link_mgr
        self._table = link_state_table
        self._debug = False
        self._node_name = cid.split(":")[-2] if ":" in cid else cid

    def _setup_readline(self):
        neighbor_cids = list(self._table.all().keys())
        readline.set_completer(
            _make_completer(neighbor_cids, COMMANDS)
        )
        readline.parse_and_bind("tab: complete")

    def _prompt(self):
        return f"quantnet-agent [{self._node_name}]> "

    async def start(self):
        self._setup_readline()
        loop = asyncio.get_event_loop()
        print("\nAgent ready. Type 'help' for commands.\n")
        while True:
            try:
                line = await loop.run_in_executor(
                    None, lambda: input(self._prompt())
                )
            except (EOFError, KeyboardInterrupt):
                print()
                break
            line = line.strip()
            if not line:
                continue
            if await self._dispatch(line):
                break  # exit/quit

    async def _dispatch(self, line: str) -> bool:
        parts = line.split()
        if not parts:
            return False
        cmd = parts[0]

        if cmd in ("exit", "quit"):
            print("Shutting down agent...")
            return True

        if cmd == "help":
            self._print_help()
            return False

        if cmd == "link" and len(parts) >= 2:
            sub = parts[1]
            args = parts[2:]

            if sub == "show":
                self._cmd_link_show(args)
            elif sub == "connect" and args:
                await self._cmd_link_connect(args[0])
            elif sub == "disconnect" and args:
                await self._cmd_link_disconnect(args[0])
            elif sub == "probe" and args:
                await self._cmd_link_probe(args[0])
            elif sub == "set" and len(args) >= 2:
                self._cmd_link_set(args[0], args[1])
            elif sub == "switch" and len(args) >= 2 and args[0] == "check":
                await self._cmd_link_switch_check(args[1])
            elif sub == "debug" and args:
                self._cmd_link_debug(args[0])
            elif sub == "monitor":
                self._cmd_link_monitor()
            else:
                print(f"Unknown link command: {sub}. Type 'help'.")
        else:
            print(f"Unknown command: {line}. Type 'help'.")

        return False

    def _cmd_link_show(self, args):
        entries = self._table.all()
        if not entries:
            print("No links configured.")
            return
        if args:
            neighbor_cid = args[0]
            entry = self._table.get(neighbor_cid)
            if entry is None:
                print(f"Unknown neighbor: {neighbor_cid}")
                return
            print(f"\nLink: {neighbor_cid}")
            print(f"  State:               {entry.state}")
            print(f"  Channel:             {entry.channel_id} → {entry.neighbor_channel_id}")
            print(f"  Switch in path:      {entry.switch_in_path}")
            print(f"  Hold time:           {entry.hold_time}s")
            last_rx = entry.last_hello_received
            print(f"  Last hello RX:       {self._age(last_rx)}")
            last_tx = entry.last_hello_sent
            print(f"  Last hello TX:       {self._age(last_tx)}")
            if entry.history:
                print("  Recent transitions:")
                for ts, state in entry.history[-10:]:
                    print(f"    {ts.strftime('%H:%M:%S')}  {state}")
            print()
        else:
            print(f"\n{'Neighbor':<45} {'State':<14} {'Last RX'}")
            print("-" * 75)
            for cid, entry in entries.items():
                print(f"{cid:<45} {entry.state:<14} {self._age(entry.last_hello_received)}")
            print()

    def _age(self, dt) -> str:
        if dt is None:
            return "never"
        secs = (datetime.utcnow() - dt).total_seconds()
        return f"{int(secs)}s ago"

    async def _cmd_link_connect(self, neighbor_cid: str):
        print(f"Triggering adjacency with {neighbor_cid}...")
        await self._link_mgr.connect(neighbor_cid)

    async def _cmd_link_disconnect(self, neighbor_cid: str):
        print(f"Disconnecting {neighbor_cid}...")
        await self._link_mgr.disconnect(neighbor_cid)

    async def _cmd_link_probe(self, neighbor_cid: str):
        entry = self._table.get(neighbor_cid)
        if entry is None:
            print(f"Unknown neighbor: {neighbor_cid}")
            return
        print(f"Running probe to {neighbor_cid}...")
        await self._link_mgr._send_probe(neighbor_cid, entry)

    def _cmd_link_set(self, param: str, value: str):
        try:
            n = int(value)
        except ValueError:
            print(f"Invalid value: {value}")
            return
        if param == "hello-interval":
            self._link_mgr.set_hello_interval(n)
            print(f"Hello interval set to {n}s")
        elif param == "hold-time":
            self._link_mgr.set_hold_time(n)
            print(f"Hold time set to {n}s")
        else:
            print(f"Unknown parameter: {param}. Use hello-interval or hold-time.")

    async def _cmd_link_switch_check(self, neighbor_cid: str):
        entry = self._table.get(neighbor_cid)
        if entry is None:
            print(f"Unknown neighbor: {neighbor_cid}")
            return
        if not entry.switch_in_path or not entry.switch_cid:
            print("No switch in path for this link.")
            return
        ok = await self._link_mgr._check_switch(
            entry.switch_cid, entry.channel_id, entry.neighbor_channel_id
        )
        print(f"Switch port check: {'OK' if ok else 'FAILED'}")

    def _cmd_link_debug(self, onoff: str):
        if onoff == "on":
            self._debug = True
            logging.getLogger("quantnet_agent.service.link_adjacency").setLevel(logging.DEBUG)
            logging.getLogger("quantnet_agent.hal.interpreter.link").setLevel(logging.DEBUG)
            print("Link debug logging enabled.")
        elif onoff == "off":
            self._debug = False
            logging.getLogger("quantnet_agent.service.link_adjacency").setLevel(logging.INFO)
            logging.getLogger("quantnet_agent.hal.interpreter.link").setLevel(logging.INFO)
            print("Link debug logging disabled.")
        else:
            print("Usage: link debug on|off")

    def _cmd_link_monitor(self):
        """Enter curses live dashboard mode."""
        try:
            curses.wrapper(self._monitor_dashboard)
        except KeyboardInterrupt:
            pass
        print("Returned to prompt.")

    def _monitor_dashboard(self, stdscr):
        curses.curs_set(0)
        stdscr.nodelay(True)
        curses.start_color()
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(4, curses.COLOR_CYAN, curses.COLOR_BLACK)

        STATE_COLOR = {
            LINK_QUANTUM_UP: 1,
            LINK_CONTROL_UP: 2,
            LINK_INIT: 4,
            LINK_DOWN: 3,
        }

        while True:
            key = stdscr.getch()
            if key == ord("q"):
                break

            stdscr.clear()
            h, w = stdscr.getmaxyx()
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            title = f" Link Adjacency Monitor ── {self._node_name} ── {now} "
            stdscr.addstr(0, 0, title[:w-1], curses.A_BOLD)
            stdscr.addstr(1, 0, "─" * (w-1))

            # Header row
            stdscr.addstr(2, 0, f"{'Neighbor':<44} {'State':<14} {'Last RX':<12}", curses.A_BOLD)
            stdscr.addstr(3, 0, "─" * (w-1))

            entries = self._table.all()
            row = 4
            split = max(h - 10, row + 2)
            for cid, entry in entries.items():
                if row >= split:
                    break
                color = curses.color_pair(STATE_COLOR.get(entry.state, 0))
                line = f"{cid[:43]:<44} {entry.state:<14} {self._age(entry.last_hello_received):<12}"
                stdscr.addstr(row, 0, line[:w-1], color)
                row += 1

            stdscr.addstr(split, 0, "─" * (w-1))
            stdscr.addstr(split+1, 0, "Recent Events", curses.A_BOLD)

            # Collect recent history from all entries
            events = []
            for cid, entry in entries.items():
                for ts, state in entry.history:
                    events.append((ts, cid, state))
            events.sort(key=lambda x: x[0], reverse=True)

            for i, (ts, cid, state) in enumerate(events[:h - split - 4]):
                color = curses.color_pair(STATE_COLOR.get(state, 0))
                line = f"  {ts.strftime('%H:%M:%S')}  {cid.split(':')[-2]:<20} {state}"
                stdscr.addstr(split + 2 + i, 0, line[:w-1], color)

            stdscr.addstr(h-2, 0, "─" * (w-1))
            stdscr.addstr(h-1, 0, " q: back to prompt ", curses.A_REVERSE)
            stdscr.refresh()

            import time
            time.sleep(1)

    def _print_help(self):
        print("""
Link adjacency commands:
  link show                      Show all links and their state
  link show <neighbor_cid>       Detailed view of one link
  link connect <neighbor_cid>    Manually trigger adjacency
  link disconnect <neighbor_cid> Bring one link to DOWN
  link probe <neighbor_cid>      Trigger quantum probe
  link set hello-interval <n>    Change hello interval (seconds)
  link set hold-time <n>         Change hold time (seconds)
  link switch check <neighbor>   Query switch port for a link
  link debug on|off              Toggle debug logging
  link monitor                   Live dashboard (q to exit)
  help                           This message
  exit / quit                    Shut down agent
""")
