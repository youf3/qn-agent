import asyncio
import ctypes
import ctypes.util
import curses
import logging
import os
import readline
import signal
import sys
import time
from datetime import datetime

from quantnet_agent.hal.link_state_table import (
    LINK_DOWN, LINK_INIT, LINK_CONTROL_UP, LINK_QUANTUM_UP,
)

log = logging.getLogger(__name__)

COMMANDS = [
    "show link", "show probe", "show logging", "show config", "show switch-check",
    "connect", "disconnect", "probe",
    "set hello-interval", "set hold-time",
    "debug link", "no debug link",
    "debug probe", "no debug probe",
    "logging console", "no logging console",
    "monitor", "monitor probe", "help", "exit", "quit",
]


def _make_completer(table):
    """Build a context-aware completer that uses the full line buffer."""
    # Top-level commands (first word)
    top_cmds = [
        "show", "connect", "disconnect", "probe", "set",
        "debug", "no", "logging", "monitor", "help", "exit", "quit",
    ]
    # Subcommand trees
    show_subs = ["link", "probe", "logging", "config", "switch-check"]
    set_subs = ["hello-interval", "hold-time"]
    debug_subs = ["link", "probe"]
    no_subs = ["debug", "logging"]

    def _link_keys():
        return list(table.all().keys())

    def _match(candidates, prefix):
        return [c for c in candidates if c.startswith(prefix)]

    def completer(text, state):
        buf = readline.get_line_buffer().lstrip()
        parts = buf.split()
        # If buffer ends with space, we're completing the next word
        completing_new = buf.endswith(" ") and buf != ""
        n = len(parts) + (1 if completing_new else 0)
        word = text  # current word fragment

        if n <= 1:
            # First word: top-level commands
            options = _match(top_cmds, word)
        elif parts[0] == "show":
            if n == 2:
                options = _match(show_subs, word)
            elif n == 3 and parts[1] in ("link", "probe", "switch-check"):
                options = _match(_link_keys(), word)
            else:
                options = []
        elif parts[0] in ("connect", "disconnect", "probe"):
            if n == 2:
                options = _match(_link_keys(), word)
            else:
                options = []
        elif parts[0] == "set":
            if n == 2:
                options = _match(set_subs, word)
            else:
                options = []
        elif parts[0] == "debug":
            if n == 2:
                options = _match(debug_subs, word)
            else:
                options = []
        elif parts[0] == "monitor":
            if n == 2:
                options = _match(["probe"], word)
            else:
                options = []
        elif parts[0] == "logging":
            if n == 2:
                options = _match(["console"], word)
            else:
                options = []
        elif parts[0] == "no":
            if n == 2:
                options = _match(no_subs, word)
            elif n == 3 and parts[1] == "debug":
                options = _match(debug_subs, word)
            elif n == 3 and parts[1] == "logging":
                options = _match(["console"], word)
            else:
                options = []
        else:
            options = []

        return options[state] if state < len(options) else None
    return completer


def _load_readline_lib():
    """Load the native readline/libedit shared library for rl_forced_update_display."""
    try:
        lib_name = ctypes.util.find_library("readline") or ctypes.util.find_library("edit")
        if lib_name:
            return ctypes.cdll.LoadLibrary(lib_name)
    except OSError:
        pass
    return None


_rl_lib = _load_readline_lib()


class REPLLogHandler(logging.Handler):
    """Log handler that prints above the readline prompt without corrupting it.

    When a log record arrives while the user is typing, this handler:
    1. Clears the current terminal line
    2. Prints the formatted log record
    3. Asks readline to redraw the prompt and the user's in-progress input

    The ``waiting`` flag must be set to True while the REPL is blocked on
    ``input()`` and False at all other times.  When False the handler prints
    the log line normally without redrawing a prompt.
    """

    def __init__(self, prompt_fn, fmt=None, filter_fn=None):
        super().__init__()
        self._prompt_fn = prompt_fn
        self._filter_fn = filter_fn
        self.waiting = False
        if fmt:
            self.setFormatter(fmt)

    def emit(self, record):
        if self._filter_fn and not self._filter_fn(record):
            return
        try:
            msg = self.format(record)
            if self.waiting:
                # Clear the current line and print the log message
                sys.stdout.write(f"\r\033[K{msg}\n")
                sys.stdout.flush()
                # Ask readline to redraw the prompt + current input buffer
                if _rl_lib and hasattr(_rl_lib, "rl_forced_update_display"):
                    _rl_lib.rl_on_new_line()
                    _rl_lib.rl_forced_update_display()
                else:
                    # Fallback: redraw bare prompt (input reappears on next keypress)
                    sys.stdout.write(self._prompt_fn())
                    sys.stdout.flush()
            else:
                sys.stdout.write(f"{msg}\n")
                sys.stdout.flush()
        except Exception:
            self.handleError(record)


class AgentREPL:
    def __init__(self, cid: str, link_mgr, link_state_table, config=None):
        self._cid = cid
        self._link_mgr = link_mgr
        self._table = link_state_table
        self._config = config
        self._debug = False
        self._debug_probe = False
        self._logging_console_enabled = False  # Hide logging by default (like iOS)
        self._node_name = cid.split(":")[-2] if ":" in cid else cid
        self._repl_handler = None
        self._last_sigint_time: float = 0.0  # for double-Ctrl+C detection
        self._loop: asyncio.AbstractEventLoop | None = None  # event loop running in bg thread

    def _setup_readline(self):
        readline.set_completer(_make_completer(self._table))
        # Include ':' and '-' in word breaks so tokens like LBNL-SWITCH:1 and
        # hello-interval are treated as single completable words
        readline.set_completer_delims(" \t\n")
        # libedit (macOS) uses different binding syntax than GNU readline
        if "libedit" in (readline.__doc__ or ""):
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")

    def _prompt(self):
        return f"quantnet-agent [{self._node_name}]> "

    def _allow_log_record(self, record: logging.LogRecord) -> bool:
        """Filter log records for the REPL handler based on console/debug flags."""
        if self._logging_console_enabled and record.levelno >= logging.INFO:
            return True
        if self._debug and record.name.startswith((
            "quantnet_agent.service.link_adjacency",
            "quantnet_agent.hal.interpreter.link",
        )):
            return True
        if self._debug_probe and record.name.startswith((
            "quantnet_agent.hal.driver.dummy",
            "quantnet_agent.service.link_adjacency",
            "quantnet_agent.hal.interpreter.link",
        )):
            return True
        return False

    def _update_repl_log_level(self):
        """Adjust REPLLogHandler level when console logging or debug flags change."""
        if self._repl_handler is None:
            return
        if self._logging_console_enabled or self._debug or self._debug_probe:
            self._repl_handler.setLevel(logging.DEBUG)
        else:
            self._repl_handler.setLevel(logging.CRITICAL)

    def _install_log_handler(self):
        """Replace the root logger's stdout handler with a REPL-aware one.

        By default, logging is hidden (CRITICAL level) to keep the REPL clean.
        Users can enable it with 'logging console', 'debug link', or 'debug probe'.
        """
        root = logging.getLogger()
        # Find the existing formatter from the current handler
        fmt = None
        for h in root.handlers:
            if isinstance(h, logging.StreamHandler) and h.stream is sys.stdout:
                fmt = h.formatter
                break
        self._repl_handler = REPLLogHandler(
            self._prompt, fmt=fmt, filter_fn=self._allow_log_record
        )
        self._update_repl_log_level()
        # Remove stdout handlers, add ours
        self._original_handlers = []
        for h in list(root.handlers):
            if isinstance(h, logging.StreamHandler) and h.stream is sys.stdout:
                self._original_handlers.append(h)
                root.removeHandler(h)
        root.addHandler(self._repl_handler)

    def _uninstall_log_handler(self):
        """Restore the original log handlers."""
        if self._repl_handler is None:
            return
        root = logging.getLogger()
        root.removeHandler(self._repl_handler)
        for h in self._original_handlers:
            root.addHandler(h)
        self._repl_handler = None
        self._original_handlers = []

    def _run_async(self, coro):
        """Run a coroutine on the background event loop and block until done."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout=30)

    def run_sync(self, loop: asyncio.AbstractEventLoop, on_shutdown=None):
        """Blocking REPL loop. Must be called from the main thread.

        The event loop runs in a background thread (started by the agent).
        Async commands are dispatched to that loop via run_coroutine_threadsafe.

        Args:
            loop: the running event loop in the background thread
            on_shutdown: optional callable invoked after the REPL exits
        """
        from quantnet_agent.common.logging import clear_buffered_logs
        self._loop = loop
        self._setup_readline()
        self._install_log_handler()
        clear_buffered_logs()
        print("\nAgent ready. Type 'help' for commands.\n")

        try:
            while True:
                try:
                    self._repl_handler.waiting = True
                    line = input(self._prompt())
                    self._repl_handler.waiting = False
                except KeyboardInterrupt:
                    # Ctrl+C: cancel this input, stay alive.
                    self._repl_handler.waiting = False
                    now = time.monotonic()
                    if now - self._last_sigint_time < 1.0:
                        print("\nForce exit.")
                        os.kill(os.getpid(), signal.SIGKILL)
                    self._last_sigint_time = now
                    print("^C")
                    continue
                except EOFError:
                    # Ctrl+D: clean shutdown
                    self._repl_handler.waiting = False
                    print()
                    break

                line = line.strip()
                if not line:
                    continue
                if self._dispatch(line):
                    break  # exit/quit
        finally:
            self._uninstall_log_handler()
            if on_shutdown:
                on_shutdown()
            # Send SIGTERM so agent's handle_exit runs deregister and loop exits.
            os.kill(os.getpid(), signal.SIGTERM)

    def _dispatch(self, line: str) -> bool:
        parts = line.split()
        if not parts:
            return False
        cmd = parts[0]
        args = parts[1:]

        if cmd in ("exit", "quit"):
            print("Shutting down agent...")
            return True

        if cmd == "help" or cmd == "?":
            self._print_help()
        elif cmd == "show":
            if not args or args[0] == "link":
                self._cmd_link_show(args[1:])
            elif args[0] == "probe":
                self._cmd_probe_show(args[1:])
            elif args[0] == "logging":
                self._cmd_show_logging()
            elif args[0] == "config":
                self._cmd_show_config()
            elif args[0] == "switch-check" and len(args) >= 2:
                self._run_async(self._cmd_link_switch_check(args[1]))
            else:
                print(f"Unknown show target: {args[0]}")
        elif cmd == "connect" and args:
            self._run_async(self._cmd_link_connect(args[0]))
        elif cmd == "disconnect" and args:
            self._run_async(self._cmd_link_disconnect(args[0]))
        elif cmd == "probe" and args:
            self._run_async(self._cmd_link_probe(args[0], args[1:]))
        elif cmd == "set" and len(args) >= 2:
            self._cmd_link_set(args[0], args[1])
        elif cmd == "debug" and args and args[0] == "link":
            self._cmd_link_debug("on")
        elif cmd == "no" and len(args) >= 2 and args[0] == "debug" and args[1] == "link":
            self._cmd_link_debug("off")
        elif cmd == "debug" and args and args[0] == "probe":
            self._cmd_probe_debug("on")
        elif cmd == "no" and len(args) >= 2 and args[0] == "debug" and args[1] == "probe":
            self._cmd_probe_debug("off")
        elif cmd == "logging" and args and args[0] == "console":
            self._cmd_logging_console("on")
        elif cmd == "no" and len(args) >= 2 and args[0] == "logging" and args[1] == "console":
            self._cmd_logging_console("off")
        elif cmd == "monitor":
            mode = "probe" if (args and args[0] == "probe") else "link"
            self._cmd_link_monitor(mode=mode)
        else:
            print(f"Unknown command: {line}")

        return False

    def _cmd_link_show(self, args):
        entries = self._table.all()
        if not entries:
            print("No links configured.")
            return
        if args:
            key = args[0]
            entry = self._table.get(key)
            if entry is None:
                print(f"Unknown link: {key}")
                return
            link_type = "quantum" if entry.is_quantum else "classical"
            if entry.direction == "out":
                arrow = f"ch {entry.channel_id} (TX) → ch {entry.neighbor_channel_id} (RX)"
            else:
                arrow = f"ch {entry.channel_id} (RX) ← ch {entry.neighbor_channel_id} (TX)"
            print(f"\nLink: {key}")
            if entry.direction == "out":
                print(f"  State:               {entry.state}")
            print(f"  Neighbor:            {entry.neighbor_cid}")
            print(f"  Direction:           {arrow}")
            print(f"  Link type:           {link_type}")
            if entry.direction == "out":
                print(f"  Switch in path:      {entry.switch_in_path}")
                print(f"  Hold time:           {entry.hold_time}s")
                print(f"  Last hello RX:       {self._age(entry.last_hello_received)}")
                print(f"  Last hello TX:       {self._age(entry.last_hello_sent)}")
                if entry.is_quantum:
                    counts = f"{entry.probe_tx_count} / {entry.probe_ok_count} / {entry.probe_fail_count}"
                    status = entry.last_probe_status or "—"
                    print(f"  Probe TX / OK / Fail: {counts}")
                    print(f"  Last probe TX:       {self._age(entry.last_probe_sent)} ({status})")
                if entry.history:
                    print("  Recent transitions:")
                    for ts, state in entry.history[-10:]:
                        print(f"    {ts.strftime('%H:%M:%S')}  {state}")
            elif entry.is_quantum:
                counts = f"{entry.probe_rx_count} / {entry.probe_ok_count} / {entry.probe_fail_count}"
                status = entry.last_probe_status or "—"
                print(f"  Probe RX / OK / Fail: {counts}")
                print(f"  Last probe RX:       {self._age(entry.last_probe_received)} ({status})")
            print()
        else:
            print(f"\n{'Link':<35} {'State':<14} {'Type':<11} {'Direction':<17} {'Last RX'}")
            print("-" * 90)
            for key, entry in entries.items():
                link_type = "quantum" if entry.is_quantum else "classical"
                if entry.direction == "out":
                    direction = f"ch{entry.channel_id}→ch{entry.neighbor_channel_id}"
                    state = entry.state
                    last_rx = "—"  # Outbound doesn't receive hellos
                else:
                    direction = f"ch{entry.channel_id}←ch{entry.neighbor_channel_id}"
                    state = "—"
                    last_rx = self._age(entry.last_hello_received)  # Show when last hello arrived
                print(f"{key:<35} {state:<14} {link_type:<11} {direction:<17} {last_rx}")
            print()

    def _get_vlink_for_entry(self, entry) -> dict | None:
        """Return virtual link metadata from LightSrc (outbound) or SignalMeasurement (inbound)."""
        if self._link_mgr is None:
            return None
        if entry.direction == "out":
            get_ls = getattr(self._link_mgr, "_get_lightsource", None)
            lightsrc = get_ls() if callable(get_ls) else None
            if lightsrc and hasattr(lightsrc, "get_outbound_vlink"):
                vlink = lightsrc.get_outbound_vlink(entry.channel_id, dst_cid=entry.neighbor_cid)
                if vlink is not None:
                    return {
                        "vlink_id": vlink.vlink_id,
                        "peer_ip": vlink.peer_ip,
                        "port": vlink.port,
                        "wavelength": vlink.wavelength,
                        "power": vlink.power,
                    }
        else:
            get_sm = getattr(self._link_mgr, "_get_signalmeasurement", None)
            sigmeas = get_sm() if callable(get_sm) else None
            in_links = getattr(sigmeas, "_inbound_links", None) if sigmeas else None
            if isinstance(in_links, dict):
                vlink = in_links.get((str(entry.neighbor_cid), str(entry.neighbor_channel_id)))
                if vlink is not None:
                    return {
                        "vlink_id": vlink.vlink_id,
                        "listen_ip": vlink.local_ip,
                        "port": vlink.port,
                        "wavelength": vlink.wavelength,
                        "power": vlink.power,
                    }
        return None

    def _cmd_probe_show(self, args):
        entries = self._table.all()
        quantum_entries = {k: v for k, v in entries.items() if v.is_quantum}
        if not quantum_entries:
            print("No quantum links configured.")
            return

        if args:
            key = args[0]
            entry = self._table.get(key)
            if entry is None:
                print(f"Unknown link: {key}")
                return
            if not entry.is_quantum:
                print(f"Link {key} is classical (not a quantum link).")
                return

            vlink = self._get_vlink_for_entry(entry)
            print(f"\nQuantum Probe Status: {key}")
            print("-" * 72)
            print(f"  Direction:           {entry.direction.upper()} (ch {entry.channel_id})")
            print(f"  Neighbor:            {entry.neighbor_cid} (ch {entry.neighbor_channel_id})")
            print(f"  Link State:          {entry.state if entry.direction == 'out' else '—'}")
            if vlink:
                if entry.direction == "out":
                    pip = str(vlink.get("peer_ip", ""))
                    pip_fmt = f"[{pip}]" if ":" in pip else pip
                    endpoint = f"{pip_fmt}:{vlink.get('port')}"
                    wl = vlink.get("wavelength")
                    pw = vlink.get("power")
                    print(f"  Virtual Link (TX):   {vlink.get('vlink_id')} → UDP {endpoint}")
                    print(f"  Optical Config:      wavelength={wl} nm, power={pw} dBm")
                else:
                    lip = str(vlink.get("listen_ip", ""))
                    lip_fmt = f"[{lip}]" if ":" in lip else lip
                    endpoint = f"{lip_fmt}:{vlink.get('port')}"
                    print(f"  Virtual Link (RX):   {vlink.get('vlink_id')} ← UDP {endpoint}")
            else:
                print("  Virtual Link:        none configured")

            if entry.direction == "out":
                print(f"  Probes Sent (TX):    {entry.probe_tx_count}")
                print(f"  Probes Succeeded:    {entry.probe_ok_count}")
                print(f"  Probes Failed:       {entry.probe_fail_count}")
                print(f"  Last Probe Sent:     {self._age(entry.last_probe_sent)}")
            else:
                print(f"  Probes Gated (RX):   {entry.probe_rx_count}")
                print(f"  Detections (OK):     {entry.probe_ok_count}")
                print(f"  Timeouts / Fails:    {entry.probe_fail_count}")
                print(f"  Last Probe RX:       {self._age(entry.last_probe_received)}")

            print(f"  Last Probe Status:   {entry.last_probe_status or 'never'}")
            print(f"  Last Probe ID:       {entry.last_probe_id or '—'}")

            if entry.last_probe_info:
                info = entry.last_probe_info
                print("  Last Probe Details:")
                if info.get("direction") == "out":
                    print(f"    RTT:               {info.get('rtt_ms', '—')} ms")
                    print(f"    Wavelength / Pwr:  {info.get('wavelength', '—')} nm / {info.get('power', '—')} dBm")
                    if info.get("reason"):
                        print(f"    Failure Reason:    {info.get('reason')}")
                else:
                    print(f"    Detected:          {info.get('detected')}")
                    print(f"    Photon Counts:     {info.get('counts', 0)}")
                    print(f"    Time-of-Flight:    {info.get('tof', '—')} s")
                    print(f"    RX Wavelength/Pwr: {info.get('wavelength', '—')} nm / {info.get('power', '—')} dBm")
                    if info.get("reason"):
                        print(f"    Failure Reason:    {info.get('reason')}")

            if entry.probe_history:
                print("\n  Recent Probe History (newest last):")
                for ts, pinfo in entry.probe_history[-10:]:
                    ts_str = ts.strftime("%H:%M:%S")
                    st = pinfo.get("status", "?")
                    pid = pinfo.get("probe_id", "—")
                    if pinfo.get("direction") == "out":
                        rtt = pinfo.get("rtt_ms")
                        rtt_str = f"rtt={rtt}ms" if rtt is not None else ""
                        reason = pinfo.get("reason") or ""
                        print(f"    {ts_str}  TX  id={pid:<8} status={st:<10} {rtt_str} {reason}".rstrip())
                    else:
                        cnt = pinfo.get("counts", 0)
                        tof = pinfo.get("tof")
                        tof_str = f"tof={tof:.2e}s" if isinstance(tof, (int, float)) else "tof=—"
                        print(f"    {ts_str}  RX  id={pid:<8} status={st:<10} counts={cnt} {tof_str}")
            print()
            return

        # Summary across all quantum links + hardware driver header
        get_ls = getattr(self._link_mgr, "_get_lightsource", None) if self._link_mgr else None
        get_sm = getattr(self._link_mgr, "_get_signalmeasurement", None) if self._link_mgr else None
        lightsrc = get_ls() if callable(get_ls) else None
        sigmeas = get_sm() if callable(get_sm) else None
        ls_vlinks = len(getattr(lightsrc, "_outbound_links", {}) or {}) if lightsrc else 0
        sm_vlinks = len(getattr(sigmeas, "_inbound_links", {}) or {}) if sigmeas else 0
        ls_name = f"{type(lightsrc).__name__} ({ls_vlinks} TX vlinks)" if lightsrc else "None"
        sm_name = f"{type(sigmeas).__name__} ({sm_vlinks} RX vlinks)" if sigmeas else "None"

        print(f"\nHardware Probe Drivers: LightSrc={ls_name} | SignalMeasurement={sm_name}")
        header = (
            f"{'Link':<20} {'Dir':<4} {'State':<12} {'TX/RX':<7} "
            f"{'OK/Fail':<9} {'Last Status':<12} {'Last Age':<10} {'VLink / Details'}"
        )
        print(header)
        print("-" * 105)
        for key, entry in quantum_entries.items():
            vlink = self._get_vlink_for_entry(entry)
            if entry.direction == "out":
                state = entry.state
                tx_rx = f"TX:{entry.probe_tx_count}"
                age = self._age(entry.last_probe_sent)
                if vlink:
                    pip = str(vlink.get("peer_ip", ""))
                    pip_fmt = f"[{pip}]" if ":" in pip else pip
                    vlink_str = f"{vlink.get('vlink_id')}→{pip_fmt}:{vlink.get('port')}"
                else:
                    vlink_str = "—"
                if entry.last_probe_info and entry.last_probe_info.get("rtt_ms") is not None:
                    vlink_str += f" ({entry.last_probe_info['rtt_ms']}ms)"
            else:
                state = "—"
                tx_rx = f"RX:{entry.probe_rx_count}"
                age = self._age(entry.last_probe_received)
                vlink_str = f"{vlink.get('vlink_id')}←:{vlink.get('port')}" if vlink else "—"
                if entry.last_probe_info and entry.last_probe_info.get("tof") is not None:
                    tof = entry.last_probe_info["tof"]
                    cnt = entry.last_probe_info.get("counts", 0)
                    vlink_str += f" (cnt={cnt}, tof={tof:.2e}s)"
            ok_fail = f"{entry.probe_ok_count}/{entry.probe_fail_count}"
            status = entry.last_probe_status or "never"
            print(
                f"{key:<20} {entry.direction:<4} {state:<12} {tx_rx:<7} "
                f"{ok_fail:<9} {status:<12} {age:<10} {vlink_str}"
            )
        print()

    def _age(self, dt) -> str:
        if dt is None:
            return "never"
        secs = (datetime.utcnow() - dt).total_seconds()
        return f"{int(secs)}s ago"

    async def _cmd_link_connect(self, target: str):
        print(f"Triggering adjacency on {target}...")
        await self._link_mgr.connect(target)

    async def _cmd_link_disconnect(self, target: str):
        print(f"Disconnecting {target}...")
        await self._link_mgr.disconnect(target)

    async def _cmd_link_probe(self, target: str, extra_args=None):
        entry = self._table.get(target)
        if entry is None:
            print(f"Unknown link: {target}")
            return
        count = 1
        if extra_args:
            try:
                count = max(1, int(extra_args[0]))
            except ValueError:
                print(f"Invalid probe count: {extra_args[0]}")
                return

        for i in range(count):
            prefix = f"[{i + 1}/{count}] " if count > 1 else ""
            print(f"{prefix}Running quantum probe on {target}...")
            res = await self._link_mgr._send_probe(target, entry)
            if not isinstance(res, dict):
                continue
            status = res.get("status", "unknown")
            pid = res.get("probe_id", "—")
            vlink = res.get("vlink") or "—"
            rtt = res.get("rtt_ms")
            rtt_str = f"{rtt} ms" if rtt is not None else "—"
            wl = res.get("wavelength", "—")
            pw = res.get("power", "—")
            if status == "ok":
                print(
                    f"  [PASS] probe_id={pid} vlink={vlink} "
                    f"optical=({wl}nm, {pw}dBm) rtt={rtt_str} state={entry.state}"
                )
            elif status == "skipped":
                print(f"  [SKIP] reason={res.get('reason')}")
            else:
                print(
                    f"  [FAIL] status={status} probe_id={pid} vlink={vlink} "
                    f"reason={res.get('reason')} rtt={rtt_str} state={entry.state}"
                )
            if i + 1 < count:
                await asyncio.sleep(0.2)

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

    async def _cmd_link_switch_check(self, target: str):
        entry = self._table.get(target)
        if entry is None:
            print(f"Unknown link: {target}")
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
            self._update_repl_log_level()
            print("Link debug logging enabled.")
        elif onoff == "off":
            self._debug = False
            if not self._debug_probe:
                logging.getLogger("quantnet_agent.service.link_adjacency").setLevel(logging.INFO)
                logging.getLogger("quantnet_agent.hal.interpreter.link").setLevel(logging.INFO)
            self._update_repl_log_level()
            print("Link debug logging disabled.")
        else:
            print("Usage: debug link / no debug link")

    def _cmd_probe_debug(self, onoff: str):
        if onoff == "on":
            self._debug_probe = True
            logging.getLogger("quantnet_agent.hal.driver.dummy").setLevel(logging.DEBUG)
            logging.getLogger("quantnet_agent.service.link_adjacency").setLevel(logging.DEBUG)
            logging.getLogger("quantnet_agent.hal.interpreter.link").setLevel(logging.DEBUG)
            self._update_repl_log_level()
            print("Quantum probe debug logging enabled (LightSrc, SignalMeasurement, Link).")
        elif onoff == "off":
            self._debug_probe = False
            logging.getLogger("quantnet_agent.hal.driver.dummy").setLevel(logging.INFO)
            if not self._debug:
                logging.getLogger("quantnet_agent.service.link_adjacency").setLevel(logging.INFO)
                logging.getLogger("quantnet_agent.hal.interpreter.link").setLevel(logging.INFO)
            self._update_repl_log_level()
            print("Quantum probe debug logging disabled.")
        else:
            print("Usage: debug probe / no debug probe")

    def _cmd_show_logging(self):
        """Show current logging console status and buffered logs."""
        from quantnet_agent.common.logging import get_buffered_logs
        status = "enabled" if self._logging_console_enabled else "disabled"
        print(f"Logging to console is {status}.")
        logs = get_buffered_logs(50)  # Show last 50 logs
        if logs:
            print(f"\nRecent logs (last {len(logs)}):")
            print("-" * 80)
            for line in logs:
                print(line)
            print("-" * 80)
        else:
            print("\nNo logs in buffer.")

    def _cmd_show_config(self):
        """Show agent configuration state."""
        import json
        cid = self._config.cid if self._config else "N/A"

        print(f"\nAgent Configuration ({cid}):")
        print("-" * 60)

        # Node info
        print(f"  Node ID                {cid}")
        if self._config:
            print(f"  Worker Threads         {self._config.threads}")
            broker = f"{self._config.mq_broker_host}:{self._config.mq_broker_port}"
            print(f"  Message Broker         {broker}")

        # Load node config for qubits and links
        if self._config and self._config.node_file:
            try:
                with open(self._config.node_file) as f:
                    node_config = json.load(f)

                # Qubits
                qubit_settings = node_config.get("qubitSettings", {})
                qubits = qubit_settings.get("qubits", [])
                if qubits:
                    print(f"\nQubits ({len(qubits)} total):")
                    for qubit in qubits:
                        qubit_id = qubit.get('ID')
                        qubit_type = qubit.get('quantumObject', 'N/A')
                        t1 = qubit.get('T1', {}).get('value', 'N/A')
                        t2 = qubit.get('T2', {}).get('value', 'N/A')
                        print(f"    Qubit {qubit_id}")
                        print(f"      Type               {qubit_type}")
                        print(f"      T1 (decay)         {t1} s")
                        print(f"      T2 (dephasing)     {t2} s")

                # Channels
                channels = node_config.get("channels", [])
                for iface in node_config.get("matterLightInterfaceSettings", []):
                    channels = channels + iface.get("channels", [])
                if channels:
                    print(f"\nChannels ({len(channels)} total):")
                    print("  " + "-" * 56)
                    header = f"  {'Ch':<3} {'Type':<14} {'Dir':<5} {'Neighbor':<30}"
                    print(header)
                    print("  " + "-" * 56)
                    for ch in channels:
                        ch_id = ch.get('ID')
                        ch_type = ch.get("type", "N/A")
                        direction = ch.get("direction", "N/A")[:3]
                        neighbor = ch.get("neighbor", {})
                        neighbor_sys = neighbor.get("systemRef", "?")
                        neighbor_ch = neighbor.get("channelRef", "?")
                        neighbor_str = f"{neighbor_sys}:{neighbor_ch}"
                        line = f"  {ch_id:<3} {ch_type:<14} {direction:<5} {neighbor_str:<30}"
                        print(line)

            except (FileNotFoundError, json.JSONDecodeError) as e:
                print(f"\n  Error loading node config: {e}")

        # Protocol Configuration
        print("\nLink Adjacency Protocol:")
        print(f"  Hello Interval         {self._link_mgr.hello_interval} seconds")
        print(f"  Hold Time              {self._link_mgr.hold_time} seconds")

        # Runtime State
        print("\nRuntime State:")
        debug_status = "Enabled" if self._debug else "Disabled"
        probe_debug_status = "Enabled" if self._debug_probe else "Disabled"
        logging_status = "Enabled" if self._logging_console_enabled else "Disabled"
        print(f"  Debug Mode             {debug_status}")
        print(f"  Probe Debug Mode       {probe_debug_status}")
        print(f"  Console Logging        {logging_status}")
        print()

    def _cmd_logging_console(self, onoff: str):
        """Enable or disable logging output to console (like iOS 'logging console')."""
        if onoff == "on":
            self._logging_console_enabled = True
            self._update_repl_log_level()
            print("Logging to console enabled.")
        elif onoff == "off":
            self._logging_console_enabled = False
            self._update_repl_log_level()
            print("Logging to console disabled.")
        else:
            print("Usage: logging console / no logging console")

    def _cmd_link_monitor(self, mode: str = "link"):
        """Enter curses live dashboard mode."""
        try:
            curses.wrapper(lambda stdscr: self._monitor_dashboard(stdscr, initial_mode=mode))
        except KeyboardInterrupt:
            pass
        print("Returned to prompt.")

    def _monitor_dashboard(self, stdscr, initial_mode: str = "link"):
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

        mode = initial_mode

        while True:
            key = stdscr.getch()
            if key == ord("q"):
                break
            elif key == ord("p"):
                mode = "probe" if mode == "link" else "link"
            elif key == ord("r") and self._loop is not None and self._link_mgr is not None:
                for lkey, lentry in self._table.all().items():
                    if lentry.direction == "out" and lentry.is_quantum:
                        asyncio.run_coroutine_threadsafe(
                            self._link_mgr._send_probe(lkey, lentry), self._loop
                        )

            stdscr.clear()
            h, w = stdscr.getmaxyx()
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            if mode == "probe":
                self._render_probe_dashboard(stdscr, h, w, now, STATE_COLOR)
            else:
                title = f" Link Adjacency Monitor ── {self._node_name} ── {now} "
                stdscr.addstr(0, 0, title[:w - 1], curses.A_BOLD)
                stdscr.addstr(1, 0, "─" * (w - 1))

                # Header row
                stdscr.addstr(2, 0, f"{'Neighbor':<44} {'State':<14} {'Last RX':<12}", curses.A_BOLD)
                stdscr.addstr(3, 0, "─" * (w - 1))

                entries = self._table.all()
                row = 4
                split = max(h - 10, row + 2)
                for cid, entry in entries.items():
                    if row >= split:
                        break
                    color = curses.color_pair(STATE_COLOR.get(entry.state, 0))
                    line = f"{cid[:43]:<44} {entry.state:<14} {self._age(entry.last_hello_received):<12}"
                    stdscr.addstr(row, 0, line[:w - 1], color)
                    row += 1

                stdscr.addstr(split, 0, "─" * (w - 1))
                stdscr.addstr(split + 1, 0, "Recent Events", curses.A_BOLD)

                # Collect recent history from all entries
                events = []
                for cid, entry in entries.items():
                    for ts, state in entry.history:
                        events.append((ts, cid, state))
                events.sort(key=lambda x: x[0], reverse=True)

                for i, (ts, cid, state) in enumerate(events[:h - split - 4]):
                    color = curses.color_pair(STATE_COLOR.get(state, 0))
                    short = cid.split(":")[-2] if ":" in cid else cid
                    line = f"  {ts.strftime('%H:%M:%S')}  {short:<20} {state}"
                    stdscr.addstr(split + 2 + i, 0, line[:w - 1], color)

                stdscr.addstr(h - 2, 0, "─" * (w - 1))
                footer = " p: probe view | r: trigger probes | q: back to prompt "
                stdscr.addstr(h - 1, 0, footer[:w - 1], curses.A_REVERSE)

            stdscr.refresh()
            time.sleep(0.5)

    def _render_probe_dashboard(self, stdscr, h: int, w: int, now: str, state_color: dict):
        """Render live quantum probe telemetry and recent probe events in curses."""
        title = f" Quantum Probe Monitor ── {self._node_name} ── {now} "
        stdscr.addstr(0, 0, title[:w - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, "─" * (w - 1))

        header = (
            f"{'Quantum Link':<20} {'Dir':<4} {'State':<12} {'TX/RX':<6} "
            f"{'OK/Fl':<7} {'Status':<10} {'Age':<8} {'ToF / Optical / VLink'}"
        )
        stdscr.addstr(2, 0, header[:w - 1], curses.A_BOLD)
        stdscr.addstr(3, 0, "─" * (w - 1))

        entries = {k: v for k, v in self._table.all().items() if v.is_quantum}
        row = 4
        split = max(h - 11, row + 2)
        for key, entry in entries.items():
            if row >= split:
                break
            vlink = self._get_vlink_for_entry(entry)
            status = entry.last_probe_status or "never"
            if status == "ok":
                color = curses.color_pair(1)
            elif status == "never":
                color = curses.color_pair(4)
            else:
                color = curses.color_pair(3)

            if entry.direction == "out":
                state = entry.state
                tx_rx = f"T:{entry.probe_tx_count}"
                age = self._age(entry.last_probe_sent)
                detail = f"{vlink.get('vlink_id', '—')}" if vlink else "—"
                if entry.last_probe_info:
                    wl = entry.last_probe_info.get("wavelength")
                    rtt = entry.last_probe_info.get("rtt_ms")
                    if wl is not None:
                        detail += f" {wl}nm"
                    if rtt is not None:
                        detail += f" rtt={rtt}ms"
            else:
                state = "—"
                tx_rx = f"R:{entry.probe_rx_count}"
                age = self._age(entry.last_probe_received)
                detail = f"{vlink.get('vlink_id', '—')}" if vlink else "—"
                if entry.last_probe_info:
                    cnt = entry.last_probe_info.get("counts", 0)
                    tof = entry.last_probe_info.get("tof")
                    tof_str = f"{tof:.2e}s" if isinstance(tof, (int, float)) else "—"
                    detail += f" cnt={cnt} tof={tof_str}"

            ok_fl = f"{entry.probe_ok_count}/{entry.probe_fail_count}"
            line = (
                f"{key[:19]:<20} {entry.direction:<4} {state:<12} {tx_rx:<6} "
                f"{ok_fl:<7} {status[:9]:<10} {age:<8} {detail}"
            )
            stdscr.addstr(row, 0, line[:w - 1], color)
            row += 1

        stdscr.addstr(split, 0, "─" * (w - 1))
        stdscr.addstr(split + 1, 0, "Recent Probe Events (TX & RX Time-Tagger)", curses.A_BOLD)

        probe_events = []
        for key, entry in entries.items():
            for ts, pinfo in entry.probe_history:
                probe_events.append((ts, key, pinfo))
        probe_events.sort(key=lambda x: x[0], reverse=True)

        max_events = max(0, h - split - 4)
        for i, (ts, key, pinfo) in enumerate(probe_events[:max_events]):
            st = pinfo.get("status", "?")
            color = curses.color_pair(1 if st == "ok" else 3)
            pid = pinfo.get("probe_id", "—")
            if pinfo.get("direction") == "out":
                rtt = pinfo.get("rtt_ms")
                rtt_str = f"rtt={rtt}ms" if rtt is not None else ""
                wl = pinfo.get("wavelength", "—")
                pw = pinfo.get("power", "—")
                extra = f"({wl}nm, {pw}dBm) {rtt_str} {pinfo.get('reason') or ''}".strip()
                line = f"  {ts.strftime('%H:%M:%S')}  TX  {key:<18} id={pid:<8} {st:<9} {extra}"
            else:
                cnt = pinfo.get("counts", 0)
                tof = pinfo.get("tof")
                tof_str = f"{tof:.2e}s" if isinstance(tof, (int, float)) else "—"
                line = (
                    f"  {ts.strftime('%H:%M:%S')}  RX  {key:<18} id={pid:<8} {st:<9} "
                    f"counts={cnt} tof={tof_str}"
                )
            stdscr.addstr(split + 2 + i, 0, line[:w - 1], color)

        stdscr.addstr(h - 2, 0, "─" * (w - 1))
        footer = " p: link view | r: trigger probes | q: back to prompt "
        stdscr.addstr(h - 1, 0, footer[:w - 1], curses.A_REVERSE)

    def _print_help(self):
        print("""
Commands:
  show link                      Show all links and their state
  show link <neighbor>           Detailed view of one link
  show probe                     Show quantum probe summary & drivers
  show probe <neighbor>          Detailed quantum probe & time-tagger history
  show logging                   Show console logging status
  show config                    Show agent & link configuration
  show switch-check <neighbor>   Query switch port for a link
  connect <neighbor>             Trigger link adjacency
  disconnect <neighbor>          Bring a link to DOWN
  probe <neighbor> [count]       Trigger quantum probe (with live result)
  set hello-interval <n>         Set hello interval (seconds)
  set hold-time <n>              Set hold time (seconds)
  debug link                     Enable link debug logging
  no debug link                  Disable link debug logging
  debug probe                    Enable live quantum probe debug output
  no debug probe                 Disable live quantum probe debug output
  logging console                Enable logging to console
  no logging console             Disable logging to console
  monitor                        Live link adjacency dashboard (p/r/q)
  monitor probe                  Live quantum probe dashboard (p/r/q)
  help                           This message
  exit / quit                    Shut down agent
""")
