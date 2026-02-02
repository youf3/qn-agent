import pyvisa
import logging
import asyncio
from typing import Optional, Literal
from quantnet_agent.hal.hwclasses import AnalogController

log = logging.getLogger(__name__)

Mode = Literal["burst_sine", "square", "dc"]


class RigolDG4162(AnalogController):
    def __init__(self, property, node, mq_broker_host, mq_broker_port):
        self.resource = property.get("device")
        self.rm = None
        self.dev = None
        self.idn = None

    async def connect(self):
        """Connect to the Rigol DG4162 function generator."""
        self.rm = pyvisa.ResourceManager()
        self.dev = self.rm.open_resource(self.resource)
        self.dev.timeout = 5000
        self.idn = self.dev.query("*IDN?").strip()
        log.info(f"Connected to {self.resource}: {self.idn}")

    async def configure(
        self,
        channel,
        mode: Optional[str] = None,
        # Legacy burst_sine parameters (for backward compatibility)
        frequency=None,
        amplitude=None,
        offset=None,
        phase=None,
        burst=None,
        burst_cycles=None,
        burst_period=None,
        burst_delay=None,
        trigger_source=None,
        # New mode-specific parameters
        amplitude_vpp=None,
        offset_v=None,
        phase_deg=None,
        # burst_sine specific
        frequency_hz=None,
        burst_delay_s=None,
        burst_period_s=None,
        # square specific
        square_freq_hz=None,
        duty_percent=None,
        pulse_width_s=None,
        # dc specific
        dc_level_v=None,
        # Raw command override
        raw_command=None,
    ):
        """
        Configure a Rigol DG4162 channel with specified waveform and burst settings.

        Supports three modes:
        - burst_sine: Burst sine wave with trigger
        - square: Square wave (continuous or burst)
        - dc: DC voltage output

        For backward compatibility, if mode is not specified, uses legacy burst_sine behavior.
        """
        if raw_command is not None:
            self.dev.write(raw_command)
            return

        ch = int(channel)
        if ch not in (1, 2):
            raise ValueError("channel must be 1 or 2")

        # Turn output OFF during configuration
        self.dev.write(f":OUTPut{ch} OFF")

        # Determine mode
        if mode is None:
            # Legacy mode - assume burst_sine
            mode = "burst_sine"
            # Map legacy parameters to new names
            frequency_hz = frequency or frequency_hz or 80e6
            amplitude_vpp = amplitude or amplitude_vpp or 1.5
            offset_v = offset if offset is not None else (offset_v if offset_v is not None else 0)
            phase_deg = phase if phase is not None else (phase_deg if phase_deg is not None else 0)
            burst_cycles = burst_cycles or 800
            burst_period_s = burst_period or burst_period_s or 100e-6
            burst_delay_s = burst_delay or burst_delay_s or 33.4e-6
            trigger_source = trigger_source or "EXT"

        mode = mode.lower()

        if mode == "burst_sine":
            # Validate required parameters
            if frequency_hz is None or amplitude_vpp is None:
                raise ValueError("burst_sine requires frequency_hz and amplitude_vpp")
            if burst_cycles is None or burst_period_s is None:
                raise ValueError("burst_sine requires burst_cycles and burst_period_s")

            self.dev.write(f":SOURce{ch}:FUNCtion SIN")
            self.dev.write(f":SOURce{ch}:FREQuency {frequency_hz}")
            self.dev.write(f":SOURce{ch}:VOLTage {amplitude_vpp}")
            self.dev.write(f":SOURce{ch}:VOLTage:OFFSet {offset_v or 0.0}")
            self.dev.write(f":SOURce{ch}:PHASe {phase_deg or 0.0}")

            # Burst configuration
            self.dev.write(f":SOURce{ch}:BURSt:STATe ON")
            self.dev.write(f":SOURce{ch}:BURSt:MODE TRIG")
            self.dev.write(f":SOURce{ch}:BURSt:NCYCles {burst_cycles}")
            self.dev.write(f":SOURce{ch}:BURSt:INT:PERiod {burst_period_s}")
            self.dev.write(f":SOURce{ch}:BURSt:TDELay {burst_delay_s or 0.0}")

            trig_src = (trigger_source or "INT").upper()
            self.dev.write(f":SOURce{ch}:BURSt:TRIGger:SOURce {trig_src}")

            log.debug(
                f"CH{ch} configured: {frequency_hz/1E6:.1f}MHz Sine, Burst Mode, "
                f"{burst_cycles} Cycles, Delay {(burst_delay_s or 0)*1E6:.1f}µs, "
                f"Trigger: {trig_src}"
            )

        elif mode == "square":
            if square_freq_hz is None or amplitude_vpp is None:
                raise ValueError("square requires square_freq_hz and amplitude_vpp")

            self.dev.write(f":SOURce{ch}:FUNCtion SQUare")
            self.dev.write(f":SOURce{ch}:FREQuency {square_freq_hz}")
            self.dev.write(f":SOURce{ch}:VOLTage {amplitude_vpp}")
            self.dev.write(f":SOURce{ch}:VOLTage:OFFSet {offset_v or 0.0}")
            self.dev.write(f":SOURce{ch}:PHASe {phase_deg or 0.0}")

            # Handle duty cycle
            if pulse_width_s is not None:
                # Convert pulse width to duty cycle
                T = 1.0 / square_freq_hz
                duty = max(0.0, min(100.0, 100.0 * (pulse_width_s / T)))
                self.dev.write(f":SOURce{ch}:FUNCtion:SQUare:DCYCle {duty}")
            else:
                duty = duty_percent if duty_percent is not None else 50.0
                duty = max(0.0, min(100.0, duty))
                self.dev.write(f":SOURce{ch}:FUNCtion:SQUare:DCYCle {duty}")

            # Ensure burst is OFF for plain square mode
            self.dev.write(f":SOURce{ch}:BURSt:STATe OFF")

            log.debug(f"CH{ch} configured: {square_freq_hz/1E6:.1f}MHz Square, " f"{duty:.1f}% duty cycle")

        elif mode == "dc":
            # DC level is set via offset
            level = dc_level_v
            if level is None:
                level = offset_v if offset_v is not None else 0.0

            self.dev.write(f":SOURce{ch}:FUNCtion DC")
            self.dev.write(f":SOURce{ch}:VOLTage:OFFSet {level}")
            # Burst off
            self.dev.write(f":SOURce{ch}:BURSt:STATe OFF")

            log.debug(f"CH{ch} configured: DC mode, {level:.3f} V")

        else:
            raise ValueError(f"Unknown mode: {mode}")

    async def set(self, channel, setting):
        """
        Turn a specific channel ON or OFF.
        """
        log.debug(f"Setting Rigol channel {channel} to {setting}")
        self.dev.write(f"OUTP{channel} {setting}")
        log.debug(f"CH{channel} turned {setting}.")

    async def cleanUp(self):
        """Disconnect from the Rigol DG4162 function generator."""
        if self.dev:
            self.dev.close()
            log.info("Connection closed.")
        self.dev = None
        return await super().cleanUp()


async def start():

    Rigol1 = RigolDG4162({"device": "TCPIP0::10.0.0.203::INSTR"}, None, None, None)  # Use this for LBNL Rigol

    await Rigol1.connect()

    await Rigol1.configure(
        channel="1", frequency=80.0e6, amplitude=1.5, burst_delay=000 * 1e-9, burst_cycles=80, trigger_source="EXT"
    )
    await Rigol1.set(1, "ON")

    await Rigol1.configure(
        channel="2", frequency=80e6, amplitude=1.5, burst_delay=000 * 1e-9, burst_cycles=80, trigger_source="EXT"
    )
    await Rigol1.set(2, "OFF")

    Rigol2 = RigolDG4162({"device": "TCPIP0::10.0.0.201::INSTR"}, None, None, None)  # UCB Rigol
    await Rigol2.connect()

    await Rigol2.configure(
        channel="2", frequency=80e6, amplitude=1.5, burst_delay=0, burst_cycles=80, trigger_source="EXT"
    )
    await Rigol2.set(2, "ON")
    await Rigol1.cleanUp()
    await Rigol2.cleanUp()


if __name__ == "__main__":
    asyncio.run(start())
