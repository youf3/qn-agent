import time
import serial
import numpy as np
import asyncio
from quantnet_agent.hal.hwclasses import Filter
import logging

log = logging.getLogger(__name__)


class CaltechEPC(Filter):  # remember to install the polctrl_firmware that has mystrtrod function
    def __init__(self, property, node, mq_broker_host, mq_broker_port):
        self.com_port = property.get("device")
        self.baudrate = int(property.get("baudrate", "9600"))
        self.timeout = float(property.get("timeout", "0.5"))
        self.device = None

    async def connect(self):
        if self.device is None:
            # print("Polarization Controller not connected. Connecting now...")
            self.device = serial.Serial(port=self.com_port, baudrate=self.baudrate, timeout=self.timeout)
            log.debug("Polarization Controller connected")
            await asyncio.sleep(1)
        else:
            log.warning("Polarization Controller already connected.")

    async def polarize(self, voltages, sleep_time=0.025):
        log.debug(f'Voltages from inside V set: {voltages}')
        try:
            if self.device is None:
                log.warning("Pol CTRL not connected")
                await self.connect()
            commands = [f"Vset {i+1} {volt}\n" for i, volt in enumerate(voltages)]

            start_time = time.time()
            for command in commands:
                # tic = time.time()
                await self._send_command(command, start_time)
                # print(f"Time taken to set voltage:{time.time()-tic}")
            await asyncio.sleep(sleep_time)  # Needs 0.1s max for stabilization

        except Exception as e:
            print(e)
            log.error("Could not set the polarization controller voltage")

    async def attenuate(self, strength):
        return await super().attenuate(strength)

    async def _send_command(self, command, start_time):
        dataStr = ""

        if self.device is None:
            await self.connect()

        while dataStr != "+ok":
            log.debug(f"Sending command: {command.strip()}")
            self.device.write(bytes(command, "utf-8"))
            data = self.device.readline()
            dataStr = data.decode().strip()
            log.debug(
                f" {dataStr} received at {self.com_port} and running time: {time.time() - start_time:.2f} seconds"
            )
            await asyncio.sleep(0.1)

    async def cleanUp(self):
        log.info("Clening up the Polarization Controller")


async def start():
    polCTRL = CaltechEPC({"device": "/dev/ttyACM6"}, None, None, None)  # EPC1 COM22;EPC2 COM21; EPC A COM20
    await polCTRL.connect()
    await polCTRL.polarize(np.array([0, 0, 0]))
    await polCTRL.cleanUp()


if __name__ == "__main__":
    asyncio.run(start())
