import time
import serial
import asyncio
import logging
from quantnet_agent.hal.hwclasses import Filter

log = logging.getLogger(__name__)


class CaltechPSG(Filter):
    def __init__(self, property, node, mq_broker_host, mq_broker_port):
        self.com_port = property.get("device")
        self.baudrate = int(property.get("baudrate", "9600"))
        self.timeout = float(property.get("timeout", "0.5"))
        self.dev = None
        self.states = {"H": "S 7\n", "V": "S 5\n", "D": "S 4\n", "A": "S 6\n", "L": "S 2\n", "R": "S 13\n"}

    async def connect(self):
        if self.dev is None:
            self.dev = serial.Serial(port=self.com_port, baudrate=self.baudrate, timeout=self.timeout)
            log.info("PSG connected")
            time.sleep(1)
        else:
            log.info("PSG already connected.")

    async def polarize(self, PSGpol):
        if self.dev is None:
            await self.connect()

        if PSGpol not in self.states:
            log.error(f"Invalid polarization state: {PSGpol}")
            return
        
        PSGpol = self.states[PSGpol]

        if self.dev is not None:  # Ensure self.dev is still connected
            try:
                log.debug(f"Setting PSG to {PSGpol}")
                self.dev.write(PSGpol.encode())
                self.dev.read(20)
            except serial.SerialException as e:
                log.info(f"Error sending command {PSGpol} to PSG: {e}")
        else:
            log.info("Failed to connect to PSG. Cannot send command.")

    async def attenuate(self, strength):
        return await super().attenuate(strength)

    async def cleanUp(self):
        if self.dev is not None:
            self.dev.close()
            log.info("####### PSG is disconnected ############# ")
            self.dev = None
        else:
            log.info("No PSG to disconnect.")


async def start():
    # Replace with the correct COM port for your device
    COM_PORT = "/dev/ttyACM3"  # (Alice) Adjust based on your hardware setup
    PSG = CaltechPSG({"device": COM_PORT}, None, None, None)  # Initialize with the correct parameters
    try:
        # Step 1: Connect to the PSG
        await PSG.connect()
        print(PSG.dev)

        # Step 2: Measure timing for a polSET operation
        start_time = time.time()
        await PSG.polarize(PSG.H)  # Example: Setting polarization to Horizontal (H)
        end_time = time.time()

        # Step 3: Print the time taken for the operation
        print(f"Time taken for polSET operation: {end_time - start_time:.6f} seconds")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Step 4: Disconnect the PSG
        await PSG.cleanUp()
        pass


if __name__ == "__main__":
    asyncio.run(start())
