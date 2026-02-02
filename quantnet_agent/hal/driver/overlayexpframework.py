from quantnet_agent.hal.hwclasses import ExpFramework
import logging
import socket
import asyncio


log = logging.getLogger(__name__)


class OverlayExpFramework(ExpFramework):

    def __init__(self, *args, **kwargs):
        props = args[0]
        self.bsm_ip = props['bsm_ip']
        self.bsm_port = int(props['bsm_port'])
        super().__init__(*args, **kwargs)

    @property
    def status(self):
        return self._status

    async def submit(self, exp_id, expName, classname, args=dict()):
        log.info(f"submitting bsm to {self.bsm_ip}")
        await self._send_message_async(self.bsm_ip, self.bsm_port, b'bsm')

    async def receive(self, exp_id):
        return {"result": ""}

    @property
    def logs(self):
        pass

    async def cleanUp(self):
        pass

    async def _send_message_async(self, ip, port, msg):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._send_message, ip, port, msg)

    def _send_message(self, ip, port, msg):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        s.sendall(msg)
        result = s.recv(1024)
        if result != b'0':
            raise Exception(f"Failed to handle {msg}")
        s.close()
