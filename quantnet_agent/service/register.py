import asyncio
import json
import logging
from datetime import datetime, timezone
from quantnet_mq.schema.models import monitor
from quantnet_agent.common.constants import Constants

log = logging.getLogger(__name__)


class Register:
    def __init__(self, cid, node_config, mqhost, mqport, rpcclient, msgclient):
        self._cid = cid
        self._node = node_config
        self._mqhost = mqhost
        self._mqport = mqport
        self._client = rpcclient
        self._rpc_client_handlers = [
            ("register", self.register_response, "quantnet_mq.schema.models.agentRegister"),
            ("deregister", self.deregister_response, "quantnet_mq.schema.models.agentDeregister"),
            ("update", self.update_response, "quantnet_mq.schema.models.agentRegister"),
        ]
        self._msgclient = msgclient
        self.registered = False
        self.started = False

    async def register_response(self):
        pass

    async def deregister_response(self):
        pass

    async def update_response(self):
        pass

    async def start(self):
        for rh in self._rpc_client_handlers:
            self._client.set_handler(rh[0], rh[1], rh[2])
        await self._client.start()
        with open(self._node, "r") as nf:
            data = nf.read()
        try:
            conf = json.loads(data)
        except Exception as e:
            log.error(f"Could not load node definition: {e}")
            return
        self.started = True
        while self.started and not self.registered:
            try:
                await self._client.call("register", conf)
                self.registered = True
            except Exception as e:
                log.error(
                    f"Could not register configuration: {e}. retrying in {Constants.REGISTRATION_RETRY_INTERVAL} sec"
                )
            await asyncio.sleep(Constants.REGISTRATION_RETRY_INTERVAL)
        asyncio.create_task(self._heartbeat())

    async def stop(self):
        self.started = False
        await self._client.call("deregister", None)

    async def _heartbeat(self):
        seq = 1
        while True:
            log.debug(f"Sending heartbeat, seq {seq}")
            msg = monitor.MonitorEvent(rid=self._cid,
                                       ts=datetime.now(timezone.utc).timestamp(),
                                       eventType="agentHeartbeat",
                                       value=seq)
            await self._msgclient.publish("monitor", msg.as_dict())
            seq += 1
            await asyncio.sleep(Constants.HEARTBEAT_INTERVAL)

    async def update(self, conf):
        await self._client.call("update", conf)
