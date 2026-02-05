import json
import logging
from quantnet_agent.hal.hwclasses import Device, Filter

log = logging.getLogger(__name__)


class RemoteDevice(Device):
    def __init__(self, property, node, mq_broker_host, mq_broker_port, rpcclient=None):
        self.name = property.get("device")
        self.rpcclient = rpcclient
        self.agent_comms_topic = property.get("topic", None)

    async def execute(self, function_name, parameters=None):
        if parameters is None:
            parameters = {}
        msg = {"device": self.name, "function": {"name": function_name, "parameters": parameters}}
        result = await self.rpcclient.call("agentSubmit", msg, topic=self.agent_comms_topic)

        try:
            parsed_result = json.loads(result)
            if parsed_result["status"]["code"] != 0:
                raise RuntimeError(f"Remote device error on {self.name}.{function_name}: {parsed_result['message']}")
            return result
        except json.JSONDecodeError:
            log.error(f"Failed to decode response from {self.name}: {result}")
            return result

    async def connect(self):
        return await self.execute("connect")


class RemotePSG(RemoteDevice, Filter):
    def __init__(self, property, node, mq_broker_host, mq_broker_port, rpcclient=None):
        super().__init__(property, node, mq_broker_host, mq_broker_port, rpcclient=rpcclient)

    async def polarize(self, polarization):
        return await self.execute("polarize", {"PSGpol": polarization})

    async def attenuate(self, strength):
        return await self.execute("attenuate", {"strength": strength})

    async def cleanUp(self):
        return await self.execute("cleanUp")
