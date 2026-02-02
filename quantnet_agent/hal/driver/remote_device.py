import json
import logging

log = logging.getLogger(__name__)


class RemoteDevice:
    def __init__(self, name, rpcclient, topic):
        self.name = name
        self.rpcclient = rpcclient
        self.agent_comms_topic = topic

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


class RemotePSG(RemoteDevice):
    async def polarize(self, polarization):
        return await self.execute("polarize", {"PSGpol": polarization})
