"""
Copyright (c) 2023- ESnet

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v2.0
and Eclipse Distribution License v1.0 which accompany this distribution.
"""

from enum import Enum
import logging
import json
import asyncio
from collections import namedtuple
from quantnet_agent.hal.hwclasses import LightMeasurement
from quantnet_mq.rpcclient import RPCClient
from quantnet_mq.rpcserver import RPCServer


log = logging.getLogger(__name__)

rpc_topic_prefix = "rpc/simulation"
rpcserver_topic_prefix = "rpc/simulationDriver"

QUANTNET_SIM = "quantsim"


class DeviceProtocol(Enum):
    QNSIMBASE = "qnsim_protocol"
    MANAGEMENT = "management_protocol"
    LIGHTSRC = "lightsrcprotocol"
    EPC = "epcprotocol"

    def __str__(self):
        return self.value


class QNsimResponseHandler:
    def __init__(self):
        """
        RPC command handlers
            (cmd name, handle function, class full path)
        """
        self._handlers = [
            ("response", self.handle_register, "quantnet_mq.schema.models.agentRegister"),
        ]

    @property
    def rpccmdhandlers(self):
        return self._handlers

    async def handle_response(self, response):
        """handle Experiment submission"""
        log.info(f"Received experiment: {response.serialize()}")
        rc = 0
        if response.payload.type == "submit":
            expid = generate_uuid()
            agentId = response.payload.agentId
            response.payload["id"] = expid

            if agentId not in self._dispatchers:
                return submitExperimentResponse(
                    status=responseStatus(code=rc, value=Code(rc).name, reason=f"Agent ID : {agentId} not registered")
                )

            await self.scheduler.schedule(self._dispatchers[agentId].startExperiment, response.payload)
            return submitExperimentResponse(
                status=responseStatus(code=rc, value=Code(rc).name),
                experiments=[
                    {
                        "phase": "init",
                        "agentId": response.payload.agentId,
                        "expName": response.payload.experimentName,
                        "param": response.payload.expParameters,
                        "exp_id": expid,
                    }
                ],
            )

        elif response.payload.type == "get":
            exps = await self._calibrator.getExperiment(response)
            exps = [exps] if isinstance(exps, dict) else exps
            return submitExperimentResponse(status=responseStatus(code=rc, value=Code(rc).name), experiments=exps)
        else:
            raise Exception(f"unknown experiment cmd type {response.payload.type}")


class SimulatorDriver:
    """
    The base class for simulator drivers.

    Parameters
    ----------
    property: dict        device property from configuration
    node: str             node configuration file path
    mq_broker_host:str    message broker host
    mq_broker_port:str    message broker port
    """

    def __init__(self, property, node, mq_broker_host, mq_broker_port, **kwargs):
        # Extract device name from property or use default
        self._device = property.get("device", "simulator")
        # Extract node name from node config
        self._node_config = node
        node_name = json.load(open(node))["systemSettings"]["name"]
        self._node = node_name

        self._rpcClient = None
        self._rpcServer = None
        self._timeout = 20
        self._result = None
        self._mq_broker_host = mq_broker_host
        self._mq_broker_port = mq_broker_port

        self._clihandlers = [
            ("simulation.delegate", None, "quantnet_mq.schema.models.simulation.delegation"),
        ]

        self._serverhandlers = [
            (
                "simulation.delegationResult",
                self.handle_sim_result,
                "quantnet_mq.schema.models.simulation.delegationResult",
            ),
        ]
        self._started = False

    @property
    def result(self):
        return self._result

    async def start(self):
        try:
            self._rpcClient = RPCClient(
                f"SimulatorDriver_{self._node}_{self._device}",
                # topic=f"{rpc_topic_prefix}/{self._node}",
                host=self._mq_broker_host,
                port=self._mq_broker_port,
            )
            for h in self._clihandlers:
                self._rpcClient.set_handler(h[0], h[1], h[2])
            await self._rpcClient.start()
        except Exception as e:
            raise Exception(f"Can not start RPCClient object: {e}")

        try:
            self._rpcServer = RPCServer(
                f"SimulatorDriver_{self._node}_{self._device}",
                topic=f"{rpcserver_topic_prefix}/{self._node}",
                host=self._mq_broker_host,
                port=self._mq_broker_port,
            )
            for h in self._serverhandlers:
                self._rpcServer.set_handler(h[0], h[1], h[2])
            await self._rpcServer.start()
        except Exception as e:
            raise Exception(f"Can not start RPCServer object: {e}")

        self._started = True

    async def stop(self):
        await self._rpcClient.start()
        await self._rpcServer.stop()
        self._started = False

    async def _sendRPC(self, data):
        payload = {}
        payload["node"] = self._node
        payload["device"] = self._device
        payload["data"] = data if isinstance(data, dict) else json.loads(data.serialize())

        agent = QUANTNET_SIM
        delegateResp = await self._rpcClient.call(
            "simulation.delegate", payload, topic=f"{rpc_topic_prefix}/{agent}", timeout=self._timeout
        )
        resp = json.loads(delegateResp)
        return resp

    async def handle_sim_result(self, result):
        """handle qnsim result"""
        log.info(f"Received quantnet_sim result: {result.serialize()}")
        self._result = json.loads(result.payload.data["message"].serialize())


class SimLightsrcDriver(SimulatorDriver):
    """
    The lightsource simulator driver
    """

    def __init__(self, property, node, mq_broker_host, mq_broker_port, **kwargs):
        super().__init__(property, node, mq_broker_host, mq_broker_port, **kwargs)
        log.info("Initializing SimLightsrcDriver")

    async def src_init(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received src_init response: {resp}")
        return resp["status"]["code"]

    async def generate(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received generate response: {resp}")
        return resp["status"]["code"]

    async def cleanup(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received cleanup response: {resp}")
        return resp["status"]["code"]


class SimEpcDriver(SimulatorDriver):
    """
    The EPC simulator driver
    """

    def __init__(self, property, node, mq_broker_host, mq_broker_port, **kwargs):
        super().__init__(property, node, mq_broker_host, mq_broker_port, **kwargs)
        log.info("Initializing SimEpcDriver")

    async def dst_init(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received dst_init response: {resp}")
        return resp["status"]["code"]

    async def calibrate(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received calibrate response: {resp}")
        return resp["status"]["code"]

    async def cleanup(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received cleanup response: {resp}")
        return resp["status"]["code"]


class SimPolarimeterDriver(SimulatorDriver, LightMeasurement):
    """
    The polarimeter simulator driver
    """

    def __init__(self, property, node, mq_broker_host, mq_broker_port, **kwargs):
        super().__init__(property, node, mq_broker_host, mq_broker_port, **kwargs)
        log.info("Initializing simulator Polarimeter")

    async def dst_init(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received dst_init response: {resp}")
        return resp["status"]["code"]

    async def measure(self, request):
        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received measure response: {resp}")
        return resp["status"]["code"]


class SimEGPDriver(SimulatorDriver):
    """
    The EGP protocol simulator driver
    """

    def __init__(self, property, node, mq_broker_host, mq_broker_port, **kwargs):
        # Override device extraction for EGP - uses 'protocol' field
        egp_property = property.copy()
        egp_property["device"] = property.get("protocol", "egp")
        super().__init__(egp_property, node, mq_broker_host, mq_broker_port, **kwargs)
        log.info("Initializing simulator EGP driver.")

    # TODO: send?
    async def req_service(self, request):

        if not self._started:
            await self.start()

        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received req_service response: {resp}")
        return resp

    # TODO: recv
    async def generate(self, request):

        if not self._started:
            await self.start()

        data = request
        resp = await self._sendRPC(data)
        log.info(f"Received generate response: {resp}")
        return resp


class PassthroughDriver(SimulatorDriver):
    """
    Passthrough Driver that send RPC messages to the given node.

    Parameters:
    -----------
    property: device property in the configuration
    node_config: The node configuration
    mqhost,mqport: message broker host name and port

    """

    max_retries = 10

    # message classes
    fields = ("cmd", "info")
    req_simulate = namedtuple("SimulationRequest", fields, defaults=(None,) * len(fields))

    def __init__(self, property, node, mq_broker_host, mq_broker_port, **kwargs):
        # Set device to QNSIMBASE protocol for passthrough
        pt_property = property.copy()
        pt_property["device"] = DeviceProtocol.QNSIMBASE.value
        super().__init__(pt_property, node, mq_broker_host, mq_broker_port, **kwargs)
        log.info("Initializing passthrough driver")

    async def send(self, data):
        """
        send data in RPC
        """

        if not self._started:
            await self.start()

        # data_dict = json.loads(data.serialize())
        out_message = PassthroughDriver.req_simulate(cmd="send", info=data.serialize())
        log.info(f"Sent request: {out_message}")
        resp = await self._sendRPC(out_message._asdict())
        log.info(f"Received response: {resp}")
        return resp

    async def get_result(self):
        """
        get the result
        """
        if not self._started:
            await self.start()

        # while not self.result:
        #     await asyncio.sleep(2)

        for attempt in range(PassthroughDriver.max_retries):
            if self.result:
                break
            if attempt < PassthroughDriver.max_retries - 1:
                await asyncio.sleep(2)
            else:
                raise Exception(f"All attempts failed.")

        return self.result
