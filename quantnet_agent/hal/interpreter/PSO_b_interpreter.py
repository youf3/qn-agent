import logging
import asyncio
from quantnet_agent.hal.HAL import ScheduleableInterpreter
from quantnet_mq.schema.models import calibration
from quantnet_mq.rpcserver import RPCServer
from quantnet_mq.schema.models import agentSubmitResponse, Status
from quantnet_mq import Code
from quantnet_agent.common.constants import Constants

log = logging.getLogger(__name__)


class PSOInterpreter(ScheduleableInterpreter):

    def __init__(self, hal):
        super().__init__(hal)
        self.parameters = {}
        self.rpcserver = RPCServer(
            self.hal._config.cid + "PSO-atoa",
            topic=f"{Constants.EXPERIMENT_TOPIC_BASE}/+",
            host=self.hal._config.mq_broker_host,
            port=self.hal._config.mq_broker_port,
        )
        self.register_atoa_server_commands()
        loop = asyncio.get_running_loop()
        loop.create_task(self.rpcserver.start())

    async def submit(self, *exp_info, **exp_param):
        log.info(f"Received Link Calibration submit request : {exp_info} {exp_param}")
        for i in exp_info[0].parameters.data:
            for k, v in i.items():
                self.parameters[k] = v

    async def update_result(self, exp_id):
        log.info(f"Getting calibration result for {exp_id}")
        return {"result": ""}

    def _sanitize_parameters(self, parameters):
        """Convert python_jsonschema_objects literals to regular Python types."""
        sanitized = {}
        for key, value in parameters.items():
            # Check if value has a _value attribute (literal object)
            if hasattr(value, "_value"):
                sanitized[key] = value._value
            # Handle nested objects/dicts
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_parameters(value)
            # Handle lists
            elif isinstance(value, list):
                sanitized[key] = [item._value if hasattr(item, "_value") else item for item in value]
            else:
                sanitized[key] = value
        return sanitized

    async def agent_submit(self, args):
        log.info(f"Received agent submit request : {args}")
        device = args["payload"]["device"]
        function = args["payload"]["function"]
        parameters = args["payload"]["function"]["parameters"]
        sanitized_params = self._sanitize_parameters(parameters)
        try:
            func_obj = getattr(self.hal.devs[device], function.name._value)
            rc = Code.OK
            message = await func_obj(**sanitized_params)
        except Exception as e:
            log.error(f"Error in agent submit: {type(e)}:{e}")
            rc = Code.INVALID_ARGUMENT
            message = f"Faild to run {device}.{function.name._value} with parameters {parameters}. Error: {e}"
        finally:
            return agentSubmitResponse(status=Status(code=rc.value, value=rc.name), message=message)

    def register_atoa_server_commands(self):
        commands = {"agentSubmit": [self.agent_submit, "quantnet_mq.schema.models.agentSubmit"]}
        for cmd in commands:
            self.rpcserver.set_handler(cmd, commands[cmd][0], commands[cmd][1])

    def get_commands(self):
        commands = {}
        return commands

    def cancel(self, request):
        log.info(f"Received calibration cancel request : {request}")
        pass

    def get_schedulable_commands(self):
        commands = {
            "calibration.submit": [
                self.submit,
                "quantnet_mq.schema.models.calibration.submit",
                calibration.submitResponse,
                None,
            ],
            "calibration.getResult": [
                self.update_result,
                "quantnet_mq.schema.models.calibration.getResult",
                calibration.getResultResponse,
                None,
            ],
            "calibration.cancel": [
                self.cancel,
                "quantnet_mq.schema.models.calibration.cancel",
                calibration.cancelResponse,
                None,
            ],
        }
        return commands
