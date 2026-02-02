import logging
import json
from datetime import datetime, timezone
from abc import abstractmethod
from quantnet_agent.hal.HAL import ScheduleableInterpreter
from quantnet_mq.schema.models import (
    agentSubmitResponse,
    Status,
)

log = logging.getLogger(__name__)


class ExperimentInterpreter(ScheduleableInterpreter):
    def __init__(self, hal, schema_module, command_namespace):
        super().__init__(hal)
        self.schema_module = schema_module
        self.command_namespace = command_namespace
        self.parameters = {}
        self.set_agent_commands()
        self.msgtopic = "experiment_data"
        self.expid = ""

    def set_agent_commands(self):
        commands = [("agentSubmit", None, "quantnet_mq.schema.models.agentSubmit")]
        for cmd in commands:
            self.hal._rpcclient.set_handler(*cmd)

    def get_commands(self):
        return {}

    async def update_result(self, exp_id):
        log.info(f"Getting result for {exp_id}")
        return {"result": ""}

    def cancel(self, request):
        log.info(f"Received cancel request : {request}")
        pass

    async def _publish(self, payload):
        """Protected publish method for experiment data."""
        msg = json.dumps({"expid": self.expid, "ts": datetime.now(timezone.utc).timestamp(), "data": payload})
        await self.hal._msgclient.publish(self.msgtopic, msg)

    async def submit(self, *exp_info, **exp_param):
        log.info(f"Received {self.command_namespace} submit request")

        self.expid = exp_param["exp_id"]._value

        if hasattr(exp_info[0], "parameters") and hasattr(exp_info[0].parameters, "data"):
            for i in exp_info[0].parameters.data:
                for k, v in i.items():
                    self.parameters[k] = v

        await self.run_experiment(exp_info[0])

    @abstractmethod
    async def run_experiment(self, exp_request):
        """
        Abstract method to run the specific experiment logic.

        Args:
            exp_request: The request object passed to submit (exp_info[0])
        """
        pass

    def get_schedulable_commands(self):
        schema = self.schema_module
        ns = self.command_namespace

        return {
            f"{ns}.submit": [
                self.submit,
                f"quantnet_mq.schema.models.{ns}.submit",
                schema.submitResponse,
                None,
            ],
            f"{ns}.getResult": [
                self.update_result,
                f"quantnet_mq.schema.models.{ns}.getResult",
                schema.getResultResponse,
                None,
            ],
            f"{ns}.cancel": [
                self.cancel,
                f"quantnet_mq.schema.models.{ns}.cancel",
                schema.cancelResponse,
                None,
            ],
        }
