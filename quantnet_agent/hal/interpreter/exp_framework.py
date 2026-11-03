import logging
from quantnet_agent.hal.HAL import ScheduleableInterpreter
from quantnet_mq.schema.models import experiment

log = logging.getLogger(__name__)


class ExperimentFramework(ScheduleableInterpreter):

    def __init__(self, hal):
        super().__init__(hal)
        self.parameters = {}

    async def submit(self, *exp_info, **exp_param):
        log.info(f"Received Experiment submit request : {exp_info} {exp_param}")
        exp_name = exp_info[0].expName._value
        class_name = exp_info[0].className._value if "className" in exp_info[0] else None
        params = {}
        for i in exp_info[0].parameters.data:
            item = i.as_dict() if hasattr(i, "as_dict") else dict(i.items())
            if "name" in item and "value" in item and len(item) == 2:
                params[item["name"]] = item["value"]
            else:
                params.update(item)
        self.parameters = dict(params)
        exp_id = exp_param["exp_id"]
        await self.hal.devs["exp_framework"].submit(exp_id, exp_name, class_name, params)

    async def update_result(self, exp_id):
        log.info(f"Getting experiment result for {exp_id}")
        result = await self.hal.devs["exp_framework"].receive(exp_id)
        return result

    def get_state(self, request):
        log.info(f"Received Experiment get_state request : {request}")
        pass

    def get_info(self, request):
        log.info(f"Received Experiment get_info request : {request}")
        pass

    def set_value(self, request):
        log.info(f"Received Experiment set_value request : {request}")
        for k, v in request.payload.items():
            self.parameters[k] = v

    def get_commands(self):
        commands = {
            "experiment.getState": [self.get_state, "quantnet_mq.schema.models.experiment.getState"],
            "experiment.getInfo": [self.get_info, "quantnet_mq.schema.models.experiment.getInfo"],
            "experiment.setValue": [self.set_value, "quantnet_mq.schema.models.experiment.setValue"],
        }
        return commands

    def cancel(self, request):
        log.info(f"Received Experiment cancel request : {request}")
        pass

    def get_schedulable_commands(self):
        commands = {
            "experiment.submit": [
                self.submit,
                "quantnet_mq.schema.models.experiment.submit",
                experiment.submitResponse,
                self.update_result
            ],
            "experiment.getResult": [
                self.update_result,
                "quantnet_mq.schema.models.experiment.getResult",
                experiment.getResultResponse,
                None
                ],
            "experiment.cancel": [
                self.cancel,
                "quantnet_mq.schema.models.experiment.cancel",
                experiment.cancelResponse,
                None
            ]
        }
        return commands
