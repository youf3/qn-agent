import logging
from quantnet_agent.hal.interpreter.experiment_interpreter import ExperimentInterpreter
from quantnet_mq.schema.models import experiment

log = logging.getLogger(__name__)


class ExperimentFramework(ExperimentInterpreter):

    def __init__(self, hal):
        super().__init__(hal, experiment, "experiment")

    async def run_experiment(self, exp_request):
        log.info(f"Running experiment: {exp_request}")
        exp_name = exp_request.expName._value
        class_name = exp_request.className._value if "className" in exp_request else None
        await self.hal.devs["exp_framework"].submit(self.expid, exp_name, class_name, self.parameters)

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
