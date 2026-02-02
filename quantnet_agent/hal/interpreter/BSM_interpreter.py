import logging
from quantnet_agent.hal.interpreter.experiment_interpreter import ExperimentInterpreter
from quantnet_mq.schema.models import experiment
from quantnet_agent.hal.interpreter.PSO.BSM import BSM
from quantnet_agent.common.constants import Constants

log = logging.getLogger(__name__)


class BSMInterpreter(ExperimentInterpreter):

    def __init__(self, hal):
        super().__init__(hal, experiment, "experiment")

    async def run_experiment(self, exp_request):
        exp_name = exp_request.expName._value
        agent_comms_topic = f"{Constants.EXPERIMENT_TOPIC_BASE}/{self.expid}"

        if exp_name == "BSM":
            # Pass self._publish as callback to BSM
            self.BSM = BSM(self.hal, self.hal._msgclient, agent_comms_topic, cb=self._publish)
            await self.BSM.init_device()
            await self.BSM.bsm()
        else:
            raise ValueError(f"Unknown experiment name: {exp_name}")
