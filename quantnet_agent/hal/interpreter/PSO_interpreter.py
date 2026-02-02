import logging
from quantnet_agent.hal.interpreter.experiment_interpreter import ExperimentInterpreter
from quantnet_mq.schema.models import calibration
from quantnet_agent.hal.interpreter.PSO.PSO import PSO
from quantnet_agent.common.constants import Constants

log = logging.getLogger(__name__)


class PSOInterpreter(ExperimentInterpreter):

    def __init__(self, hal):
        super().__init__(hal, calibration, "calibration")
        self.PSO = None

    async def run_experiment(self, exp_request):
        log.info("Received Link Calibration submit request")
        log.debug(f"{exp_request}")

        agent_comms_topic = f"{Constants.EXPERIMENT_TOPIC_BASE}/{self.expid}"

        self.PSO = PSO(self.hal, agent_comms_topic, cb=self._publish)
        await self.PSO.init()
        await self.PSO.run_Bob_H1_Stabilization()
        await self.PSO.run_Bob_D2_Stabilization()
        await self.PSO.run_Alice_H1D2_Stabilization()
        await self.PSO.run_Bob_H2_Stabilization()

    def get_workflow_state(self):
        """
        Get current PSO workflow state for integration with QFC workflow.

        Returns:
            dict: Current stabilization state and visibilities
        """
        if self.PSO is None:
            return {"initialized": False, "message": "PSO not initialized"}

        return {
            "initialized": True,
            "step1_success": self.PSO.step1_success,
            "step2_success": self.PSO.step2_success,
            "step3_success": self.PSO.step3_success,
            "step4_success": self.PSO.step4_success,
            "visibilities": {
                "bob_h1": self.PSO.step1_visibility,
                "bob_d2": self.PSO.step2_visibility,
                "bob_h2": self.PSO.step4_visibility,
                "alice_h1": self.PSO.H1_visibility,
                "alice_d2": self.PSO.D2_visibility,
                "alice_h2": self.PSO.H2_visibility,
            },
        }

    def verify_pso_stabilization(self):
        """
        Verify PSO stabilization meets thresholds for QFC operation.

        Returns:
            bool: True if all stabilization steps are successful
        """
        if self.PSO is None:
            return False

        return self.PSO.step1_success and self.PSO.step2_success and self.PSO.step3_success and self.PSO.step4_success
