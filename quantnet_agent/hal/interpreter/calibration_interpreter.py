from quantnet_agent.hal.HAL import LocalTaskInterpreter
import logging

log = logging.getLogger(__name__)


class CalibrationInterpreter(LocalTaskInterpreter):

    def __init__(self, hal):
        super().__init__(hal)
        self.parameters = {}

    async def run(self, arg, **kwargs):
        log.debug(f"Received task submit request : {arg} {kwargs}")
        exp_name = arg[0]
        class_name = arg[1]
        for k, v in arg[2].items():
            self.parameters[k] = v
        params = {"exp_type": "local_calibration", **self.parameters}
        exp_id = kwargs["exp_id"]
        await self.hal.devs["exp_framework"].submit(exp_id, exp_name, class_name, params)

    def stop(self):
        log.debug("Stopping local task")
        self.hal.devs["exp_framework"].reset()

    async def receive(self, *args, **kwargs):
        exp_id = args[0]
        log.debug(f"Getting experiment result for {exp_id}")
        result = await self.hal.devs["exp_framework"].receive(*args)
        return result

    def get_state(self, request):
        log.debug(f"Received Experiment get_state request : {request}")
        pass

    def get_info(self, request):
        log.debug(f"Received Experiment get_info request : {request}")
        pass

    def set_value(self, request):
        log.debug(f"Received Experiment set_value request : {request}")
        for k, v in request.payload.items():
            self.parameters[k] = v
