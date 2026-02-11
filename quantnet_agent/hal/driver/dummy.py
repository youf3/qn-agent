from quantnet_agent.hal.hwclasses import (
    LightSrc,
    Filter,
    SignalMeasurement,
    LightMeasurement,
    ExpFramework,
    DigitalController,
    AnalogController,
)
import logging
import asyncio
import random
from quantnet_agent.hal.interpreter.PSO.utility import MeasurementType

log = logging.getLogger(__name__)


class DummyLightSrc(LightSrc):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        self._power = 0
        self._wavelength = 0
        self._status = 0
        self._failrate = float(args[0].get("failrate", 0))
        log.info("Initializing Dummy Lightsource")
        return

    @property
    def wavelength(self):
        return self._wavelength

    @wavelength.setter
    def wavelength(self, freq):
        log.info(f"Setting wavelength to {freq}")
        self._wavelength = freq

    @property
    def power(self):
        return self._power

    @power.setter
    def power(self, power):
        log.info(f"Setting power to {power}")
        self._power = power

    async def generate(self, pol):
        log.info(f"Generating {pol} light with to power = {self._power}, wavelength = {self._wavelength}")
        if random.random() < self._failrate:
            log.error("Light source failed")
            return 1
        else:
            log.info("Calibrating for 10 sec")
            await asyncio.sleep(10)
            return 0

    async def cleanUp(self):
        self._status = 0
        log.info("Cleaning up for 10 sec")
        await asyncio.sleep(10)
        return 0

    @property
    def status(self):
        return self._status


class DummyPSG(Filter):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        self.dev = "DummyPSG"
        log.info("DummyPSG initialized")

    async def connect(self):
        log.info("DummyPSG connected")

    async def polarize(self, state):
        log.debug(f"DummyPSG polarize to {state}")
        return 0

    async def attenuate(self, attenuation):
        log.debug(f"DummyPSG attenuate to {attenuation}")
        return 0

    async def cleanUp(self):
        log.info("DummyPSG cleanup")
        return 0


class DummyEPC(Filter):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        log.info("EPC initialized")
        return

    async def polarize(self, _):
        log.debug("Calibrating link using EPC")
        log.debug("Link calibrate")
        return 0

    async def attenuate(self, _):
        log.debug("Attenuate link using EPC")
        log.debug("Link attenuated")
        return 0

    async def cleanUp(self):
        self._status = 0
        return 0


class DummyPolarimeter(LightMeasurement):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        self._wavelength = 0
        log.info(f"Initializing LightMeasurement with wavelength={self._wavelength}")
        return

    @property
    def wavelength(self):
        return self._wavelength

    @wavelength.setter
    def wavelength(self, freq):
        log.info(f"Setting wavelength to {freq}")
        self._wavelength = freq

    @property
    def power(self):
        return self._power

    def sweep(self):
        pass

    async def measure(self):
        log.info("Measuring SOP")
        for i in range(5):
            log.info(f"Generating random SOPs :[{random.random()},{random.random()},{random.random()}]")
            await asyncio.sleep(1)
        log.info("reached threashold SOP :[0.995, 0.995, 0.995]")
        return 0

    async def cleanUp(self):
        self._status = 0
        return 0


class DummyExpFramework(ExpFramework):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        log.info("Initializing ExpFramework")
        self.result_ind = 0
        return

    async def submit(self, exp_id, expName, classname, args=dict()):
        log.info(f"Submitting exp:{expName} with id {id} with args:{args} for 10 sec")
        # await asyncio.sleep(10)
        return 0

    async def receive(self, *args, **kwargs):
        exp_id = args[0]
        log.info(f"Receiving result from exp:{exp_id}")
        results = [
            {
                "current_scan.plots.x": [
                    170000000.0,
                    172068965.5172414,
                    174137931.03448275,
                    176206896.55172414,
                    178275862.06896552,
                    180344827.58620688,
                    182413793.10344827,
                    184482758.62068966,
                    186551724.13793105,
                    188620689.6551724,
                    190689655.1724138,
                    192758620.68965518,
                    194827586.20689654,
                    196896551.72413793,
                    198965517.24137932,
                    201034482.75862068,
                    203103448.27586207,
                    205172413.79310346,
                    207241379.31034482,
                    209310344.8275862,
                    211379310.3448276,
                    213448275.86206895,
                    215517241.37931034,
                    217586206.89655173,
                    219655172.4137931,
                    221724137.93103448,
                    223793103.44827586,
                    225862068.96551722,
                    227931034.4827586,
                    230000000.0,
                ],
                "current_scan.plots.y": [
                    [1896.0],
                    [1715.0],
                    [1913.0],
                    [1844.0],
                    [2061.0],
                    [1977.0],
                    [1936.0],
                    [2039.0],
                    [1937.0],
                    [2319.0],
                    [2458.0],
                    [2743.0],
                    [2943.0],
                    [3469.0],
                    [4059.0],
                    [4817.0],
                    [5377.0],
                    [5807.0],
                    [6039.0],
                    [6113.0],
                    [4882.0],
                    [3784.0],
                    [3607.0],
                    [4132.0],
                    [4200.0],
                    [4517.0],
                    [4693.0],
                    [5064.0],
                    [5146.0],
                    [2963.0],
                ],
            },
            {
                "current_scan.plots.x": [
                    30.0,
                    28.94736842105263,
                    27.894736842105264,
                    26.842105263157894,
                    25.789473684210527,
                    24.736842105263158,
                    23.684210526315788,
                    22.63157894736842,
                    21.578947368421055,
                    20.526315789473685,
                    19.473684210526315,
                    18.42105263157895,
                    17.36842105263158,
                    16.315789473684212,
                    15.263157894736842,
                    14.210526315789474,
                    13.157894736842106,
                    12.105263157894736,
                    11.05263157894737,
                    10.0,
                ],
                "current_scan.plots.y": [
                    [-2.0],
                    [-26.0],
                    [15.0],
                    [4.0],
                    [-31.0],
                    [-14.0],
                    [69.0],
                    [63.0],
                    [149.0],
                    [177.0],
                    [494.0],
                    [576.0],
                    [1112.0],
                    [2253.0],
                    [2947.0],
                    [3776.0],
                    [4876.0],
                    [5963.0],
                    [7261.0],
                    [8522.0],
                ],
            },
            {
                "current_scan.plots.x": [
                    60000000.0,
                    61379310.344827585,
                    62758620.68965517,
                    64137931.03448276,
                    65517241.37931035,
                    66896551.72413793,
                    68275862.06896552,
                    69655172.4137931,
                    71034482.7586207,
                    72413793.10344827,
                    73793103.44827586,
                    75172413.79310346,
                    76551724.13793103,
                    77931034.48275863,
                    79310344.8275862,
                    80689655.1724138,
                    82068965.51724139,
                    83448275.86206897,
                    84827586.20689656,
                    86206896.55172414,
                    87586206.89655173,
                    88965517.24137932,
                    90344827.5862069,
                    91724137.93103449,
                    93103448.27586207,
                    94482758.62068966,
                    95862068.96551725,
                    97241379.31034483,
                    98620689.65517241,
                    100000000.0,
                ],
                "current_scan.plots.y": [
                    [3034.0],
                    [3063.0],
                    [3198.0],
                    [3290.0],
                    [3017.0],
                    [3097.0],
                    [3066.0],
                    [3188.0],
                    [3201.0],
                    [3348.0],
                    [3278.0],
                    [3243.0],
                    [3223.0],
                    [3145.0],
                    [3238.0],
                    [3159.0],
                    [3193.0],
                    [3328.0],
                    [3227.0],
                    [3342.0],
                    [3338.0],
                    [3433.0],
                    [3293.0],
                    [3261.0],
                    [3205.0],
                    [3437.0],
                    [3306.0],
                    [3325.0],
                    [3359.0],
                    [3394.0],
                ],
            },
            {
                "current_scan.plots.x": [
                    30.0,
                    28.94736842105263,
                    27.894736842105264,
                    26.842105263157894,
                    25.789473684210527,
                    24.736842105263158,
                    23.684210526315788,
                    22.63157894736842,
                    21.578947368421055,
                    20.526315789473685,
                    19.473684210526315,
                    18.42105263157895,
                    17.36842105263158,
                    16.315789473684212,
                    15.263157894736842,
                    14.210526315789474,
                    13.157894736842106,
                    12.105263157894736,
                    11.05263157894737,
                    10.0,
                ],
                "current_scan.plots.y": [
                    [3514.0],
                    [3372.0],
                    [3508.0],
                    [3575.0],
                    [3508.0],
                    [3536.0],
                    [3671.0],
                    [3489.0],
                    [3597.0],
                    [3494.0],
                    [3596.0],
                    [3576.0],
                    [3661.0],
                    [3721.0],
                    [3531.0],
                    [3609.0],
                    [3596.0],
                    [3626.0],
                    [3681.0],
                    [3543.0],
                ],
            },
        ]
        index = self.result_ind % 4
        self.result_ind += 1
        return {"dummy status": "done", "results": results[index]}

    @property
    def logs(self):
        log.info("Getting log from ExpFramework")
        return "Dummy logs"

    async def cleanUp(self):
        self._status = 0
        return 0


class DummySignalMeasurement(SignalMeasurement):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        log.info("Initializing Dummy SignalMeasurement")
        return

    async def measure(self, *args, **kwargs):
        log.debug("Measuring light")
        channels = kwargs.get("channels", [1, 2])
        command = args[0]
        import numpy as np

        # Use simple fixed values for deterministic testing or random for simulation
        # Using fixed values to avoid "random" failures in threshold checks if not handled
        if command == MeasurementType.RATE:
            # ROI: BSM.py uses sum(single_counts).
            # If channels=[1,2,3,4], result should be array of length 4.
            # PSO.py uses result indices.
            result = np.array([1000.0] * len(channels))
        elif command == MeasurementType.COINCIDENCE:
            # ROI: BSM.py expects coincidence_hist[channel_pair_index]
            # It sums it: np.sum(HOM_coincidence_hist)
            # It expects shape (len(channel_pairs), n_bins)
            n_bins = kwargs.get("n_bins", 100)
            # Return list of arrays, one per channel pair
            # Shape: (len(channels), n_bins)
            num_pairs = len(channels)
            result = [np.ones(n_bins) * 10 for _ in range(num_pairs)]
        else:
            result = np.array([1.0] * len(channels))
        return result

    async def cleanUp(self):
        self._status = 0
        return 0


class DummyController(DigitalController, AnalogController):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        log.info("Initializing Dummy Controller")
        return

    async def configure(self, **kwargs):
        log.info("Configure Dummy Controller")
        return 0

    async def set(self, **kwargs):
        log.info("set Dummy Controller")
        return 0

    async def control_channel(self, channel, setting):
        log.info(f"DummyController control_channel {channel} {setting}")
        return 0

    async def cleanUp(self):
        self._status = 0
        return 0
