from abc import ABC, abstractmethod
import logging

log = logging.getLogger(__name__)


class Device(ABC):
    def __init__(self, *args, **kwargs):
        self._status = 0

    @property
    def status(self):
        return self._status

    @abstractmethod
    async def cleanUp(self):
        pass


class LightSrc(Device):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    @property
    @abstractmethod
    def wavelength(self):
        pass

    @wavelength.setter
    @abstractmethod
    def wavelength(self, freq):
        pass

    @property
    @abstractmethod
    def power(self):
        pass

    @power.setter
    @abstractmethod
    def power(self, power):
        pass

    @abstractmethod
    async def generate(self):
        pass


class Filter(Device):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    @abstractmethod
    def polarize(self, *args):
        pass

    @abstractmethod
    def attenuate(self, strength):
        pass


class LightMeasurement(Device):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    @property
    def wavelength(self):
        pass

    @wavelength.setter
    @abstractmethod
    def wavelength(self, freq):
        pass

    @property
    @abstractmethod
    def power(self):
        pass

    @abstractmethod
    def sweep(self):
        pass

    @abstractmethod
    async def measure(self):
        pass


class SignalMeasurement(Device):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    @property
    @abstractmethod
    def window(self):
        pass

    @window.setter
    @abstractmethod
    def window(self, value):
        pass

    @abstractmethod
    async def measure(self, *args, **kwargs):
        pass


class AnalogController(Device):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @property
    def property(self):
        pass

    @property.setter
    @abstractmethod
    def property(self, *kwargs):
        pass


class DigitalController(Device):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    @property
    def property(self):
        pass

    @property.setter
    @abstractmethod
    def property(self, *kwargs):
        pass


class ExpFramework(Device):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    @abstractmethod
    async def submit(self, exp_id, expName, classname, args=dict()):
        pass

    @abstractmethod
    async def receive(self, exp_id):
        pass

    @property
    @abstractmethod
    def logs(self):
        pass
