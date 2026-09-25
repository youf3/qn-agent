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

    @abstractmethod
    def measure(self):
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


class EntanglementSource(Device):
    """Abstract interface for continuous entanglement generation.

    Deployment-specific drivers implement this to expose whatever
    entanglement generation mechanism the hardware provides.  QNCP does
    not implement the generation mechanism itself — that is entirely
    behind this interface.

    The five operations form a lifecycle:
        capabilities() → enable(peer, cfg) → status(peer) → consume(peer) → disable(peer)
    """

    @abstractmethod
    async def enable(self, peer_id: str, config: dict) -> dict:
        """Enable continuous entanglement generation with a peer.

        Parameters
        ----------
        peer_id : str
            Identifier of the peer QPU node.
        config : dict
            Deployment-specific configuration.  Common keys:
              pool_size (int): how many pairs to maintain
              min_fidelity (float): minimum acceptable pair fidelity
              comm_positions (list[int]): comm qubits for storage

        Returns
        -------
        dict
            ``{"status": "ok"}`` or ``{"status": "error", "reason": "..."}``.
        """
        pass

    @abstractmethod
    async def status(self, peer_id: str) -> dict:
        """Query entanglement status with a peer.

        Returns
        -------
        dict
            ``{"available": bool, "pairs": int, "enabled": bool}``
        """
        pass

    @abstractmethod
    async def consume(self, peer_id: str) -> dict | None:
        """Consume one entangled pair with a peer.

        Returns
        -------
        dict or None
            Pair data if available::

                {"comm_qubit_local": int, "comm_qubit_remote": int,
                 "fidelity": float, "generation_time": float}

            ``None`` if no pair is currently available.
        """
        pass

    @abstractmethod
    async def disable(self, peer_id: str) -> dict:
        """Disable continuous entanglement generation with a peer.

        Returns
        -------
        dict
            ``{"status": "ok"}`` or ``{"status": "error", "reason": "..."}``.
        """
        pass

    @abstractmethod
    async def capabilities(self) -> dict:
        """Report what this entanglement source supports.

        Returns
        -------
        dict
            ``{"continuous_generation": bool, "max_peers": int,
              "max_pool_size": int, "supports_fidelity_tracking": bool}``
        """
        pass
