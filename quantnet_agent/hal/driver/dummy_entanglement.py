"""
Dummy entanglement source driver for testing.

Always succeeds and returns synthetic pair data.  Configurable via
``agent.cfg`` device properties:

    success_rate      float  (default 1.0)  fraction of consume() calls
                             that return a pair when the pool is non-empty
    generation_delay  float  (default 0.0)  seconds to wait in enable()
                             before pairs appear
    pair_lifetime     float  (default 0 = unlimited)  seconds after which
                             pairs are considered expired
"""

import asyncio
import logging
import time

from quantnet_agent.hal.hwclasses import EntanglementSource

log = logging.getLogger(__name__)


class DummyEntanglementSource(EntanglementSource):

    def __init__(self, property_dict, node_config, mqhost, mqport,
                 *args, **kwargs):
        super().__init__()
        self._success_rate = float(property_dict.get("success_rate", 1.0))
        self._generation_delay = float(
            property_dict.get("generation_delay", 0.0)
        )
        self._pair_lifetime = float(property_dict.get("pair_lifetime", 0))
        # {peer_id: [pair_dict, ...]}
        self._pools: dict[str, list[dict]] = {}
        self._enabled_peers: set[str] = set()
        log.info("Initializing DummyEntanglementSource")

    # -- helpers --------------------------------------------------------------

    def _make_pair(self, peer_id: str, index: int) -> dict:
        return {
            "comm_qubit_local": 100 + index,
            "comm_qubit_remote": 200 + index,
            "fidelity": 0.95,
            "generation_time": time.time(),
        }

    def _is_expired(self, pair: dict) -> bool:
        if self._pair_lifetime <= 0:
            return False
        return (time.time() - pair["generation_time"]) > self._pair_lifetime

    # -- EntanglementSource interface -----------------------------------------

    async def enable(self, peer_id: str, config: dict) -> dict:
        pool_size = config.get("pool_size", 1)
        log.info(
            "DummyEntanglementSource: enable peer=%s pool_size=%d",
            peer_id, pool_size,
        )
        if self._generation_delay > 0:
            await asyncio.sleep(self._generation_delay)
        self._pools[peer_id] = [
            self._make_pair(peer_id, i) for i in range(pool_size)
        ]
        self._enabled_peers.add(peer_id)
        return {"status": "ok"}

    async def status(self, peer_id: str) -> dict:
        if peer_id not in self._enabled_peers:
            return {"available": False, "pairs": 0, "enabled": False}
        pool = self._pools.get(peer_id, [])
        usable = [p for p in pool if not self._is_expired(p)]
        return {
            "available": len(usable) > 0,
            "pairs": len(usable),
            "enabled": True,
        }

    async def consume(self, peer_id: str) -> dict | None:
        pool = self._pools.get(peer_id, [])
        # Remove expired pairs first
        pool[:] = [p for p in pool if not self._is_expired(p)]
        if not pool:
            return None
        import random
        if random.random() > self._success_rate:
            return None
        pair = pool.pop(0)
        log.info(
            "DummyEntanglementSource: consumed pair for peer=%s "
            "(remaining=%d)",
            peer_id, len(pool),
        )
        return pair

    async def disable(self, peer_id: str) -> dict:
        log.info("DummyEntanglementSource: disable peer=%s", peer_id)
        self._pools.pop(peer_id, None)
        self._enabled_peers.discard(peer_id)
        return {"status": "ok"}

    async def capabilities(self) -> dict:
        return {
            "continuous_generation": True,
            "max_peers": 8,
            "max_pool_size": 32,
            "supports_fidelity_tracking": False,
        }

    async def cleanUp(self):
        self._pools.clear()
        self._enabled_peers.clear()
        return 0
