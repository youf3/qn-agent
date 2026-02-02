import asyncio
import os
import signal
import logging
import uvloop
import importlib
import json
from types import FrameType
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from quantnet_agent.common.config import Config
from quantnet_agent.service.register import Register
from quantnet_agent.scheduler.scheduler import AgentScheduler
from quantnet_mq.schema.models import Schema
from quantnet_mq.msgclient import MsgClient

log = logging.getLogger(__name__)


class QuantnetAgent:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.started = False
        self.should_exit = False
        self.force_exit = False
        self.threads = config.threads
        self.msgclient = MsgClient(self.config.cid, host=self.config.mq_broker_host, port=self.config.mq_broker_port)
        self._tpool = ThreadPoolExecutor(self.threads)
        self.scheduler = AgentScheduler(config.cid, self.msgclient)
        self._sreg = None

    async def handle_exit(self, sig: int, frame: Optional[FrameType]) -> None:
        if self.should_exit and sig == signal.SIGINT:
            self.force_exit = True
        else:
            self.should_exit = True
        await self._sreg.stop()

    def run(self) -> None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        return asyncio.run(self.serve())

    async def serve(self) -> None:
        process_id = os.getpid()

        # install signal handlers
        loop = asyncio.get_event_loop()
        try:
            for signame in ("SIGINT", "SIGTERM"):
                sig = getattr(signal, signame)
                loop.add_signal_handler(sig, lambda signame=signame: asyncio.create_task(self.handle_exit(sig, None)))
        except NotImplementedError:
            return

        log.info(f"Started agent process [{process_id}]")

        await self.startup()
        if self.should_exit:
            return
        await self.main_loop()
        await self.shutdown()

        log.info(f"Finished agent process [{process_id}]")

    def load_schema(self, path):
        if not path:
            log.warning("No additional schema path specified, proceeding with defaults")
            return
        Schema.load_schema(path)

    async def startup(self) -> None:
        self.load_schema(self.config.schema_path)
        log.info(f"Agent started with protocol namespaces:\n{Schema()}")
        self.node = self.get_node(json.load(open(self.config.node_file))["systemSettings"]["type"])
        self._sreg = Register(
            self.config.cid, self.config.node_file, self.config.mq_broker_host, self.config.mq_broker_port,
            self.node._rpcclient, self.node._msgclient
        )
        asyncio.create_task(self._sreg.start())
        await self.scheduler.start()
        await self.node.start()
        self.started = True

    async def main_loop(self) -> None:
        counter = 0
        should_exit = self.should_exit
        while not should_exit:
            counter += 1
            counter = counter % 864000
            await asyncio.sleep(0.1)
            should_exit = self.should_exit

    async def shutdown(self) -> None:
        log.info("Shutting down")

    def get_node(self, node_type):
        nodes_module = importlib.import_module("quantnet_agent.hal.node")
        return getattr(nodes_module, node_type)(self.config, self.scheduler, self.msgclient)
