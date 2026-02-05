import logging
import importlib.util
import os
import inspect

# import pprint
from quantnet_agent.common.constants import Constants
from abc import ABC
from quantnet_mq.rpcserver import RPCServer
from quantnet_mq.rpcclient import RPCClient
from quantnet_agent.hal.HAL import (
    HardwareAbstractionLayer,
    CMDInterpreter,
    LocalTaskInterpreter,
    CoreInterpreter,
    ScheduleableInterpreter,
)
from quantnet_agent.hal.interpreter.experiment_interpreter import ExperimentInterpreter
from quantnet_agent.hal.local_task_manager import LocalTaskManager
from quantnet_mq import Code
from quantnet_mq.schema.models import agentSubmitResponse, Status

builtin_interpreter_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "interpreter")

log = logging.getLogger(__name__)


class Node(ABC):
    def __init__(self, config, scheduler, msgclient):
        self._cid = config.cid
        self._node = config.node_file
        self._mqhost = config.mq_broker_host
        self._mqport = config.mq_broker_port
        self.tasks = config.tasks
        self.devices = config.devices
        self.proto_plugins = config.proto_plugins
        self.interpreter_path = config.interpreter_path
        self._rpcserver = RPCServer(self._cid, topic=f"rpc/{self._cid}", host=self._mqhost, port=self._mqport)
        self._rpcclient = RPCClient(config.rpc_client_name, host=self._mqhost, port=self._mqport)
        self._msgclient = msgclient
        self.local_parameters = {}
        self.plugins = {}
        self.core = {}
        self.scheduler = scheduler

        # ATOA RPC System - Client always created, server only if any device is exposed
        self._atoa_rpcclient = RPCClient(f"{self._cid}-atoa-client", host=self._mqhost, port=self._mqport)

        # Check if any device has expose_remote = true
        has_exposed_devices = any(
            device_config.get("expose_remote", False) for device_config in config.devices.values()
        )

        # Store exposed device names for access control
        self._exposed_devices = {
            device_name
            for device_name, device_config in config.devices.items()
            if device_config.get("expose_remote", False)
        }

        if has_exposed_devices:
            log.info(f"Agent {self._cid} has exposed devices: {self._exposed_devices}. Starting ATOA server.")
            self._atoa_rpcserver = RPCServer(
                f"{self._cid}-atoa", topic=f"{Constants.EXPERIMENT_TOPIC_BASE}/+", host=self._mqhost, port=self._mqport
            )
            self.register_atoa_commands()
        else:
            log.info(f"Agent {self._cid} has no exposed devices. ATOA server disabled.")
            self._atoa_rpcserver = None

        self.hal = HardwareAbstractionLayer(
            config, self._rpcclient, self._msgclient, atoa_rpcclient=self._atoa_rpcclient
        )
        self.local_task_manager = LocalTaskManager(
            self._cid, scheduler, self._msgclient, delay=int(config.task_properties.get("traverse_delay", "0"))
        )

        # Built-in interpreters
        for ns, interpreter in Constants.DEFAULT_INTERPRETERS.items():
            self.register_command_namespace(ns, os.path.join(builtin_interpreter_path, interpreter))

        # User-defined interpreters
        for ns, interpreter in self.proto_plugins.items():
            self.register_command_namespace(ns, os.path.join(self.interpreter_path, interpreter), builtin=False)

        # local task interpreters
        for task in self.tasks:
            self.register_command_namespace(Constants.DEFAULT_TASK_NS, Constants.DEFAULT_TASK_INTERPRETER, task=task)

        log.info(f"Agent {self._cid} will load the followings: \n{str(self)}")

    def __str__(self):
        ret = f"{'NAME':<30}{'TYPE':<30}RESOURCE\n"
        ret += f"{''.ljust(60, '-')}\n"
        for name, v in self.devices.items():
            ns = "driver"
            path = v.get("driver")
            ret += f"{name:<30}{ns:<30}{path}\n"
        for name, v in Constants.DEFAULT_INTERPRETERS.items():
            ns = "build-in interpreters"
            path = v
            ret += f"{name:<30}{ns:<30}{path}\n"
        for name, v in self.proto_plugins.items():
            ns = "user-defined interpreters"
            path = v
            ret += f"{name:<30}{ns:<30}{path}\n"
        for task in self.tasks:
            ns = "task"
            name = task["Name"]
            path = Constants.DEFAULT_TASK_PATH
            ret += f"{name:<30}{ns:<30}{path}\n"
        return ret.strip()

    def reset(self):
        pass

    def stop(self):
        self.local_task_manager.stop()
        pass

    def update(self):
        pass

    @property
    def status(self):
        log.debug("Getting local node state")
        return self.local_task_manager.status

    def get_node(self, node_type):
        nodes_module = importlib.import_module("quantnet_agent.hal.node")
        return getattr(nodes_module, node_type)()

    def register_task(self, interpreter, task):
        self.local_task_manager.add_task(task, interpreter)

    def register_command(self, ns, interpreter_obj, builtin, is_core):
        # interpreter_obj = interpreter(self) if is_core else interpreter(self.hal)

        for cmd, interpreter_map in interpreter_obj.get_commands().items():

            if builtin:
                log.debug(f"Registering command {cmd} from built-in interpreter{interpreter_obj} in namespace {ns}")
                if is_core:
                    if ns not in self.core:
                        self.core[ns] = {}
                    target = self.core[ns]
                else:
                    if ns not in self.plugins:
                        self.plugins[ns] = {}
                    target = self.plugins[ns]
                target[cmd] = interpreter_map
            else:
                log.debug(f"Registering command {cmd} from user interpreter{interpreter_obj} in namespace {ns}")
                if ns in self.core:
                    if cmd in self.core[ns]:
                        log.error(f"Cannot overwrite Cmd {cmd} from core component {self.core[ns][cmd]}")
                        raise Exception("Duplicated command definition")
                elif ns in self.plugins:
                    log.error(f"Cmd {cmd} is already registered from {self.plugins[ns][cmd]}")
                    raise Exception("Duplicated command definition")
                else:
                    self.plugins[ns] = {}
                    self.plugins[ns][cmd] = interpreter_map
                    log.debug(f"Overwriting command {cmd} with interpreter{interpreter_obj} in namespace {ns}")

            self._rpcserver.set_handler(cmd, interpreter_map[0], interpreter_map[1])

    def register_command_namespace(self, ns, cmd_interpreter, name=None, builtin=True, task=None):
        # TODO: check if module complies with spec

        log.debug(f"Registering namespace {ns}")
        try:
            module_spec = importlib.util.spec_from_file_location(ns, cmd_interpreter)
            plugin_modules = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(plugin_modules)
        except FileNotFoundError:
            log.error(f"Interpreter not found for namespace {ns} at {cmd_interpreter}")
            exit(1)
        for module_name in dir(plugin_modules):
            module = getattr(plugin_modules, module_name)
            if not inspect.isclass(module):
                continue
            if (
                issubclass(module, CMDInterpreter)
                and not module == CMDInterpreter
                and not module == ScheduleableInterpreter
                and not module == ExperimentInterpreter
            ):
                interpreter_obj = module(self.hal)
                self.register_command(ns, interpreter_obj, builtin, False)
                if issubclass(module, ScheduleableInterpreter):
                    self.scheduler.register_command(ns, interpreter_obj, self._rpcserver)
            elif issubclass(module, CoreInterpreter) and not module == CoreInterpreter:
                interpreter_obj = module(self)
                self.register_command(ns, interpreter_obj, builtin, True)
            elif issubclass(module, LocalTaskInterpreter) and not module == LocalTaskInterpreter:
                self.register_task(module(self.hal), task)

    def register_atoa_commands(self):
        """Register commands for the Agent-to-Agent RPC server."""
        if self._atoa_rpcserver is None:
            return
        commands = {"agentSubmit": [self.handle_agent_submit, "quantnet_mq.schema.models.agentSubmit"]}
        for cmd, (handler, schema) in commands.items():
            self._atoa_rpcserver.set_handler(cmd, handler, schema)

    async def handle_agent_submit(self, args):
        """Handle remote requests to control local HAL devices."""
        log.info(f"Received remote agent submit request : {args}")
        payload = args["payload"]
        device_name = payload["device"]
        function_info = payload["function"]
        function_name = (
            function_info["name"]._value if hasattr(function_info["name"], "_value") else function_info["name"]
        )
        parameters = function_info["parameters"]

        rc = Code.INVALID_ARGUMENT  # Default error code
        message = ""

        try:
            # Access control: verify device is exposed for remote access
            if device_name not in self._exposed_devices:
                raise PermissionError(
                    f"Device {device_name} is not exposed for remote access. "
                    f"Available devices: {self._exposed_devices}"
                )

            if device_name not in self.hal.devs:
                raise ValueError(f"Device {device_name} not found in HAL")

            device = self.hal.devs[device_name]
            func_obj = getattr(device, function_name)
            rc = Code.OK
            message = await func_obj(**parameters)
        except PermissionError as e:
            log.warning(f"Access denied for remote agent submit: {e}")
            message = str(e)
        except Exception as e:
            log.error(f"Error in remote agent submit: {type(e)}:{e}")
            message = f"Failed to run {device_name}.{function_name} with parameters {parameters}. Error: {e}"
        finally:
            return agentSubmitResponse(status=Status(code=rc.value, value=rc.name), message=message)

    async def start(self):
        await self._rpcserver.start()
        if self._atoa_rpcserver is not None:
            await self._atoa_rpcserver.start()
        await self.local_task_manager.start()


class QNode(Node):
    def __init__(self, config, scheduler, msgclient):
        super().__init__(config, scheduler, msgclient)

    def sync(self):
        pass


class MNode(Node):
    def __init__(self, config, scheduler, msgclient):
        super().__init__(config, scheduler, msgclient)

    def sync(self):
        pass


class BSMNode(Node):
    def __init__(self, config, scheduler, msgclient):
        super().__init__(config, scheduler, msgclient)

        if "epc" not in config.devices:
            log.error("This agent has no EPC configuration")
            return
        if "polarimeter" not in config.devices:
            log.error("This agent has no polarimeter configuration")
            return

    def sync(self):
        pass


class OpticalSwitch(Node):
    def __init__(self, config, scheduler, msgclient):
        super().__init__(config, scheduler, msgclient)
        self._rpc_calibration_server_handlers = []

    def sync(self):
        pass
