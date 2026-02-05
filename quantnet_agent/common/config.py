import os
import logging
import configobj
import json
from quantnet_agent.common.constants import Constants

log = logging.getLogger(__name__)


def get_config(config_file):
    config_files = []
    if config_file:
        config_files.append(config_file)
    else:
        if "QUANTNET_HOME" in os.environ:
            config_files.append(f"{os.environ['QUANTNET_HOME']}/etc/agent.cfg")
        else:
            config_files.append("/etc/quantnet/agent.cfg")

    for cf in config_files:
        try:
            config = configobj.ConfigObj(cf)
        except IOError:
            continue
    return config


class Config:
    def __init__(
        self,
        config_file: str = None,
        node_file: str = None,
        debug: bool = False,
        agent_id: str = None,
        role: str = None,
        mq_broker_host: str = None,
        mq_broker_port: int = None,
        interpreter_path: str = None,
        schema_path: str = None,
    ):
        self._config = None
        self.devices = {}

        self._config = get_config(config_file)

        if not self._config:
            log.warning("No configuration file found, continuing with defaults")

        if node_file:
            self.node_file = node_file
        else:
            self.node_file = self.config_get("agent", "node_file", raise_exception=False)

        if mq_broker_host:
            self.mq_broker_host = mq_broker_host
        else:
            self.mq_broker_host = self.config_get("mq", "host", default="127.0.0.1")

        if mq_broker_port:
            self.mq_broker_port = mq_broker_port
        else:
            self.mq_broker_port = self.config_get("mq", "port", default="1883")

        self.threads = int(self.config_get("agent", "threads", default=8))

        if debug:
            self.debug = debug
        else:
            self.config_get("agent", "debug", default=False)

        if agent_id:
            self.cid = agent_id
        else:
            try:
                self.cid = self.config_get("agent", "agent_id")
            except Exception:
                self.cid = None

        if role:
            self.role = role
        else:
            self.role = self.config_get("agent", "role", default=None, raise_exception=False)

        self.rpc_client_name = self.config_get("mq", "rpc_client_name", default=f"qn-client-{Constants.INSTANCE_UUID}")
        if interpreter_path:
            self.interpreter_path = interpreter_path
        else:
            self.interpreter_path = self.config_get("interpreters", "path", raise_exception=False)

        if "protocols" in self._config and len(self._config["protocols"]) > 0:
            if self.interpreter_path is None:
                raise Exception("Interpreter location for protocols is not found")
            self.proto_plugins = self._config["protocols"]
        else:
            self.proto_plugins = {}

        if schema_path:
            self.schema_path = schema_path
        else:
            self.schema_path = self.config_get("schemas", "path", raise_exception=False)

        if "devices" in self._config:
            self.devices = self._config["devices"]

        self.tasks = []
        self.task_properties = {}

        if "tasks" in self._config:
            for task, property in self._config["tasks"].items():
                try:
                    if type(property) is configobj.Section:
                        with open(os.path.join(Constants.DEFAULT_TASK_PATH, property["path"]), "r") as file:
                            calibration_task = json.load(file)
                            if float(calibration_task["Periodicity"]) <= Constants.SLOTSIZE.total_seconds():
                                raise Exception(
                                    f"Task {task} interval is too short."
                                    "It should be larger than the TDMA slot size "
                                    f"{Constants.SLOTSIZE.total_seconds() * 1e3}"
                                )
                            self.tasks.append(calibration_task)
                    else:
                        self.task_properties[task] = property
                except Exception as e:
                    log.error(f"Cannot load local task {task} : {e}")

    def config_get(self, section, option, raise_exception=True, default=None, check_config_table=True):
        """
        Return the string value for a given option in a section

        :param section: the named section.
        :param option: the named option.
        :param raise_exception: Boolean to raise or not NoOptionError or NoSectionError.
        :param default: the default value if not found.
        :param check_config_table: if not set, avoid looking at config table
        .
        :returns: the configuration value.
        """
        try:
            return self._config[section][option]
        except (configobj.ConfigObjError, KeyError) as err:
            if raise_exception and default is None:
                raise err
            return default
