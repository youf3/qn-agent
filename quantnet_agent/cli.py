import socket
import click
import sys
from quantnet_agent.service import QuantnetAgent
from quantnet_agent.common import Constants, Config
from quantnet_agent.common.logging import setup_logging


@click.command("Quantnet Agent")
@click.option(
    "-c", "--config", default=Constants.DEFAULT_CONFIG_FILE, help="Main configuration file", show_default=True
)
@click.option("-n", "--node-config", default=None, help="Node configuration file", show_default=True)
@click.option("-a", "--agent_id", default=socket.getfqdn(), help="Specify an agent identifier", show_default=True)
@click.option("-r", "--role", default=None, help="Specify the agent role", show_default=True)
@click.option("--mq-broker-host", "mq_broker_host", type=str, help="Message queue broker host", show_default=True)
@click.option("--mq-broker-port", "mq_broker_port", type=int, help="Message queue broker port", show_default=True)
@click.option("-d", "--debug", is_flag=True, show_default=True, default=False, help="Enable debug logging")
@click.option(
    "--interpreter-path",
    "interpreter_path",
    type=str,
    help="Location of additional command interpreters",
    show_default=True,
)
@click.option(
    "--schema-path",
    "schema_path",
    type=str,
    help="Specify a path containing additional schema files",
    show_default=True,
)
def main(config, agent_id, role, node_config, mq_broker_host, mq_broker_port, debug, interpreter_path, schema_path):
    cobj = Config(
        config,
        node_config,
        debug,
        agent_id=agent_id,
        role=role,
        mq_broker_host=mq_broker_host,
        mq_broker_port=mq_broker_port,
        interpreter_path=interpreter_path,
        schema_path=schema_path,
    )

    if cobj.node_file is None:
        print("No node configuration file specified. Use --node-config (-n) to provide one.")
        sys.exit(1)

    setup_logging(cobj)

    agent = QuantnetAgent(cobj)
    agent.run()


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
