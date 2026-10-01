import logging
from datetime import datetime
from quantnet_agent.hal.HAL import CMDInterpreter
from quantnet_agent.hal.link_state_table import LINK_CONTROL_UP

log = logging.getLogger(__name__)


class LinkInterpreter(CMDInterpreter):
    def __init__(self, hal):
        super().__init__(hal)

    def get_commands(self) -> dict:
        cmds = {
            "link.hello": [
                self.handle_hello,
                "quantnet_mq.schema.models.link_adjacency.linkHello",
            ],
            "link.probe": [
                self.handle_probe,
                "quantnet_mq.schema.models.link_adjacency.linkProbe",
            ],
        }
        node_type = getattr(self.hal._config, "node_type", None)
        if node_type == "OpticalSwitch":
            cmds["link.switchPortCheck"] = [
                self.handle_switch_port_check,
                "quantnet_mq.schema.models.link_adjacency.linkSwitchPortCheck",
            ]
        return cmds

    async def handle_hello(self, msg):
        from quantnet_mq.schema import models
        table = getattr(self.hal, "link_state_table", None)
        if table is None:
            log.warning("LinkInterpreter: no link_state_table on hal")
            return models.link_adjacency.linkHelloResponse(status="error")

        neighbor_cid = msg.payload.src_cid
        entry = table.get(neighbor_cid)
        if entry is None:
            log.warning("LinkInterpreter: received hello from unknown neighbor %s", neighbor_cid)
            return models.link_adjacency.linkHelloResponse(status="unknown")

        # Update last received timestamp
        table.update(neighbor_cid, last_hello_received=datetime.utcnow())

        # Check for bilateral confirmation
        my_cid = getattr(self.hal._config, "cid", None)
        seen = list(msg.payload.seen_neighbors) if msg.payload.seen_neighbors else []
        if my_cid and my_cid in seen and entry.state == "INIT":
            log.info("LinkInterpreter: bilateral hello confirmed with %s → CONTROL_UP", neighbor_cid)
            table.record_transition(neighbor_cid, LINK_CONTROL_UP)

        return models.link_adjacency.linkHelloResponse(status="ok")

    async def handle_probe(self, msg):
        from quantnet_mq.schema import models
        # Photon probe hardware sequence would go here via hal.devs
        # For now: return ok (dummy / simulation path)
        log.info("LinkInterpreter: received probe from %s", msg.payload.src_cid)
        return models.link_adjacency.linkProbeResponse(status="ok")

    async def handle_switch_port_check(self, msg):
        from quantnet_mq.schema import models
        log.info(
            "LinkInterpreter: switch port check src=%s dst=%s",
            msg.payload.src_channel, msg.payload.dst_channel,
        )
        # Query hal.devs for switch routing state if available
        # Default to ok for simulation/dummy environments
        status = "ok"
        try:
            switch_dev = self.hal.devs.get("optical_switch")
            if switch_dev:
                status = switch_dev.check_port(msg.payload.src_channel, msg.payload.dst_channel)
        except Exception as e:
            log.warning("LinkInterpreter: switch port check failed: %s", e)
            status = "port_down"
        return models.link_adjacency.linkSwitchPortCheckResponse(status=status)
