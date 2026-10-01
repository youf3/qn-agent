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
            "link.control": [
                self.handle_control,
                "quantnet_mq.schema.models.link_adjacency.linkControl",
            ],
        }
        node_type = getattr(self.hal._config, "node_type", None)
        if node_type == "OpticalSwitch":
            cmds["link.switchPortCheck"] = [
                self.handle_switch_port_check,
                "quantnet_mq.schema.models.link_adjacency.linkSwitchPortCheck",
            ]
        else:
            cmds["link.hello"] = [
                self.handle_hello,
                "quantnet_mq.schema.models.link_adjacency.linkHello",
            ]
            cmds["link.probe"] = [
                self.handle_probe,
                "quantnet_mq.schema.models.link_adjacency.linkProbe",
            ]
        return cmds

    # ── link.control dispatcher ────────────────────────────────────────

    async def handle_control(self, msg):
        from quantnet_mq.schema import models

        action = msg.payload.action
        target = getattr(msg.payload, "target", None)
        param = getattr(msg.payload, "param", None)
        value = getattr(msg.payload, "value", None)
        table = getattr(self.hal, "link_state_table", None)
        link_mgr = getattr(self.hal, "link_mgr", None)

        try:
            if action == "show":
                data = {"links": self._serialize_table(table)}
                return models.link_adjacency.linkControlResponse(status="ok", data=data)

            elif action == "show_detail":
                entry = table.get(target) if table else None
                if entry is None:
                    return models.link_adjacency.linkControlResponse(
                        status="error", data={"message": f"Unknown neighbor: {target}"}
                    )
                data = self._serialize_entry(target, entry)
                return models.link_adjacency.linkControlResponse(status="ok", data=data)

            elif action == "connect":
                await link_mgr.connect(target)
                return models.link_adjacency.linkControlResponse(status="ok", data={})

            elif action == "disconnect":
                await link_mgr.disconnect(target)
                return models.link_adjacency.linkControlResponse(status="ok", data={})

            elif action == "probe":
                entry = table.get(target) if table else None
                if entry is None:
                    return models.link_adjacency.linkControlResponse(
                        status="error", data={"message": f"Unknown neighbor: {target}"}
                    )
                await link_mgr._send_probe(target, entry)
                return models.link_adjacency.linkControlResponse(status="ok", data={})

            elif action == "set":
                if param == "hello-interval":
                    link_mgr.set_hello_interval(value)
                elif param == "hold-time":
                    link_mgr.set_hold_time(value)
                else:
                    return models.link_adjacency.linkControlResponse(
                        status="error", data={"message": f"Unknown param: {param}"}
                    )
                return models.link_adjacency.linkControlResponse(status="ok", data={})

            elif action == "switch_check":
                entry = table.get(target) if table else None
                if entry is None or not entry.switch_in_path or not entry.switch_cid:
                    return models.link_adjacency.linkControlResponse(
                        status="error", data={"message": "No switch in path"}
                    )
                ok = await link_mgr._check_switch(
                    entry.switch_cid, entry.channel_id, entry.neighbor_channel_id
                )
                return models.link_adjacency.linkControlResponse(
                    status="ok", data={"switch_ok": ok}
                )

            else:
                return models.link_adjacency.linkControlResponse(
                    status="error", data={"message": f"Unknown action: {action}"}
                )

        except Exception as e:
            log.error("link.control error: %s", e)
            return models.link_adjacency.linkControlResponse(
                status="error", data={"message": str(e)}
            )

    # ── serialization helpers ──────────────────────────────────────────

    @staticmethod
    def _serialize_table(table):
        if table is None:
            return {}
        result = {}
        for cid, entry in table.all().items():
            result[cid] = {
                "state": entry.state,
                "channel_id": entry.channel_id,
                "neighbor_channel_id": entry.neighbor_channel_id,
                "last_hello_received": entry.last_hello_received.isoformat() if entry.last_hello_received else None,
                "last_hello_sent": entry.last_hello_sent.isoformat() if entry.last_hello_sent else None,
                "hold_time": entry.hold_time,
                "switch_in_path": entry.switch_in_path,
            }
        return result

    @staticmethod
    def _serialize_entry(cid, entry):
        history = []
        for ts, state in entry.history[-20:]:
            history.append({"timestamp": ts.isoformat(), "state": state})
        return {
            "neighbor": cid,
            "state": entry.state,
            "channel_id": entry.channel_id,
            "neighbor_channel_id": entry.neighbor_channel_id,
            "last_hello_received": entry.last_hello_received.isoformat() if entry.last_hello_received else None,
            "last_hello_sent": entry.last_hello_sent.isoformat() if entry.last_hello_sent else None,
            "hold_time": entry.hold_time,
            "switch_in_path": entry.switch_in_path,
            "switch_cid": entry.switch_cid,
            "history": history,
        }

    # ── existing link adjacency handlers ───────────────────────────────

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
