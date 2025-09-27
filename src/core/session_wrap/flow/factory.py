from typing import Dict

from core.session_wrap.flow import TwoWayFlowInterface
from core.session_wrap.flow.realizations.ai_flow import AIFlow
from core.session_wrap.transport import TwoWayTransportInterface
from core.session_wrap.types import FlowType
from core.worker.worker_manager import CPUCommands


class TwoWayFlowFactory:
    type_mapping: Dict[FlowType, TwoWayFlowInterface] = {
        FlowType.average_image: AIFlow,
    }

    @classmethod
    def get_flow(
            cls,
            f_type: FlowType,
            transport: TwoWayTransportInterface,
            cpu_commands: CPUCommands
    ) -> TwoWayFlowInterface:
        return cls.type_mapping[f_type](transport, cpu_commands) # noqa