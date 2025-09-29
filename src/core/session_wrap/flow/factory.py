from typing import Dict

from core.session_wrap.flow import TwoWayFlowInterface
from core.session_wrap.flow.realizations.ai_flow import SIFlow
from core.session_wrap.transport import TwoWayTransportInterface
from core.session_wrap.types import FlowType
from core.worker.worker_manager import CPUCommands


class TwoWayFlowFactory:
    # TODO finalize other flow wrapper implementations
    type_mapping: Dict[FlowType, TwoWayFlowInterface] = {
        FlowType.summary_image: SIFlow,
    }

    @classmethod
    def get_flow(
            cls,
            f_type: FlowType,
            transport: TwoWayTransportInterface,
            cpu_commands: CPUCommands
    ) -> TwoWayFlowInterface:
        return cls.type_mapping[f_type](transport, cpu_commands) # noqa