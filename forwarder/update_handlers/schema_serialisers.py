from typing import Callable, Dict

from forwarder.parse_config_update import EpicsProtocol
from forwarder.update_handlers.ep00_serialiser import (
    CA_ep00_Serialiser,
    PVA_ep00_Serialiser,
)
from forwarder.update_handlers.f142_serialiser import (
    CA_f142_Serialiser,
    PVA_f142_Serialiser,
)
from forwarder.update_handlers.no_op_serialiser import no_op_Serialiser
from forwarder.update_handlers.nttable_senv_serialiser import (
    CA_nttable_senv_Serialiser,
    PVA_nttable_senv_Serialiser,
)
from forwarder.update_handlers.tdct_serialiser import (
    CA_tdct_Serialiser,
    PVA_tdct_Serialiser,
)

schema_serialisers: Dict[EpicsProtocol, Dict[str, Callable]] = {
    EpicsProtocol.CA: {
        "f142": CA_f142_Serialiser,
        "tdct": CA_tdct_Serialiser,
        "nttable_senv": CA_nttable_senv_Serialiser,
        "no_op": no_op_Serialiser,
        "ep00": CA_ep00_Serialiser,
    },
    EpicsProtocol.FAKE: {
        "f142": PVA_f142_Serialiser,
        "tdct": PVA_tdct_Serialiser,
        "nttable_senv": PVA_nttable_senv_Serialiser,
        "no_op": no_op_Serialiser,
        "ep00": PVA_ep00_Serialiser,
    },
    EpicsProtocol.PVA: {
        "f142": PVA_f142_Serialiser,
        "tdct": PVA_tdct_Serialiser,
        "nttable_senv": PVA_nttable_senv_Serialiser,
        "no_op": no_op_Serialiser,
        "ep00": PVA_ep00_Serialiser,
    },
}
