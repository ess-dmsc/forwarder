from typing import Callable, Dict

from forwarder.update_handlers.f142_serialiser import f142_Serialiser
from forwarder.update_handlers.nttable_senv_serialiser import nttable_senv_Serialiser
from forwarder.update_handlers.tdct_serialiser import tdct_Serialiser
from forwarder.update_handlers.no_op_serialiser import no_op_Serialiser
from forwarder.update_handlers.ep00_serialiser import ep00_Serialiser

schema_serialisers: Dict[str, Callable] = {
    "f142": f142_Serialiser,
    "tdct": tdct_Serialiser,
    "nttable_senv": nttable_senv_Serialiser,
    "no_op": no_op_Serialiser,
    "ep00": ep00_Serialiser,
}
