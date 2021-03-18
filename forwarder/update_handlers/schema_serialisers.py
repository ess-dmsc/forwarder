from typing import Callable, Dict

from forwarder.update_handlers.tdct_serialiser import tdct_Serialiser
from forwarder.update_handlers.nttable_senv_serialiser import nttable_senv_Serialiser
from forwarder.update_handlers.f142_serialiser import f142_Serialiser

schema_serialisers: Dict[str, Callable] = {
    "f142": f142_Serialiser,
    "tdct": tdct_Serialiser,
    "nttable_senv": nttable_senv_Serialiser,
}
