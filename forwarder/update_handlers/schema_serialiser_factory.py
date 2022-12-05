from typing import Callable, Dict, Iterable, Union

from forwarder.common import EpicsProtocol
from forwarder.update_handlers.ep01_serialiser import (
    ep01_CASerialiser,
    ep01_PVASerialiser,
)
from forwarder.update_handlers.f142_serialiser import (
    f142_CASerialiser,
    f142_PVASerialiser,
)
from forwarder.update_handlers.f144_serialiser import (
    f144_CASerialiser,
    f144_PVASerialiser,
)
from forwarder.update_handlers.no_op_serialiser import (
    no_op_CASerialiser,
    no_op_PVASerialiser,
)
from forwarder.update_handlers.nttable_senv_serialiser import nttable_senv_PVASerialiser
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser
from forwarder.update_handlers.tdct_serialiser import (
    tdct_CASerialiser,
    tdct_PVASerialiser,
)


class SerialiserFactory:

    _schema_serialisers: Dict[EpicsProtocol, Dict[str, Callable]] = {
        EpicsProtocol.CA: {
            "f142": f142_CASerialiser,
            "f144": f144_CASerialiser,
            "tdct": tdct_CASerialiser,
            "no_op": no_op_CASerialiser,
            "ep01": ep01_CASerialiser,
        },
        EpicsProtocol.FAKE: {
            "f142": f142_PVASerialiser,
            "f144": f144_PVASerialiser,
            "tdct": tdct_PVASerialiser,
            "nttable_senv": nttable_senv_PVASerialiser,
            "no_op": no_op_PVASerialiser,
            "ep01": ep01_PVASerialiser,
        },
        EpicsProtocol.PVA: {
            "f142": f142_PVASerialiser,
            "f144": f144_PVASerialiser,
            "tdct": tdct_PVASerialiser,
            "nttable_senv": nttable_senv_PVASerialiser,
            "no_op": no_op_PVASerialiser,
            "ep01": ep01_PVASerialiser,
        },
    }

    @classmethod
    def create_serialiser(
        cls, protocol: EpicsProtocol, schema: str, source_name: str
    ) -> Union[CASerialiser, PVASerialiser]:
        return cls._schema_serialisers[protocol][schema](source_name)

    @classmethod
    def get_protocols(cls) -> Iterable[EpicsProtocol]:
        return cls._schema_serialisers.keys()

    @classmethod
    def get_schemas(cls, protocol: EpicsProtocol) -> Iterable[str]:
        return cls._schema_serialisers[protocol].keys()
