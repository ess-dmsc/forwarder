from abc import ABC, abstractmethod
from typing import Callable, Dict, Iterable, Optional, Tuple, Union

from caproto import Message as CA_Message
from p4p import Value

from forwarder.parse_config_update import EpicsProtocol


class CASerialiser(ABC):
    @abstractmethod
    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        pass

    @abstractmethod
    def conn_serialise(
        self, pv: str, state: str
    ) -> Tuple[Optional[bytes], Optional[int]]:
        pass


class PVASerialiser(ABC):
    @abstractmethod
    def serialise(
        self, update: Union[Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        pass


class SerialiserFactory:
    from forwarder.update_handlers.ep00_serialiser import (
        ep00_CASerialiser,
        ep00_PVASerialiser,
    )
    from forwarder.update_handlers.f142_serialiser import (
        f142_CASerialiser,
        f142_PVASerialiser,
    )
    from forwarder.update_handlers.no_op_serialiser import (
        no_op_CASerialiser,
        no_op_PVASerialiser,
    )
    from forwarder.update_handlers.nttable_senv_serialiser import (
        nttable_senv_PVASerialiser,
    )
    from forwarder.update_handlers.tdct_serialiser import (
        tdct_CASerialiser,
        tdct_PVASerialiser,
    )

    _schema_serialisers: Dict[EpicsProtocol, Dict[str, Callable]] = {
        EpicsProtocol.CA: {
            "f142": f142_CASerialiser,
            "tdct": tdct_CASerialiser,
            "no_op": no_op_CASerialiser,
            "ep00": ep00_CASerialiser,
        },
        EpicsProtocol.FAKE: {
            "f142": f142_PVASerialiser,
            "tdct": tdct_PVASerialiser,
            "nttable_senv": nttable_senv_PVASerialiser,
            "no_op": no_op_PVASerialiser,
            "ep00": ep00_PVASerialiser,
        },
        EpicsProtocol.PVA: {
            "f142": f142_PVASerialiser,
            "tdct": tdct_PVASerialiser,
            "nttable_senv": nttable_senv_PVASerialiser,
            "no_op": no_op_PVASerialiser,
            "ep00": ep00_PVASerialiser,
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
