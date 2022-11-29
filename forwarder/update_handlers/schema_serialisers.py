from abc import abstractmethod
from typing import Callable, Dict, Iterable, Optional, Protocol, Tuple, Union

from caproto import Message as CA_Message
from p4p import Value

from forwarder.parse_config_update import EpicsProtocol


class CASerialiser(Protocol):
    @abstractmethod
    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        raise NotImplementedError

    @abstractmethod
    def conn_serialise(
        self, pv: str, state: str
    ) -> Tuple[Optional[bytes], Optional[int]]:
        raise NotImplementedError


class PVASerialiser(Protocol):
    @abstractmethod
    def serialise(
        self, update: Union[Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        raise NotImplementedError


class SerialiserFactory:
    from forwarder.update_handlers.ep01_serialiser import (
        ep01_CASerialiser,
        ep01_PVASerialiser,
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
            "ep01": ep01_CASerialiser,
        },
        EpicsProtocol.FAKE: {
            "f142": f142_PVASerialiser,
            "tdct": tdct_PVASerialiser,
            "nttable_senv": nttable_senv_PVASerialiser,
            "no_op": no_op_PVASerialiser,
            "ep01": ep01_PVASerialiser,
        },
        EpicsProtocol.PVA: {
            "f142": f142_PVASerialiser,
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
