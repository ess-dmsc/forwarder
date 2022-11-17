from abc import ABC, abstractmethod
from typing import Callable, Dict, Iterable, Optional, Tuple, Union

from caproto import Message as CA_Message
from p4p import Value

from forwarder.parse_config_update import EpicsProtocol


class CASerialiser(ABC):
    @abstractmethod
    def serialise(
        self, update: CA_Message, **unused  # to-do: remove "unused"
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        pass

    @abstractmethod
    def ca_conn_serialise(  # to-do: rename method to conn_serialise
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
        CA_ep00_Serialiser,
        PVA_ep00_Serialiser,
    )
    from forwarder.update_handlers.f142_serialiser import (
        CA_f142_Serialiser,
        PVA_f142_Serialiser,
    )
    from forwarder.update_handlers.no_op_serialiser import (
        CA_no_op_Serialiser,
        PVA_no_op_Serialiser,
    )
    from forwarder.update_handlers.nttable_senv_serialiser import (
        PVA_nttable_senv_Serialiser,
    )
    from forwarder.update_handlers.tdct_serialiser import (
        CA_tdct_Serialiser,
        PVA_tdct_Serialiser,
    )

    _schema_serialisers: Dict[EpicsProtocol, Dict[str, Callable]] = {
        EpicsProtocol.CA: {
            "f142": CA_f142_Serialiser,
            "tdct": CA_tdct_Serialiser,
            "no_op": CA_no_op_Serialiser,
            "ep00": CA_ep00_Serialiser,
        },
        EpicsProtocol.FAKE: {
            "f142": PVA_f142_Serialiser,
            "tdct": PVA_tdct_Serialiser,
            "nttable_senv": PVA_nttable_senv_Serialiser,
            "no_op": PVA_no_op_Serialiser,
            "ep00": PVA_ep00_Serialiser,
        },
        EpicsProtocol.PVA: {
            "f142": PVA_f142_Serialiser,
            "tdct": PVA_tdct_Serialiser,
            "nttable_senv": PVA_nttable_senv_Serialiser,
            "no_op": PVA_no_op_Serialiser,
            "ep00": PVA_ep00_Serialiser,
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
