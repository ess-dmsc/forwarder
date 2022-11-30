from abc import abstractmethod
from typing import Optional, Protocol, Tuple, Union

from caproto import Message as CA_Message
from p4p import Value


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
