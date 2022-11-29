from typing import Tuple

import p4p
from caproto import Message as CA_Message

from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser


class no_op_CASerialiser(CASerialiser):
    def __init__(self, source_name: str):
        pass

    def serialise(self, update: CA_Message, **unused) -> Tuple[None, None]:
        return None, None

    def conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None


class no_op_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        pass

    def serialise(self, update: p4p.Value) -> Tuple[None, None]:
        return None, None
