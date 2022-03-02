from caproto import Message as CA_Message
from typing import Tuple
import p4p


class no_op_Serialiser:
    def __init__(self, source_name: str):
        pass

    def pva_serialise(self, update: p4p.Value) -> Tuple[bytes, int]:
        return None, None

    def ca_serialise(self, update: CA_Message, **unused) -> Tuple[bytes, int]:
        return None, None

    def ca_conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None
