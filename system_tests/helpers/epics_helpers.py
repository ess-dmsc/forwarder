from caproto.sync.client import write as epics_write
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)
from p4p.client.thread import Context


def change_pv_value(pvname, value, protocol):
    """
    Epics call to change PV value.
    N.B. this function uses CA (EPICS v3) to change the value, but the SoftIOC makes PVs available over CA and PVA

    :param pvname:(string) PV name
    :param value: PV value to change to
    :return: none
    """
    print(f"Updating PV {pvname} value to {value}")
    if protocol == Protocol.CA:
        response = epics_write(pvname, value, notify=True, timeout=10)
        print(f"{response}\n", flush=True)
        assert response.status.success
    elif protocol == Protocol.PVA:
        ctxt = Context("pva")
        response = ctxt.put(pvname, value, timeout=10, throw=False, wait=True)
        print(f"{response}\n", flush=True)
        assert not isinstance(response, Exception)
    else:
        raise RuntimeError(f"Unknown EPICS protocol: {protocol}")
