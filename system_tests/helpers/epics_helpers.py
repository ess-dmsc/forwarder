from caproto.sync.client import write as epics_write


def change_pv_value(pvname, value):
    """
    Epics call to change PV value.
    N.B. this function uses CA (EPICS v3) to change the value, but the SoftIOC makes PVs available over CA and PVA

    :param pvname:(string) PV name
    :param value: PV value to change to
    :return: none
    """
    response = epics_write(pvname, value, notify=True, timeout=10)
    print(f"Updating PV {pvname} value to {value}")
    print(f"{response}\n", flush=True)

    assert response.status.success
