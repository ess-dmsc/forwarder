from forwarder.parse_config_update import Channel, EpicsProtocol


CHANNELS = set()

def add_to_configuration(channels):
    for channel in channels:
        if channel in CHANNELS:

        CHANNELS


def test_on_add_pv_dumped_config_contains_pv_details():
    channel_to_add = Channel("MY_PV_NAME", EpicsProtocol.PVA, "MY_TOPIC", "f142")

    add_to_configuration((channel_to_add,))


    assert new_pv in dumped_config
