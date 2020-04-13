from p4p.client.thread import Context as PVAContext


class PVAUpdateHandler:
    """
    Monitors via EPICS v4 Process Variable Access (PVA),
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    PVA support from p4p library.
    """

    def __init__(self, context: PVAContext, pv_name: str):
        # pv_subscription = context.monitor(pv_name, cb)
        pass
