import graphyte


class StatisticsReporter:
    def __init__(
        self,
        graphyte_server: str,
        logger,
        prefix: str = "throughput",
        update_interval_s: int = 10,
    ):
        self._graphyte_server = graphyte_server
        self._logger = logger
        self._update_interval_s = update_interval_s
        self._last_update_s = 0

        self._sender = graphyte.Sender(self._graphyte_server, prefix=prefix)
        print("Hello World ", self._sender)

    def send_pv_numbers(self, number, timestamp):
        if timestamp > self._last_update_s + self._update_interval_s:
            try:
                self._sender.send("number_pvs", number, timestamp)
            except Exception as ex:
                self._logger.error(f"Could not send statistic: {ex}")

            self._last_update_s = int(timestamp)
