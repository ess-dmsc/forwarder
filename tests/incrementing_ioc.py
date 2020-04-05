#!/usr/bin/env python3
from caproto.server import pvproperty, PVGroup, ioc_arg_parser, run
from caproto import ChannelType, AlarmStatus, AlarmSeverity


class IncrementingIOC(PVGroup):
    dt = pvproperty(value=2.0)  # seconds
    x_int = pvproperty(value=0, dtype=ChannelType.INT)

    def __init__(self, prefix, *, macros=None, parent=None, name=None):
        super().__init__(prefix, macros=macros, parent=parent, name=name)
        self.alarm_status = AlarmStatus.NO_ALARM
        self.alarm_severity = AlarmSeverity.NO_ALARM

    async def _toggle_alarm(self, instance):
        """
        Change the alarm state every nth value
        :return:
        """
        nth = 5
        if self.x_int.value % nth == 0:
            if self.alarm_status == AlarmStatus.READ:
                self.alarm_status = AlarmStatus.NO_ALARM
                self.alarm_severity = AlarmSeverity.NO_ALARM
            else:
                self.alarm_status = AlarmStatus.READ
                self.alarm_severity = AlarmSeverity.MINOR_ALARM
            await instance.alarm.write(
                status=self.alarm_status, severity=self.alarm_severity
            )

    @x_int.startup
    async def x_int(self, instance, async_lib):
        """
        Periodically update the value
        """
        while True:
            # compute next value
            x_int = self.x_int.value + 1

            # update the ChannelData instance and notify any subscribers
            await instance.write(value=x_int)
            await self._toggle_alarm(instance)

            # Let the async library wait for the next iteration
            await async_lib.library.sleep(self.dt.value)


if __name__ == "__main__":
    ioc_options, run_options = ioc_arg_parser(
        default_prefix="incrementing_ioc:",
        desc="Run an IOC with an incrementing value.",
    )
    ioc = IncrementingIOC(**ioc_options)
    run(ioc.pvdb, **run_options)
