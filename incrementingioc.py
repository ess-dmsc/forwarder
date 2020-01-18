#!/usr/bin/env python3
from caproto.server import pvproperty, PVGroup, ioc_arg_parser, run
from caproto import ChannelType


class IncrementingIOC(PVGroup):
    dt = pvproperty(value=1.0)  # seconds
    x_int = pvproperty(value=0, dtype=ChannelType.INT)

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

            # Let the async library wait for the next iteration
            await async_lib.library.sleep(self.dt.value)


if __name__ == '__main__':
    ioc_options, run_options = ioc_arg_parser(
        default_prefix='incrementing_ioc:',
        desc='Run an IOC with an incrementing value.')
    ioc = IncrementingIOC(**ioc_options)
    run(ioc.pvdb, **run_options)
