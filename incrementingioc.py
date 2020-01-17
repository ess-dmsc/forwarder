#!/usr/bin/env python3
from caproto.server import pvproperty, PVGroup, ioc_arg_parser, run


class IncrementingIOC(PVGroup):
    dt = pvproperty(value=0.5)  # seconds
    x = pvproperty(value=0.0)

    @x.startup
    async def x(self, instance, async_lib):
        """
        Periodically update the value
        """
        while True:
            # compute next value
            x = self.x.value + 1

            # update the ChannelData instance and notify any subscribers
            await instance.write(value=x)

            # Let the async library wait for the next iteration
            await async_lib.library.sleep(self.dt.value)


if __name__ == '__main__':
    ioc_options, run_options = ioc_arg_parser(
        default_prefix='incrementing_ioc:',
        desc='Run an IOC with an incrementing value.')
    ioc = IncrementingIOC(**ioc_options)
    run(ioc.pvdb, **run_options)
