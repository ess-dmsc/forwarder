#!/usr/bin/env python3
import random
from textwrap import dedent
import numpy as np

from caproto.server import PVGroup, ioc_arg_parser, pvproperty, run
from caproto import ChannelType, AlarmSeverity, AlarmStatus


class SimpleTestIOC(PVGroup):
    """
    A simple test IOC for running test PVs.
    """
    delta_time = 1.0  # seconds
    DOUBLE = pvproperty(value=0.0, doc="A random float value", dtype=float)
    DOUBLE3 = pvproperty(value=0.0, record="ai", units="mm", doc="A random float value with an alarm",upper_alarm_limit=1.0,
                   lower_alarm_limit=-1.0,upper_warning_limit=0.5,
                   lower_warning_limit=-0.5,alarm_group="al_grp")
    FLOATARRAY = pvproperty(value=np.zeros(5, dtype=float).tolist(), doc="A random float array value")
    LONG = pvproperty(value=0, doc="A random int value", dtype=int)
    BOOL = pvproperty(value=False, doc="A random boolean value", dtype=bool)
    ENUM = pvproperty(value=0, enum_strings=["Yes", "No", "Maybe", "Perhaps"], doc="A random enum value", dtype=ChannelType.ENUM)
    STR = pvproperty(value="init_value", doc="A random string", dtype=ChannelType.STRING)


if __name__ == '__main__':
    ioc_options, run_options = ioc_arg_parser(
        default_prefix='SIMPLE:',
        desc=dedent(SimpleTestIOC.__doc__))
    ioc = SimpleTestIOC(**ioc_options)
    run(ioc.pvdb, **run_options)