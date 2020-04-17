from caproto.server import pvproperty, PVGroup, ioc_arg_parser, run
from caproto import ChannelType
from multiprocessing import Process


class SimpleIOC(PVGroup):
    DOUBLE = pvproperty(value=0.0, dtype=ChannelType.DOUBLE)
    LONG = pvproperty(value=0, dtype=ChannelType.LONG)
    STR = pvproperty(value="test", dtype=ChannelType.STRING)
    FLOATARRAY = pvproperty(value=[1.0, 2.0, 3.0])
    ENUM = pvproperty(
        value="INIT", enum_strings=["INIT", "START", "STOP"], dtype=ChannelType.ENUM
    )


def _run_server():
    ioc_options, run_options = ioc_arg_parser(
        default_prefix="SIMPLE:", desc="Simple IOC"
    )
    ioc = SimpleIOC(**ioc_options)
    run(ioc.pvdb, **run_options)


def start_ca_server() -> Process:
    process = Process(target=_run_server)
    process.start()
    return process


def stop_ca_server(process: Process):
    process.terminate()
    process.join()
    process.close()


if __name__ == "__main__":
    _run_server()
