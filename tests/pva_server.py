from p4p.nt import NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV

scalar_double_pv = SharedPV(nt=NTScalar("d", valueAlarm=True), initial=0.0)
scalar_long_pv = SharedPV(nt=NTScalar("l"), initial=0)
scalar_string_pv = SharedPV(nt=NTScalar("s"), initial="initial value")
# NTEnum, NTNDArray


@scalar_long_pv.put
@scalar_double_pv.put
@scalar_string_pv.put
def handle(pv, op):
    # Just store new value and update subscribers
    pv.post(op.value())
    op.done()


def run_pva_server():
    Server.forever(
        providers=[
            {
                "SIMPLE:PVA:DOUBLE": scalar_double_pv,
                "SIMPLE:PVA:STR": scalar_string_pv,
                "SIMPLE:PVA:LONG": scalar_long_pv,
            }
        ]
    )  # Runs until KeyboardInterrupt


if __name__ == "__main__":
    run_pva_server()
