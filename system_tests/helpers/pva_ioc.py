import time
from p4p.nt import NTScalar, NTEnum
from p4p.server import Server
from p4p.server.thread import SharedPV

DOUBLE = SharedPV(nt=NTScalar("d"), initial=0.0)
DOUBLE3 = SharedPV(
    nt=NTScalar("d"),
    initial={
        "value": 0.0,
        "timeStamp.secondsPastEpoch": int(time.time()),
        "timeStamp.nanoseconds": 0,
        "alarm.message": "HIGH_ALARM",
        "alarm.severity": 1,
        "alarm.status": 4,
    },
)
STR = SharedPV(nt=NTScalar("s"), initial="Initial string")
ENUM = SharedPV(
    nt=NTEnum(), initial={"index": 0, "choices": ["Yes", "No", "Maybe", "Perhaps"]}
)
LONG = SharedPV(nt=NTScalar("i"), initial=0)
BOOL = SharedPV(nt=NTScalar("?"), initial=False)
FLOATARRAY = SharedPV(nt=NTScalar("ad"), initial=[0, 0, 0, 0, 0])


@DOUBLE.put
@DOUBLE3.put
@STR.put
@ENUM.put
@LONG.put
@BOOL.put
@FLOATARRAY.put
def handle(pv, op):
    pv.post(
        {
            "value": op.value(),
            "timeStamp.secondsPastEpoch": int(time.time()),
            "timeStamp.nanoseconds": 0,
        }
    )
    op.done()


Server.forever(
    providers=[
        {
            "SIMPLE:DOUBLE": DOUBLE,
            "SIMPLE:DOUBLE3": DOUBLE3,
            "SIMPLE:STR": STR,
            "SIMPLE:ENUM": ENUM,
            "SIMPLE:LONG": LONG,
            "SIMPLE:BOOL": BOOL,
            "SIMPLE:FLOATARRAY": FLOATARRAY,
        }
    ]
)
