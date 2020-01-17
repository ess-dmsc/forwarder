from caproto.threading.client import Context
from time import sleep

ctx = Context()
x, = ctx.get_pvs('incrementing_ioc:x')
sub = x.subscribe()


def monitor_callback(response):
    print(response.data[0])


token = sub.add_callback(monitor_callback)

while True:
    sleep(10)
