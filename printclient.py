from caproto.threading.client import Context
from flatbufferhelpers import create_f142_message
from time import sleep

ctx = Context()
x, = ctx.get_pvs('incrementing_ioc:x')
sub = x.subscribe()


def monitor_callback(response):
    print(response.data[0])
    create_f142_message()


token = sub.add_callback(monitor_callback)

while True:
    sleep(10)
