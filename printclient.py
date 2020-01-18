from caproto.threading.client import Context
from kafka.kafkahelpers import create_producer, publish_f142_message
from time import sleep

# EPICS
ctx = Context()
x, = ctx.get_pvs('incrementing_ioc:x')
sub = x.subscribe()

# Kafka
producer = create_producer()


def monitor_callback(response):
    print(response.data[0])
    publish_f142_message(producer, "python_forwarder_topic", int(response.data[0]))


token = sub.add_callback(monitor_callback)

while True:
    sleep(10)
