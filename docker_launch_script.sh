#!/bin/bash

python system_tests/helpers/pva_ioc.py &
python system_tests/helpers/ca_ioc.py &
python forwarder_launch.py & 

wait -n

exit $?
