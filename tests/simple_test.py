# MANUAL TEST
# docker-compose up
# Start incrementingioc.py
# Start forwarder.py
# Send config message to add PV: kafkacat -P -b localhost -t forwarder-config config_message_add.json
# Wait
# Send config message to remove PV: kafkacat -P -b localhost -t forwarder-config config_message_remove.json
# Stop everything
