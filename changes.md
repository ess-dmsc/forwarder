# Changes

## Next

* 

## v2.1.0

* Fix: Do not re-publish al00 alarms if state has not changed
* Fix: In ep01 even if the PV was connected it appeared as unknown
* Fix: Updating tdct to handle empty arrays coming from EPICS
* Feat: Forward al00 messages for tdct, se00 and senv schemas
* Feat: Export `kafka_delivery_errors` metric when Kafka producer permanently fails delivery
* Feat: Add support for SASL_SSL in Kafka
* Feat: Make the package installable.
* Libs: Update confluent_kafka to v2.1

## v2.0.0

* Using ep01 instead of ep00 for EPICS connection events.
* Adding the schema "se00", which is in an upgrade to the schema "senv" which could not handle floats/doubles.
* Added support for f144 and al00 schemas.
* Refactoring to separate Serialisers in classes that implement a typing.Protocol.

## pre-2.0

* Repetition of messages will now be triggered only if no new message has been received within the configurable repetition time (`pv-update-period`).
* Drastic improvement to serialisation of f142 messages by updating to streaming-data-types 0.15.1
* Fixed bug when serialising empty senv-messages.
* Fixed bug where the wrong type was picked when casting numpy arrays in the f142 serialiser.
* Known bad PV updates will no longer be cached for (attempted) re-transmission.
* Some exceptions in the PVA/CA update handling code will now produce log messages.
* Re-factored the code to allow automatic instantiation of forwarding modules
* Added separate ep00 module for handling only connection status serialisation
* New ep00 module is now automatically instantiated for every PV that the forwarder is configured to forward
* System test running has been simplified and fixed
* Set the "facility" and "process_name" fields in the Graylog messages to "ESS" and "forwarder"
* System tests renamed to integration tests
* Repeated messages will now have the Kafka timestamp set to the current system time. This in an effort to reduce the load on the Kafka broker.
