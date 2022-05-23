# Changes

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
