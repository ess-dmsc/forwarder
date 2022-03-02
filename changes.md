# Changes

* Repetition of messages will now be triggered only if no new message has been received within the configurable repetition time (`pv-update-period`).
* Drastic improvement to serialisation of f142 messages by updating to streaming-data-types 0.15.1
* Fixed bug when serialising empty senv-messages.
* Fixed bug where the wrong type was picked when casting numpy arrays in the f142 serialiser.
* Known bad PV updates will no longer be cached for (attempted) re-transmission.
* Some exceptions in the PVA/CA update handling code will now produce log messages.
* Added separate ep00 module for handling only connection status serialisation
