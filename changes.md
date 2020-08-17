# Changes

*Add to release notes and clear this list when creating a new release*

- When requesting to remove stream configurations, wildcards '*' and '?' can now be
used in the output topic and channel name fields.

- Can request Forwarder to store the current configuration and retrieve it when restarting.

- Added more information to the README.

- Added system test for stored config.

- Empty PV updates are not forwarded and are not cached to send in periodic updates.
This was added to address the issue of empty chopper timestamp updates when a chopper is not spinning.

- Chopper timestamps to be forwarded with tdct schema are now assumed to be in nanoseconds and relative
to the EPICS update timestamp, they are converted to nanosecond-precision unix timestamps when forwarded. 

- Connection state changes with the EPICS server are published to Kafka using the ep00 schema
