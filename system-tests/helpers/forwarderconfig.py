import json


class ForwarderConfig:
    """
    Class that converts the pv information to a forwarder config.
    """

    def __init__(self, topic, using_v4=False, schema="f142"):
        self.schema = schema
        self.topic = topic
        self.using_v4 = using_v4

    def _get_converter(self):
        """
        Get the flatbuffers schema and the topic it's being applied to.
        
        :return:(dict) The dictionary of the schema and topic for the flatbuffers converter.
        """
        return {"schema": self.schema, "topic": "//localhost:9092/" + self.topic}

    def _create_stream(self, blk):
        """
        Create a stream for the JSON for specified block.
        
        :param blk:(string) The block containing the PV data.
        :return:(dict) The stream information including channel and flatbuffer encoding.
        """
        return {
            "channel": blk,
            "converter": self._get_converter(),
            "channel_provider_type": "pva" if self.using_v4 else "ca",
        }

    def create_forwarder_configuration(self, pvs):
        """
        Add all specified PVs and return JSON string.
        
        :param pvs:(list) The PVs in all blocks.
        :return: (string) The JSON configuration string.
        """
        output_dict = {"cmd": "add", "streams": [self._create_stream(pv) for pv in pvs]}
        return json.dumps(output_dict)

    def remove_forwarder_configuration(self, pvs):
        """
        Remove old forwarder configuration with the stop_channel command.
        
        :param pvs:(list) All PVs to be removed.
        :return:(list) A list of json strings with all PVs to remove.
        """
        output_list = []
        for pv in pvs:
            out_dict = {"cmd": "stop_channel", "channel": pv}
            output_list.append(json.dumps(out_dict))
        return output_list
