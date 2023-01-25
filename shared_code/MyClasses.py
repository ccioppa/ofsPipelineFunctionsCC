import typing
import json

class SerializableClass(object):
    """ Example serializable class.

    For a custom class to be serializable in
    Python Durable Functions, it needs to include both `to_json` and `from_json`,
    and `@staticmethod`s for serializing to JSON and back respectively. 
    These get called internally by the framework.
    """
    def __init__(self, payload: str):
        """ Construct the class
        Parameters
        ----------
        payload: str
            A payload to encapsulate
        """
        self.payload = payload

    def get_payload(self) -> str:
        """" Returns the payload """
        return self.payload

    @staticmethod
    def to_json(obj: object) -> str:
        """ Serializes a `SerializableClass` instance
        to a JSON string.

        Parameters
        ----------
        obj: SerializableClass
            The object to serialize

        Returns
        -------
        json_str: str
            A JSON-encoding of `obj`
        """
        return str(obj.payload)

    @staticmethod
    def from_json(json_str: str) -> object:
        """ De-serializes a JSON string to a
        `SerializableClass` instance. It assumes
        that the JSON string was generated via
        `SerializableClass.to_json`

        Parameters
        ----------
        json_str: str
            The JSON-encoding of a `SerializableClass` instance
        
        Returns
        --------
        obj: SerializableClass
            A SerializableClass instance, de-serialized from `json_str`
        """
        payload = json.loads(json_str)
        obj = SerializableClass(payload)
        return obj