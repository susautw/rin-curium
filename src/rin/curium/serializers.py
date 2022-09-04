from json import JSONEncoder, JSONDecoder, JSONDecodeError
from threading import Lock
from typing import Type, Union, Dict

from rin import jsonutils

from . import ISerializer, CommandBase, exc
from .utils import cmd_to_dict_filter, add_error_handler


class JSONSerializer(ISerializer):
    _registry: Dict[str, Type[CommandBase]]
    _registry_lock: Lock

    def __init__(self, encoder: JSONEncoder = None, decoder: JSONDecoder = None):
        current_coder = jsonutils.get_current_coder()
        self.encoder = current_coder.encoder if encoder is None else encoder
        self.decoder = current_coder.decoder if decoder is None else decoder
        self._registry = {}
        self._registry_lock = Lock()

    @add_error_handler(TypeError, reraise_by=exc.UnsupportedObjectError)
    def serialize(self, cmd: CommandBase) -> bytes:
        json_str = self.encoder.encode(cmd.to_dict(recursive=True, filter=cmd_to_dict_filter))
        return json_str.encode()

    @add_error_handler(JSONDecodeError, reraise_by=exc.InvalidFormatError)
    def deserialize(self, raw_data: Union[bytes, dict]) -> CommandBase:
        if isinstance(raw_data, bytes):
            raw_data = self.decoder.decode(raw_data.decode())
        if '__cmd_name__' not in raw_data:
            raise exc.InvalidFormatError(f'{raw_data} does not contain __cmd_name__')

        cmd_name = raw_data.pop("__cmd_name__")
        with self._registry_lock:
            if cmd_name not in self._registry:
                raise exc.CommandNotRegisteredError(cmd_name)
            cmd_typ = self._registry[cmd_name]
        return cmd_typ(raw_data)

    def register_cmd(self, cmd_type: Type[CommandBase]) -> None:
        with self._registry_lock:
            if cmd_type.__cmd_name__ in self._registry:
                if cmd_type is not self._registry[cmd_type.__cmd_name__]:
                    raise exc.CommandHasRegisteredError(
                        f"Register command {cmd_type} using a duplicated name "
                        f"with command {self._registry[cmd_type.__cmd_name__]}'s name"
                    )
            self._registry[cmd_type.__cmd_name__] = cmd_type
