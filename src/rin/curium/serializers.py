from json import JSONEncoder, JSONDecoder
from threading import Lock
from typing import Type, Union, Dict

from rin import jsonutils

from . import ISerializer, CommandBase
from .utils import cmd_to_dict_filter


class JSONSerializer(ISerializer):
    _registry: Dict[str, Type[CommandBase]]
    _registry_lock: Lock

    def __init__(self, encoder: JSONEncoder = None, decoder: JSONDecoder = None):
        current_coder = jsonutils.get_current_coder()
        self.encoder = current_coder.encoder if encoder is None else encoder
        self.decoder = current_coder.decoder if decoder is None else decoder
        self._registry = {}
        self._registry_lock = Lock()

    def serialize(self, cmd: CommandBase) -> bytes:
        json_str = self.encoder.encode(cmd.to_dict(recursive=True, filter=cmd_to_dict_filter))
        return json_str.encode()

    def deserialize(self, raw_data: Union[bytes, dict]) -> CommandBase:
        if isinstance(raw_data, bytes):
            raw_data = self.decoder.decode(raw_data.decode())
        if '__cmd_name__' not in raw_data:
            raise ValueError(f'{raw_data} does not contain __cmd_name__')

        cmd_name = raw_data.pop("__cmd_name__")
        with self._registry_lock:
            if cmd_name not in self._registry:
                raise RuntimeError(f'command {cmd_name} is not registered')
            cmd_typ = self._registry[cmd_name]
        return cmd_typ(raw_data)

    def register_cmd(self, cmd_type: Type[CommandBase]):
        with self._registry_lock:
            self._registry[cmd_type.__cmd_name__] = cmd_type
