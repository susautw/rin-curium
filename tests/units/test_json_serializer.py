import pytest
from rin.curium import exc
from rin.curium.serializers import JSONSerializer
from units.fake_commands import MyCommand, AnotherCommand


@pytest.fixture
def serializer():
    return JSONSerializer()


def test_serialize(serializer):
    cmd = MyCommand(x=2, y=[1, 2, 3])
    assert serializer.serialize(cmd) == b'{"x": 2, "y": [1, 2, 3], "__cmd_name__": "my_command"}'


def test_serialize__with_obj_cannot_convert_to_json(serializer):
    cmd = MyCommand(x=object(), y=[1, 2, 3])
    with pytest.raises(exc.UnsupportedObjectError):
        serializer.serialize(cmd)


def test_deserialize(serializer):
    excepted_cmd_dict = {"x": 2, "y": [1, 2, 3], "z": 1, "p": True, '__cmd_name__': 'my_command'}

    raw_data = b'{"x": 2, "y": [1, 2, 3], "__cmd_name__": "my_command"}'
    raw_data_dict = {"x": 2, "y": [1, 2, 3], "__cmd_name__": "my_command"}

    serializer.register_cmd(MyCommand)

    assert serializer.deserialize(raw_data).to_dict(recursive=True) == excepted_cmd_dict
    assert serializer.deserialize(raw_data_dict).to_dict(recursive=True) == excepted_cmd_dict


@pytest.mark.parametrize("data, match", [
    (b'{wrong: raw_data}', None),
    (b'{"x": "no_command_name"}', "{'x': 'no_command_name'} does not contain __cmd_name__"),
    ({"x": "no_command_name"}, "{'x': 'no_command_name'} does not contain __cmd_name__"),
])
def test_deserialize__with_wrong_format_raw_data(serializer, data, match):
    with pytest.raises(exc.InvalidFormatError, match=match):
        serializer.deserialize(data)


def test_deserialize__with_not_registered_command_name(serializer):
    with pytest.raises(exc.CommandNotRegisteredError, match="not_registered_cmd"):
        serializer.deserialize({"__cmd_name__": "not_registered_cmd"})


def test_register_cmd(serializer):
    serializer.register_cmd(MyCommand)
    assert serializer._registry[MyCommand.__cmd_name__] == MyCommand


def test_register_cmd__with_duplicated_command_name(serializer):
    serializer.register_cmd(MyCommand)
    serializer.register_cmd(MyCommand)  # expected no error raised
    with pytest.raises(
            exc.CommandHasRegisteredError,
            match=f"Register command {AnotherCommand} using a duplicated name with command {MyCommand}'s name"
    ):
        serializer.register_cmd(AnotherCommand)
