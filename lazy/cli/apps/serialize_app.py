"""
CLI to work with lazy.serialize

- Tested to work with local, s3, gs

Useful examples:

- working with secrets like AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
---
echo "$AWS_ACCESS_KEY_ID" | xargs lazy encode b64
>>> QUtJ... 
echo "$AWS_ACCESS_KEY_ID" | xargs lazy encode bgz
>>> H4s...

echo "$AWS_ACCESS_KEY_ID" | export AWS_KEY_ID_BGZ=$(xargs lazy encode bgz)
echo "$AWS_KEY_ID_BGZ" | xargs lazy decode bgz
>>> A...
---

- working with service_account credentials
--- 
lazy encode jsonbgz --input-path '/path/to/adc.json'
>>> H4sIAJxJ52EC/5VWWa...

lazy decode jsonbgz $ADC_BGZ --output-path '/path/to/adc.json'
>>> /path/to/adc.json
---

- read & replace a template file
--- 
lazy read replace 's3://bucket/to/config_template.yaml' --output-path '/local/path/config.yaml' \
    --fix "<CLUSTER_NAME>=$CLUSTER_NAME" \
    --fix "<CLUSTER_VER>=$CLUSTER_VER" \
    --fix "<IMAGE_TAG>=$IMAGE_TAG"
>>> /local/path/config.yaml
---
"""

from .base import *
from lazy.serialize import Serializer

__all__ = ( 
    'encodeCli',
    'decodeCli',
    'keysCli',
    'readCli',
    'writeCli',
)
encodeCli = createCli(name = 'encode')
decodeCli = createCli(name = 'decode')
keysCli = createCli(name = 'keys')

readCli = createCli(name = 'read')
writeCli = createCli(name = 'write')


## Will import and run func dynamically
def _get_path(path: str):
    from lazy.io import get_path
    return get_path(path)


@readCli.command('file', short_help = "Reads from a File, writes to the --output-path [optional]")
def read_file(
        input_path: str = Argument(None),
        output_path: Optional[str] = Argument(None),
        binary: Optional[bool] = Option(False),
    ):
    input_path = _get_path(input_path)
    data = input_path.read_text() if not binary else input_path.read_binary()
    if output_path:
        output_path = _get_path(output_path)
        if binary: output_path.write_binary(data)
        else: output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@readCli.command('replace', short_help = "Reads from a File, replaces --fix x=y=num [sed-like], writes to the --output-path [optional]")
def read_replace_file(
        input_path: str = Argument(None),
        fix: Optional[List[str]] = Option(None),
        output_path: Optional[str] = Option(None),
        binary: Optional[bool] = Option(False),
    ):
    input_path = _get_path(input_path)
    data = input_path.read_text() if not binary else input_path.read_binary()
    for f in fix:
        change = f.split('=')
        data = data.replace(str(change[0]), str(change[1]), int(change[2])) if len(change) == 3 else data.replace(str(change[0]), str(change[1]))
    if output_path:
        output_path = _get_path(output_path)
        if binary: output_path.write_binary(data)
        else: output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@writeCli.command('file', short_help = "Write to a File from text or from --input-path [optional], writes it to the --output-path [optional]")
def write_file(
        text: Optional[str] = Argument(None),
        input_path: str = Option(None),
        output_path: Optional[str] = Option(None),
        append: Optional[bool] = Option(False),
        binary: Optional[bool] = Option(False),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = input_path.read_text() if not binary else input_path.read_binary()
    else:
        data = text.encode('utf-8') if binary else text
    if output_path:
        output_path = _get_path(output_path)
        if binary: output_path.write_binary(data)
        else: 
            mode = 'a' if append else 'w'
            with output_path.open(mode = mode) as f:
                f.write(data)
                if append: f.write('\n')
        echo(output_path.as_posix())
    else:
        echo(data)


@decodeCli.command('json', short_help = "Decodes JSON string or JSON File into text")
def decode_json(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = input_path.read_json()
    else:
        data = Serializer.DefaultJson.loads(text)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(Serializer.DefaultJson.dumps(data))
        echo(output_path.as_posix())
    else:
        echo(Serializer.DefaultJson.dumps(data))


@encodeCli.command('jsonb64', short_help = "Encodes a JSON String or JSON File into Base64")
def encode_json_b64(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.Base.b64_encode(Serializer.DefaultJson.dumps(input_path.read_json()))
    else:
        # We do this to ensure valid JSON encoding
        data = Serializer.Base.b64_encode(Serializer.DefaultJson.dumps(Serializer.DefaultJson.loads(text)))
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@encodeCli.command('jsonbgz', short_help = "Encodes a JSON String or JSON File into Base64 + GZIP")
def encode_json_bgz(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.Base.b64_gzip_encode(Serializer.DefaultJson.dumps(input_path.read_json()))
    else:
        # We do this to ensure valid JSON encoding
        data = Serializer.Base.b64_gzip_encode(Serializer.DefaultJson.dumps(Serializer.DefaultJson.loads(text)))
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@decodeCli.command('jsonb64', short_help = "Decodes a Base64 String into JSON")
def decode_json_b64(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.DefaultJson.loads(Serializer.Base.b64_decode(input_path.read_text()))
    else:
        data = Serializer.DefaultJson.loads(Serializer.Base.b64_decode(text))
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(Serializer.DefaultJson.dumps(data))
        echo(output_path.as_posix())
    else:
        echo(Serializer.DefaultJson.dumps(data))


@decodeCli.command('jsonbgz', short_help = "Decodes a Base64 + GZIP String into JSON")
def decode_json_bgz(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.DefaultJson.loads(Serializer.Base.b64_gzip_decode(input_path.read_text()))
    else:
        data = Serializer.DefaultJson.loads(Serializer.Base.b64_gzip_decode(text))
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(Serializer.DefaultJson.dumps(data))
        echo(output_path.as_posix())
    else:
        echo(Serializer.DefaultJson.dumps(data))


@decodeCli.command('pickle')
def decode_pickle(
        text: str = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.Pkl.loads(input_path.read_bytes())
    else:
        data = Serializer.Pkl.loads(text.encode('utf-8'))
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_bytes(Serializer.Pkl.dumps(data))
        echo(output_path.as_posix())
    else:
        echo(data)


@encodeCli.command('pickle')
def encode_pickle(
        text: str = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = input_path.read_bytes()
    else:
        data = Serializer.Pkl.dumps(text.encode('utf-8'))
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_bytes(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@decodeCli.command('b64', short_help = "Decodes string or Text File from Base64")
def decode_b64(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.Base.b64_decode(input_path.read_text())
    else:
        data = Serializer.Base.b64_decode(text)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@encodeCli.command('b64', short_help = "Encodes string or Text File to Base64")
def encode_b64(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.Base.b64_encode(input_path.read_text())
    else:
        data = Serializer.Base.b64_encode(text)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@decodeCli.command('bgz', short_help = "Decodes string or Text File from Base64 + GZIP")
def decode_bgz(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.Base.b64_gzip_decode(input_path.read_text())
    else:
        data = Serializer.Base.b64_gzip_decode(text)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@encodeCli.command('bgz', short_help = "Encodes string or Text File to Base64 + GZIP")
def encode_bgz(
        text: Optional[str] = Argument(None),
        input_path: Optional[str] = Option(None),
        output_path: Optional[str] = Option(None),
    ):
    if input_path:
        input_path = _get_path(input_path)
        data = Serializer.Base.b64_gzip_encode(input_path.read_text())
    else:
        data = Serializer.Base.b64_gzip_encode(text)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(data)
        echo(output_path.as_posix())
    else:
        echo(data)


@keysCli.command('uuid', short_help = "Creates a UUID from UUID4")
def create_uuid(
        length: Optional[int] = Argument(None), 
        output_path: Optional[str] = Option(None),
        clean: bool = Option(False),
    ):
    result_uid = ''
    if length:
        while len(result_uid) <= length:
            _uid = Serializer.Base.get_uuid()
            if clean: _uid = _uid.replace('-', '')
            result_uid += _uid
        result_uid = result_uid[:length]
    else:
        result_uid = Serializer.Base.get_uuid()
        if clean: result_uid = result_uid.replace('-', '')
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(result_uid)
        echo(output_path.as_posix())
    else:
        echo(result_uid)


@keysCli.command('secret', short_help = "Creates an Alphanumeric Secret Passcode")
def create_alpha_passcode(
        length: Optional[int] = Argument(32),
        output_path: Optional[str] = Option(None),
    ):
    result = Serializer.Secret.alphanumeric_passcode(length=length)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(result)
        echo(output_path.as_posix())
    else:
        echo(result)


@keysCli.command('keypair', short_help = "Creates a Keypair [access_key | secret_key]")
def create_keypair(
        key_length: Optional[int] = Option(16), 
        secret_length: Optional[int] = Option(32),
        output_path: Optional[str] = Option(None),
    ):
    result = Serializer.Secret.keypair(k_len=key_length, s_len=secret_length)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(result)
        echo(output_path.as_posix())
    else:
        echo(result)


@keysCli.command('token', short_help = "Creates a Token [API Token]")
def create_token(
        length: Optional[int] = Argument(16),
        clean: Optional[bool] = Option(True), 
        safe: Optional[bool] = Option(False),
        output_path: Optional[str] = Option(None),
    ):
    result = Serializer.Secret.token(length=length, safe=safe, clean=clean)
    if output_path:
        output_path = _get_path(output_path)
        output_path.write_text(result)
        echo(output_path.as_posix())
    else:
        echo(result)

