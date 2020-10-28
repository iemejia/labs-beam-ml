import io
import struct
from typing import Any, Tuple

import avro

from lucidoitdoit import exception


class LuciFrame(object):
    """Helper methods for reading and writing from a connection."""

    @staticmethod
    def read_binary(connection, length: int) -> bytes:
        """Blocking call to read the specified number of bytes from the connection."""
        block = b""
        while length > 0:
            # This is a blocking call and will continue to wait until the client disconnects.
            data = connection.recv(length)
            if len(data) == 0:
                raise exception.LuciClientDisconnect()
            length -= len(data)
            if block == b"" and length == 0:
                return data
            block += data
        return block

    @staticmethod
    def read_framed_binary(connection):
        """Blocking call to read the next frame of bytes from the connection."""
        (length,) = struct.unpack("!i", LuciFrame.read_binary(connection, 4))
        return LuciFrame.read_binary(connection, length)

    @staticmethod
    def read_coded_binary(connection) -> Tuple[int, bytes]:
        """Blocking call to read the next frame of bytes from the connection."""
        (length,) = struct.unpack("!i", LuciFrame.read_binary(connection, 4))
        if length < 0:
            return (length, bytes(0))
        return (length, LuciFrame.read_binary(connection, length))

    @staticmethod
    def read_datum(connection, schema) -> Any:
        reader = io.BytesIO(LuciFrame.read_framed_binary(connection))
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(schema)
        return datum_reader.read(decoder)

    @staticmethod
    def write_response_code(connection, code: int) -> None:
        connection.sendall(struct.pack("!i", code))

    @staticmethod
    def write_framed_binary(connection, data: bytes, length: int = None) -> None:
        if length is None:
            length = len(data)
        connection.sendall(struct.pack("!i", length))
        connection.sendall(data)

    @staticmethod
    def write_datum(connection, datum, schema):
        bio = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bio)
        datum_writer = avro.io.DatumWriter(schema)
        datum_writer.write(datum, encoder)
        LuciFrame.write_framed_binary(connection, bio.getvalue())
