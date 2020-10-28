#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import socket

import lucidoitdoit
from lucidoitdoit.frame import LuciFrame
from lucidoitdoit.udf import LuciUdfRegistry


class LuciDoItServer(object):
    def __init__(self, host: str, port: int) -> None:
        """Initialize the  with a value."""
        self.host = host
        self.port = port
        self.__udf_registry = LuciUdfRegistry()

    def __run_00_register_code(self, conn) -> None:
        """Registers code to be executed as a udf."""
        code = LuciFrame.read_framed_binary(conn).decode("utf-8")
        udf_id = self.__udf_registry.put(code)
        logging.info("Registering code as %s", udf_id.decode("utf-8"))
        LuciFrame.write_framed_binary(conn, udf_id)

    def __run_01_execute(self, conn) -> None:
        """Executes a udf on a given record."""
        udf_id = LuciFrame.read_framed_binary(conn)
        input = LuciFrame.read_framed_binary(conn).decode("utf-8")

        out = self.__udf_registry.exec(udf_id, input)

        if isinstance(out, str):
            LuciFrame.write_framed_binary(conn, out.encode("utf-8"))
        elif isinstance(out, list):
            for outstr in out:
                LuciFrame.write_framed_binary(conn, outstr.encode("utf-8"))
        else:
            raise SystemError(f"Unknown type {type(out)}")

        LuciFrame.write_response_code(conn, -1)

    def serve_client(self, connection):
        """Communicate with the client on the given socket.

        This blocks until the server or client disconnects (and a LuciClientDisconnect exception
        is thrown.
        """
        while True:
            # The first byte determines what command is run
            cmd = LuciFrame.read_binary(connection, length=1)
            if cmd[0] == 0:
                self.__run_00_register_code(connection)
            elif cmd[0] == 1:
                self.__run_01_execute(connection)
            elif cmd[0] == 2:
                raise lucidoitdoit.exception.LuciShutdownRequested()
            else:
                raise SystemError(f"Unknown command {cmd[0]}")

    def run(self) -> None:
        """Runs the UDF execution service."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            logging.info("Listening: %s", s.getsockname())
            while True:
                s.listen()
                try:
                    # addr is a tuple of host, port
                    connection, addr = s.accept()
                    logging.info("Connected: %s", addr)
                    with connection:
                        self.serve_client(connection)

                except lucidoitdoit.exception.LuciClientDisconnect:
                    logging.info("Client shutdown: %s", addr)
                finally:
                    s.shutdown(0)

    def run_multi(self) -> None:
        import socketserver

        class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
            def handle(inself):
                logging.info("Connected: %s", inself)
                # TODO error handling
                self.serve_client(inself.request)

        class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
            pass

        server = ThreadedTCPServer((self.host, self.port), ThreadedTCPRequestHandler)
        logging.info("Listening: %s", server.socket.getsockname())
        with server:
            server.serve_forever()


if __name__ == "__main__":
    pass
