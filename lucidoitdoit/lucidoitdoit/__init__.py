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
import sys
from typing import List

import lucidoitdoit.exception
from lucidoitdoit.frame import LuciFrame
from lucidoitdoit.udf import LuciUdfRegistry

"""LuciDoItServer is used to execute arbitrary python code on arbitrary data

Of course executing arbitrary python code is a dangerous, dangerous thing to do.  On the other
hand, do it.  Do it!  Do it do it do it.  It's your machine, who's gonna tell you want you can
and can't do with it?
"""


class LuciDoItServer(object):
    """Runs a server to execute python code"""

    def __init__(self, host: str, port: int, info_file: str = None) -> None:
        """
        Create a server.

        :param host: The host address to bind the server.
        :param port: The port to use, or 0 to pick any free port.
        :param info_file:  A file to save server information in.
        """
        self.host = host
        self.port = port
        self.__info_file = info_file
        self.__udf_registry = LuciUdfRegistry()

    def __on_bind(self, socket):
        """Called when the server socket is bound."""
        self.port = socket.getsockname()[1]
        if self.__info_file:
            with open(self.__info_file, "w") as f:
                logging.info("Writing info file: %s", self.__info_file)
                f.write(str(self.port))
        logging.info("Listening: %s", socket.getsockname())

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
            raise SystemError("Unknown type {}".format(type(out)))

        LuciFrame.write_response_code(conn, -1)

    def __run_02_request_server_shutdown(self, conn) -> None:
        """Requests a server shutdown."""
        LuciFrame.write_framed_binary(conn, "OK".encode("utf-8"))
        raise lucidoitdoit.exception.LuciShutdownRequested()

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
                self.__run_02_request_server_shutdown(connection)
            else:
                raise SystemError("Unknown command {}".format(cmd[0]))

    def run(self) -> None:
        """Runs the UDF execution service for a single client

        This can only serve one client at a time.  No other clients can connect while that client
        is being served.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            self.__on_bind(s)
            shutdown_requested = False
            while not shutdown_requested:
                s.listen(0)
                try:
                    # addr is a tuple of host, port
                    connection, addr = s.accept()
                    logging.info("Connected: %s", addr)
                    with connection:
                        self.serve_client(connection)
                except lucidoitdoit.exception.LuciClientDisconnect:
                    logging.info("Client disconnected: %s", addr)
                except lucidoitdoit.exception.LuciShutdownRequested:
                    shutdown_requested = True
                    logging.info("Client requested shutdown: %s", addr)
                finally:
                    s.shutdown(0)

    def run_multi(self) -> None:
        import socketserver

        class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
            def handle(inner_self):
                logging.info("Connected: %s", inner_self.client_address)
                # TODO error handling
                try:
                    self.serve_client(inner_self.request)
                except lucidoitdoit.exception.LuciClientDisconnect:
                    logging.info("Client disconnected: %s", inner_self.client_address)
                except lucidoitdoit.exception.LuciShutdownRequested:
                    logging.info(
                        "Client requested shutdown: %s", inner_self.client_address
                    )
                    inner_self.server.shutdown()

        class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
            pass

        server = ThreadedTCPServer((self.host, self.port), ThreadedTCPRequestHandler)
        self.__on_bind(server.socket)
        if sys.version_info.minor <= 5:
            # Python 3.5 doesn't have this as a context manager.
            server.serve_forever()
        else:
            with server:
                server.serve_forever()


class LuciDoItClient(object):
    """Client API for communicating with the server."""

    def __init__(self, host: str = "", port: int = 0) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port

    def __enter__(self):
        # Code to start a new transaction
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        return self

    def __exit__(self, type, value, tb):
        self.socket.close()

    def send_code(self, code: str) -> str:
        self.socket.sendall(bytes([0]))
        LuciFrame.write_framed_binary(self.socket, code.encode("utf-8"))
        return LuciFrame.read_framed_binary(self.socket).decode("utf-8")

    def send_input(self, code_id: str, msg: str) -> List[str]:
        self.socket.sendall(bytes([1]))
        LuciFrame.write_framed_binary(self.socket, code_id.encode("utf-8"))
        LuciFrame.write_framed_binary(self.socket, msg.encode("utf-8"))

        while True:
            (code, payload) = LuciFrame.read_coded_binary(self.socket)
            if code == -1:
                break
            yield payload.decode("utf-8")

    def send_shutdown(self) -> str:
        self.socket.sendall(bytes([2]))
        return LuciFrame.read_framed_binary(self.socket).decode("utf-8")


if __name__ == "__main__":
    pass
