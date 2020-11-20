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
import os
import tempfile
import threading
import time
import unittest

import lucidoitdoit


class LuciDoItServerThread(threading.Thread):
    """Thread for running a LuciDoItDoIt server."""

    def __init__(
        self,
        name,
        host: str = "",
        port: int = 0,
        info_file: str = None,
        multi: bool = False,
    ) -> None:
        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__)
        self.name = name
        self.server = lucidoitdoit.LuciDoItServer(host, port, info_file)
        self.multi = multi

    def run(self) -> None:
        self.log.info("Starting server thread %s.", self.name)
        if self.multi:
            self.server.run_multi()
        else:
            self.server.run()
        self.log.info("Finished server thread %s.", self.name)

    def get_port(self) -> int:
        """Blocking call that waits for the bind to occur on the running server."""
        while self.server.port == 0:
            time.sleep(1)
        self.log.info("Server discovered on port %s.", self.server.port)
        return self.server.port


class LuciDoItServerTestSuite(unittest.TestCase):
    """Test cases for running the LuciDoItServer."""

    def test_basic(self):
        """Very basic test case starting a server, testing a UDF and stopping."""

        # Run the server for a single client.
        logging.info("Starting a server.")
        server_thread = LuciDoItServerThread("test_basic", multi=False)
        server_thread.start()

        # Block until the server has been launched.
        port = server_thread.get_port()
        self.assertNotEqual(port, 0)

        code = "output = input.upper()"

        with lucidoitdoit.LuciDoItClient("", port) as client:
            logging.info("Connected: %s", client.socket.getsockname())

            code_id = client.send_code(code)
            self.assertNotEqual(id, "")

            output = list(client.send_input(code_id, "a"))
            self.assertEqual(1, len(output))
            self.assertIn("A", output)

            output = client.send_shutdown()
            self.assertEqual("OK", output)

        logging.info("Waiting for server to stop.")
        server_thread.join()
        logging.info("Server stopped.")

    def test_basic_multi(self):
        """A server supports two simultaneous clients."""
        logging.info("Starting a server.")
        server_thread = LuciDoItServerThread("test_basic", multi=True)
        server_thread.start()

        results = []
        with lucidoitdoit.LuciDoItClient("", server_thread.get_port()) as client1:
            with lucidoitdoit.LuciDoItClient("", server_thread.get_port()) as client2:
                # Note that the clients are mixing who registers code and who executes it!
                results.extend(
                    client2.send_input(client1.send_code("output = [input] * 75"), "A")
                )
                results.extend(
                    client1.send_input(client2.send_code("output = [input] * 50"), "B")
                )
                results.extend(
                    client2.send_input(client1.send_code("output = [input] * 25"), "C")
                )
            # client2 closed at this point and disconnected from the server
            client1.send_shutdown()

        self.assertEqual(150, len(results))
        server_thread.join()

    def test_basic_with_info_file(self):
        """A server generates an info file with port information."""
        d = tempfile.TemporaryDirectory()
        info_file = os.path.join(d.name, "lucidoitdoit.socket")

        self.assertTrue(os.path.exists(d.name))
        self.assertFalse(os.path.exists(info_file))

        # Run the server for a single client, and block until it's started.
        server_thread = LuciDoItServerThread("test_basic", info_file=info_file)
        server_thread.start()
        server_thread.get_port()

        # The file should be created and contain the port.
        self.assertTrue(os.path.exists(info_file))
        with open(info_file, "r") as f:
            content = f.read().strip()
            self.assertEqual(content, str(server_thread.get_port()))

        # Request a server shutdown.
        with lucidoitdoit.LuciDoItClient("", server_thread.get_port()) as client:
            output = client.send_shutdown()
            self.assertEqual("OK", output)
        server_thread.join()

        # The file isn't auto-cleaned.
        self.assertTrue(os.path.exists(info_file))
        d.cleanup()
        self.assertFalse(os.path.exists(d.name))
        self.assertFalse(os.path.exists(info_file))


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    unittest.main()
