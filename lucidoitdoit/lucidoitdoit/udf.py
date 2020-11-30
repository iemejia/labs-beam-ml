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

import ast
import logging
import uuid
from typing import Any

import RestrictedPython as rp
from RestrictedPython import safe_builtins
from RestrictedPython import limited_builtins
from RestrictedPython import utility_builtins


class LuciUdfRegistry(object):
    @staticmethod
    def udfize_def_string(code: str) -> str:
        """Return a string with the code as a function"""
        return """\
def udf(input):
 output = None
 {}
 return output
""".format(
            " ".join(line for line in code.splitlines(True))
        )

    @staticmethod
    def get_safe_globals():
        glbCtx = {"__builtins__": safe_builtins}

        # default overrides
        # https://restrictedpython.readthedocs.io/en/latest/usage/basic_usage.html#necessary-setup

        # enable classes
        glbCtx["__metaclass__"] = type

        # enable getters / iteration
        from RestrictedPython.Guards import safer_getattr
        from RestrictedPython.Eval import default_guarded_getitem
        from RestrictedPython.Eval import default_guarded_getiter
        glbCtx['_getattr_'] = safer_getattr
        glbCtx['_getitem_'] = default_guarded_getitem
        glbCtx['_getiter_'] = default_guarded_getiter

        # enable specific imports
        import importlib
        glbCtx['__import__'] = importlib.__import__
        
        return glbCtx



    @staticmethod
    def udfize_def(code: str, glbCtx: dict = None, lclCtx: dict = None):
        if glbCtx is None:
            glbCtx = LuciUdfRegistry.get_safe_globals()
        if lclCtx is None:
            lclCtx = {}

        udf = LuciUdfRegistry.udfize_def_string(code)
        udf_ast = ast.parse(udf, filename="<udf>")

        result = rp.compile_restricted(udf_ast, filename="<string>", mode="exec")
        exec(result, glbCtx, lclCtx)

        return lclCtx["udf"]

    def __init__(self) -> None:
        """Initialize the internal dictionary."""
        self.log = logging.getLogger(__name__)
        self.udf_registry = {}

    def put(self, code: str) -> bytes:
        id = str(uuid.uuid4()).encode("utf-8")
        try:
            self.udf_registry[id] = LuciUdfRegistry.udfize_def(code)
        except SyntaxError as e:
            # This might not be an accurate assessment of the position of the error.
            line = e.lineno - 2
            column = e.offset - 1
            self.log.debug("Guessing line and column: %s:%s", line, column)
            raise e
        return id

    def exec(self, id: bytes, input: str) -> Any:
        return self.udf_registry[id](input)


if __name__ == "__main__":
    input = "a,b,c,d,e"
    # script = "print(input)\n   output=input"
    # script = "import os"
    # script = "output = input + '1'"
    # script = "output = [input for i in range(0,5)]"
    script = "element = input.split(',')\noutput = element[3].upper()\n"
    # script = "import os\noutput = [input for i in range(0,5)]"
    # script = "import json\nelement = input.split(',')\noutput = element[3].upper()\n"
    # script = "i = 1\nwhile i < 6:\n\tprint(i)\n\ti += 1\noutput=input"

    # input = {"id": "id", "airlines": ['airline1'], "date": '01/01/2010', "lat": 40.7772, "lon": -73.9552 }
    # with open('lucidoitdoit/examples/geohash.py', 'r') as file:
    #     script = file.read()

    registry = LuciUdfRegistry()
    try:
        glbCtx = None
        lclCtx = {}
        udf = registry.udfize_def(script, glbCtx, lclCtx)
        output = udf(input)
        logging.debug(output)
        print(output)
    except SyntaxError as e:
        print(e)
