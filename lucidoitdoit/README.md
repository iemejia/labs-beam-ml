python example
==============

* The [**`socket`**](https://docs.python.org/3/library/socket.html) module - low-level networking
  interface
  - [Tutorial - Python sockets](https://realpython.com/python-sockets/)
  - [Unix domain sockets](https://pymotw.com/2/socket/uds.html)
* The [**`serversocket`**](https://docs.python.org/3/library/http.server.html) module - A framework for network servers
* The [**`http.server`**](https://docs.python.org/3/library/http.server.html) module 
* The [**`ast`**](https://docs.python.org/3/library/ast.html) module - Abstract Syntax Trees
  - Scary example code:
    - [**_Scary1_**](https://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html): has 
      some good discussion on how to sanitize
    - [**_Scary2_**](https://nedbatchelder.com/blog/201302/finding_python_3_builtins.html)
    - [**_Scary3_**](https://stackoverflow.com/questions/35804961/python-eval-is-it-still-dangerous-if-i-disable-builtins-and-attribute-access)
  - [Tutorial - Dynamic Python](https://realpython.com/python-eval-function/)
  - [Better docs](https://greentreesnakes.readthedocs.io/en/latest/)
  

Project packaging and setup
---------------------------

How do you set up a project again?  What are all of those files for?

- [How To Package Your Python Code](https://python-packaging.readthedocs.io/en/latest/)
- [Hitchhiker's Guide - Structuring Your Project](https://docs.python-guide.org/writing/structure/)
- [Python packaging - Past, Present, Future](https://www.bernat.tech/pep-517-518/) (Feb 2019)
- Example code:
  - https://github.com/pypa/sampleproject
  - https://github.com/navdeep-G/samplemod

Files:

* [**`.gitignore`**](https://raw.githubusercontent.com/github/gitignore/master/Python.gitignore): 
  Lots of examples of temporary and build files generated during the software lifecycle.  
* [**`LICENSE`** (ASL-2)](https://www.apache.org/licenses/LICENSE-2.0): How you want your software 
  used and distributed. _See also setup.py_
* [**`MANIFEST.in`**](https://packaging.python.org/guides/using-manifest-in/): Files included in 
  the source distribution
* **`README.md`**: This file, used in github but also in the distributed package. 
* [**`setup.py`**](https://setuptools.readthedocs.io/en/latest/) 


Testing and automation
----------------------

### [Black](https://black.readthedocs.io/en/stable/): The uncompromising code formatter

```
pip install black
black lucidoitdoit tests bin
```

### [Nose](https://nose.readthedocs.io/en/latest/) is nicer testing for python

```
pip install nose
nosetests
```

### [Tox](https://tox.readthedocs.io/en/latest/): standardize testing in Python

```
# TODO: The config files.
pip install tox
tox
```

### [Flake8](https://flake8.pycqa.org/en/latest/): Your Tool For Style Guide Enforcement

```
pip install flake
flake8 lucidoitdoit tests bin
```

IntelliJ setup
--------------

* IntelliJ
  - [Using the Python plugin](https://www.jetbrains.com/help/idea/plugin-overview.html#63317)
  - Set up a Python SDK virtualenv in `$PROJECT/env`


Building
--------

```
# Setting up and using virtualenv.
python -m venv env/
source env/bin/activate

setup.py test
setup.py sdist

# After installing, you can run the script.
setup.py install

# Run a server.
lucidoitdoit server

# In the same virtualenv while the server is running.
lucidoitdoit client 'output = input.upper().split(" ")' "a b c"
```
