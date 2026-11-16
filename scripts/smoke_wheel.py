"""Run against the installed wheel, outside the source package."""

import sys
import sysconfig
from importlib.metadata import version

import pyasynq
from pyasynq.testing import memory_url

pyasynq.Asynq(memory_url())
assert pyasynq.__version__ == version("pyasynq")
if sysconfig.get_config_var("Py_GIL_DISABLED"):
    assert not sys._is_gil_enabled(), "Native module enabled the GIL"
print(pyasynq.__version__, sys.version)
