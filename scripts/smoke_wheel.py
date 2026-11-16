"""Run against the installed wheel, outside the source package."""

import subprocess
import sys
import sysconfig
from importlib.metadata import version

import pyasynq
from pyasynq.testing import memory_url

pyasynq.Asynq(memory_url())
assert pyasynq.__version__ == version("spater")
for command in ("spater", "pyasynq"):
    help_text = subprocess.check_output([command, "--help"], text=True)
    assert "usage: spater" in help_text
    assert "worker" in help_text and "scheduler" in help_text and "inspect" in help_text
if sysconfig.get_config_var("Py_GIL_DISABLED"):
    assert not sys._is_gil_enabled(), "Native module enabled the GIL"
print(pyasynq.__version__, sys.version)
