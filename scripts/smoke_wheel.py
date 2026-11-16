"""Run against the installed wheel, outside the source package."""

import subprocess
import sys
import sysconfig
from importlib.metadata import distribution, version
from importlib.util import find_spec

import spater
from spater.testing import memory_url

assert find_spec("pyasynq") is None, "Wheel installs the removed pyasynq module"
assert {ep.name for ep in distribution("spater").entry_points if ep.group == "console_scripts"} == {"spater"}
spater.Asynq(memory_url())
assert spater.__version__ == version("spater")
for command in ("spater",):
    help_text = subprocess.check_output([command, "--help"], text=True)
    assert "usage: spater" in help_text
    assert "worker" in help_text and "scheduler" in help_text and "inspect" in help_text
if sysconfig.get_config_var("Py_GIL_DISABLED"):
    assert not sys._is_gil_enabled(), "Native module enabled the GIL"
print(spater.__version__, sys.version)
