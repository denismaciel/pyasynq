import typing
from importlib.metadata import version

import pyasynq


def test_version_comes_from_rust():
    assert pyasynq.version() == version("pyasynq")
    assert pyasynq.__version__ == pyasynq.version()


def test_task_info_is_subscriptable():
    assert typing.get_origin(pyasynq.TaskInfo[int]) is pyasynq.TaskInfo
