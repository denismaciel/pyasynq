import typing
from importlib.metadata import version

import spater


def test_version_comes_from_rust():
    assert spater.version() == version("spater")
    assert spater.__version__ == spater.version()


def test_task_info_is_subscriptable():
    assert typing.get_origin(spater.TaskInfo[int]) is spater.TaskInfo
