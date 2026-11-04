import pyasynq


def test_version_comes_from_rust():
    assert pyasynq.version() == "0.1.0"
    assert pyasynq.__version__ == "0.1.0"
