from dbt.version import __version__

def test_version():
    assert __version__ == '1.7.8'

