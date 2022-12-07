"""test_airflow_ml module."""
from airflow_ml import __version__


def test_version():
    """Test version."""
    assert __version__ == "0.1.0"
