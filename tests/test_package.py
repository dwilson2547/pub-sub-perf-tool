"""Test package imports"""


def test_import():
    """Test that the package can be imported"""
    import pub_sub_perf_tool
    assert pub_sub_perf_tool.__version__ == "0.1.0"
