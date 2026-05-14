import pytest
#from src.agent import run_query
import logging
from src.tools import is_readonly_query, execute_query
from src.data_manager import city_status, _load_into_spark

def test_is_readonly_query_allows():
    assert is_readonly_query("SELECT * from Buildings") is True
    assert is_readonly_query("  WITH x AS (SELECT 1) SELECT * FROM x  ") is True

def test_is_readonly_query_rejects():
    assert is_readonly_query("CREATE TABLE x (a INT)") is False
    assert is_readonly_query("DROP TABLE buildings") is False
    assert is_readonly_query("INSERT INTO x VALUES (1)") is False

# mock get_sedona()
def test_execute_query_returns_string_on_success(mocker):
    mock_get_sedona = mocker.patch("src.tools.get_sedona") # creates a MagicMock object and plugs it into that path
    # configure what the mock object return through .return_value
    mock_sedona = mocker.MagicMock() # define this to shorten syntax later
    mock_get_sedona.return_value = mock_sedona
    mock_sedona.sql.return_value.limit.return_value.toPandas.return_value.to_string.return_value= "col\n1"

    result = execute_query.invoke({"query": "SELECT 1"})

    assert isinstance(result, str) and "1" in result
    mock_sedona.sql.assert_called_once_with("SELECT 1")

# test real sedona
@pytest.fixture(scope="session")
def sedona():
    """Real Sedona session, created once per test run."""
    from src.sedona_client import get_sedona
    return get_sedona()

@pytest.mark.integration  # optional: run with pytest -m integration
def test_execute_query_real_sedona_simple(sedona):
    """Execute a trivial query against real Sedona (no buildings table)."""
    result = execute_query.invoke({"query": "SELECT 1 AS n"})
    assert isinstance(result, str)
    assert "1" in result

# test city status
def test_city_status(caplog):
    with caplog.at_level(logging.INFO):
        res = city_status("sydney")
    assert isinstance(res, dict)
    assert "Checking S3 for" in caplog.text
    assert res["on_disk"] is True
    assert res["in_spark"] is False
    assert res["enriched_location"] == "s3"

@pytest.mark.parametrize(
    "city, enriched_expected_status",
[("sydney",True), ("melbourne",False), ("brisbane",False), ("perth",False)])
def test_multi_city_status(city, enriched_expected_status):
    res = city_status(city)
    assert res["on_disk"] == enriched_expected_status

# # test _load_into_spark
# def test__load_into_spark(caplog, sedona):
#     # assert 
#     with caplog.at_level(logging.INFO):
#         res = _load_into_spark("sydney")
#     assert "pre-enriched" in caplog.text
