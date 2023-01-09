"""
Test the ingest module
"""
import pytest
from mdd import ingest_api #pylint: disable=import-error

class TestIngestAPI:
    """
    class to test the ingest_api module
    """
    @pytest.mark.parametrize("url, expected",
        [("https://statsapi.web.nhl.com/api/v1/teams", {"copyright", "teams"})]
    )

    def test_get_api_data(self, url, expected):
        """
        This method tests the get_api_data method.
        """
        #arrange


        #act
        test_ingest_api = ingest_api.IngestAPI()
        actual = test_ingest_api.get_json_from_endpoint(url)

        #assert we get back the keys we expect
        assert actual.keys() == expected

    @pytest.mark.parametrize("url, result_name, expected_count",
        [("https://statsapi.web.nhl.com/api/v1/teams", "teams", 32)])
    def test_get_nhl_results_row_count(self, url, result_name, expected_count):
        """
        This method tests the get_nhl_results method.
        """
        #arrange

        #act
        test_ingest_api = ingest_api.IngestAPI()
        actual = test_ingest_api.get_nhl_results_dataframe(
            test_ingest_api.get_json_from_endpoint(url), result_name)

        #assert we get back the keys we expect
        assert actual.count() == expected_count
