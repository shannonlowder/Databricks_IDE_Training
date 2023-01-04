"""
Test the ingest module
"""
#import pytest
#from nyctlc import ingest #pylint: disable=import-error
from nyctlc import ingest

class TestIngest:
    """
    This class contains the unit tests for the ingest module.

    Methods:
        test_download_source: This method tests the download_source method.

    """
    def test_download_source(self):
        """
        This method tests the download_source method.

        Parameters:

        """
        #arrange
        i = ingest.Ingest()
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-10.parquet"
        destination_folder = "/Users/slowder/Repositories/Databricks/LocalStorage/bronze"


        #act
        result = i.download_source(url, destination_folder)
        #assert
        assert result is True
