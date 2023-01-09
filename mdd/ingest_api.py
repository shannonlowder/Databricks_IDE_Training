"""
This module will demonstrate an API in a metadata driven way.
"""
import json
import requests
#from pyspark.context import SparkContext
from pyspark.sql import SparkSession

class IngestAPI:
    """
    This class contains methods to ingest API data in a metadata driven way.
    """
    #internal spark variables
    _builder = SparkSession.builder.appName("IngestAPI")

    _spark = SparkSession.builder.getOrCreate()

    _sc = _spark.sparkContext

    def __init__(self):
        """
        This method initializes the class.
        """

    def get_json_from_endpoint(self, url, timeout = 60):
        """
        This method gets the JSON data from the API.

        Parameters:
            url (str): This is the url to the API endpoint.
            timeout (int): This is the timeout for the API call.

        """
        try:
            response = requests.get(url, timeout=timeout)
            return response.json()
        except requests.exceptions.HTTPError as request_error:
            print(request_error)
            return None

    def get_nhl_results_dataframe(self, input_json, result_name):
        """
        The NHL API returns a dictionary of copyright and <results> for each endpoint.
        This method will get the results from the JSON data as a dataframe.

        Parameters:
            input_json (dict): This is the JSON data
            result_name (str): This is the name of the results key in the JSON data.
        """
        if not result_name in input_json.keys():
            print(f"The JSON data does not contain {result_name}.")
            return None

        result_json = json.dumps(input_json[result_name])
        return self._spark.read.json(self._sc.parallelize([result_json]))
