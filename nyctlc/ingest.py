"""
This module contains the code to ingest the data from the NYC Taxi & Limousine Commission website.

Classes:
    Ingest: This class contains the code to download the data from the NYC Taxi & Limousine
    Commission website and store it in delta format.

"""
import requests
from pyspark.sql import SparkSession
import dbutils.fs.dbfs_utils as dbfs #pylint: disable=import-error

class Ingest:
    """
    This class contains the code to ingest the data from the NYC Taxi & Limousine Commission
    website.
    Attributes:
        tmp_folder (str): path to your tmp (/tmp or c:\temp).

    Methods:

    """

    #_builder = SparkSession.builder.appName("nyctlc.ingest")

    _spark = SparkSession.builder.getOrCreate()

    tmp_folder = "/tmp"

    def __init__(self):
        """
        This method initializes the class. Not in use at this time.

        Parameters:

        """

    # may have to have the users upload the file to dbfs (standing in for ADF)
    # it would be nice to get dbutils to work locally, without connecting to a cluster
    def download_source(self, url, destination_folder, timeout=60):
        """
        This method downloads the source data from the web. Databricks doesn't like direct download
        to target. It likes download to tmp, then move to destination_folder.

        Parameters:
            url (str): The URL to download the data from.
            destination_folder (str): The folder to download the data to.
            timeout (int): The number of seconds to wait before timing out the request.

        returns:
            True if the download was successful, False otherwise.
        """

        #get the file name
        file_name = url.split("/")[-1]
        #get the file as a stream
        with requests.get(url, stream=True, timeout= timeout) as source:
            try:
                with open(self.tmp_folder + "/" +file_name, "wb") as target:
                    for chunk in source.iter_content(chunk_size=8192):
                        if chunk:
                            target.write(chunk)
            except OSError as write_error:
                print(write_error)
                return False

        #move the file from tmp to destination
        #dbutils = pyspark.dbutils.DBUtils(self._spark)
        #dbutils.fs.mv(self.tmp_folder + "/" +file_name, destination_folder + "/" +file_name)
        print(destination_folder + "/" +file_name)
        return dbfs.mv(self.tmp_folder + "/" +file_name, destination_folder + "/" +file_name)

