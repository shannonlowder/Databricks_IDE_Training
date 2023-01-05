"""
This module contains the code to ingest the data from the NYC Taxi & Limousine Commission website.

Classes:
    Ingest: This class contains the code to download the data from the NYC Taxi & Limousine
    Commission website and store it in delta format.

"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

class Ingest:
    """
    This class contains the code to ingest the data from the NYC Taxi & Limousine Commission
    website.

    Attributes:
        _builder (SparkSession.builder):  This is the SparkSession builder.
        _spark (SparkSession): This is the SparkSession.

    Methods:

    """

    _builder = SparkSession.builder.appName("nyctlc.Ingest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    _spark = configure_spark_with_delta_pip(_builder).getOrCreate()

    def __init__(self):
        """
        This method initializes the class. Not in use at this time.

        Parameters:

        """

    def load_parquet_to_delta(self, source_path, destination_folder) -> bool:
        """
        This method creates the silver delta table.

        Parameters:`
            source_path (str): path to the source file or folder.
            destination_folder (str): path to the destination folder.
        Returns:
            bool: True if successful, False if not.
        """
        try:
            source_df = self._spark.read.parquet(source_path)
            source_df.write.format("delta").mode("append").save(destination_folder)
            return True
        except BaseException as e: #pylint: disable=broad-except invalid-name
            print(e)
            return False
