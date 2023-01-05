"""
Test the ingest module
"""
import pytest
import shutil
from delta import configure_spark_with_delta_pip
from nyctlc import ingest #pylint: disable=import-error
from pyspark.sql import SparkSession

class TestIngest:
    """
    This class contains the unit tests for the ingest module.

    Attributes:
        _builder (SparkSession.builder):  This is the SparkSession builder.
        _spark (SparkSession): This is the SparkSession.

    Methods:
        test_download_source: This method tests the download_source method.

    """

    _builder = SparkSession.builder.appName("nyctlc.TestIngest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    _spark = configure_spark_with_delta_pip(_builder).getOrCreate()

    @pytest.mark.parametrize("source_folder, destination_folder, expected",
        [
            ("/Users/slowder/Repositories/Databricks/LocalStorage/bronze/fhv",
            "/Users/slowder/Repositories/Databricks/LocalStorage/silver/fhv",
            1174988)])
    def test_load_parquet_to_delta_rowcount(self, source_folder, destination_folder, expected):
        """
        This method tests the download_source method.

        Parameters:

        """
        #arrange
        ingest_instance = ingest.Ingest()

        #act
        ingest_instance.load_parquet_to_delta(source_folder, destination_folder)
        actual = self._spark.read.format("delta").load(destination_folder).count()

        #assert
        assert actual == expected

        #cleanup, remove the destination folder when done.
        shutil.rmtree(destination_folder)
