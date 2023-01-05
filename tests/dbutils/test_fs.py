"""
Test the LocalDBUtils.fs module
"""
import os
import pathlib
import pytest
import dbutils #pylint: disable=import-error

class TestDBUtilsFS:
    """
    This class contains the unit tests for the dbutils module.

    Methods:
        test_help: This method tests the help method.

    """
    @pytest.mark.skip(reason="skip for early demo")
    #@pytest.mark.parametrize("src, dest, recurse, expected", [("/tmp/test_src", "/tmp/test_dest", True, True)
    #    , ("/tmp/test_src/src_test.txt", "/tmp/test_dest", False, True)])
    def test_cp(self, src, dest, recurse, expected):
        """
        This method tests the cp method.

        Parameters:
            src (str): The source filepath
            dest (str): The destination filepath
            recurse (bool): If true, copy directories recursively
            expected (bool): The expected result

        """
        #arrange
        # if dealing with a file, get the parent directory
        if pathlib.Path(src).is_file():
            src_directory = pathlib.Path(src).parent
        else:
            src_directory = src
        #if we don't have the source directory, create it
        if not os.path.exists(src_directory):
            os.makedirs(src_directory)

        #create a file, if the src  is a file
        if pathlib.Path(src).is_file():
            with open(src, "w", encoding='utf-8') as f:#pylint: disable=invalid-name
                f.write("test")

        #if the destination exists, remove it
        if os.path.exists(pathlib.Path(dest)):
            os.removedirs(pathlib.Path(dest))

        #act
        result = dbutils.fs.cp(src, dest)

        #assert
        assert result == expected

        #cleanup
        os.removedirs(pathlib.Path(dest))

    @pytest.mark.skip(reason="skip for early demo")
    def test_fs_help(self):
        """
        Simple test to make sure the help method works.

        Parameters:

        """
        #arrange
        #dbutils = dbu.LocalDBUtils()
        #act
        result = dbutils.fs.help()

        #assert
        assert result is True

    @pytest.mark.skip(reason="Not implemented yet")
    def test_fs_mv(self):
        """
        This method tests the mv method.

        Parameters:

        """
        #arrange

        #act
        result = dbutils.fs.mv("bronze", "silver")

        #assert
        assert result is True
