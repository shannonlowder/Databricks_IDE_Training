"""
This module contains all the stand-in code for dbutils.fs


RESUME HERE: run through all the functions to make sure I have them right.
the FILE:// handler shouldn't be necessary
"""
import glob
import os
import shutil
from typing import List

#i don't think we need recurse. if filepath is path, copy the path
#if it'sa  file, just copy the file
## what about src's that are globs?
def cp(path_from: str, path_to: str, recurse: bool = False) -> bool:#pylint: disable=invalid-name
    """
    Copies a file or directory, possibly across FileSystems

    Parameters:
        path_from (str): The path to the file or directory to copy
        path_to (str): The path to the destination file or directory
        recurse (bool): If true, copy directories recursively

    Returns:
        True if the copy is successful, False otherwise
    """
    #do we really want to get rid of the destination?
    #if os.path.exists(path_to):
    #    shutil.rmtree(path_to, ignore_errors=True)
    try:
        if recurse or os.path.isdir(path_from):
            shutil.copytree(path_from, path_to)
        else:
            shutil.copy(path_from, path_to)
        return True
    except BaseException as copy_error:#pylint: disable=broad-except
        print(copy_error)
        return False

def head(file: str, max_bytes: int = 65536) -> str:
    """
    Returns up to the first 'maxBytes' bytes of the given file as a String encoded in UTF-8

    Parameters:
        file (str): The path to the file to read
        maxBytes (int): The maximum number of bytes to read

    Returns:
        The first 'maxBytes' bytes of the file as a String encoded in UTF-8
    """
    rtn = ''
    if file:
        with open(file, 'r', encoding='UTF-8') as f:
            rtn = f.read(max_bytes)
    return rtn

def help() -> bool: #pylint: disable=redefined-builtin
    """
    This method prints the help for the class.
    """
    print('''
    This class is a stand-in for the Databricks dbutils.fs

    Methods:
        mv(from: String, to: String, recurse: boolean = false): boolean -> Moves a file or directory, possibly across FileSystems


    ''')
    return True
def ls(dir: str) -> List:
    """
    Lists the contents of a directory
    """
    rtn = list()
    if dir:
        if not os.path.exists(dir):
            raise FileNotFoundError
        for f in glob.glob(dir[7:] + "/*"):
            ff = f.replace('//', '/')
            rtn.append(FileInfo("file://{}".format(ff)))

    return rtn

def mkdirs(dir: str) -> bool:
    """
    Creates the given directory if it does not exist, also creating any necessary parent directories
    """
    if dir:
        os.makedirs(dir, exist_ok=True)
        return True

def mv(from_path, to_path) -> bool: #pylint: disable=invalid-name
    """
    This method moves a file or directory, possibly across FileSystems
    Parameters:
        from_path (str): The path to the file or directory to move
        to_path (str): The path to the destination file or directory


    Returns:
        True if the move is successful, False otherwise
    """
    try:
        shutil.move(from_path, to_path)
        return True
    except OSError as move_error:
        print(move_error)
        return False

def put(file: str, contents: str, overwrite: bool = False) -> bool:
    """
    Writes the given String out to a file, encoded in UTF-8
    """
    if file:
        if not overwrite and os.path.exists(file):
            raise Exception('already exist: {}'.format(file))
        with open(file, 'w', encoding='UTF-8') as f:
            f.write(contents)
        return True

    return False


def rm(dir: str, recurse: bool = False) -> bool:
    """
    Removes a file or directory
    """
    if dir.startswith("file:"):
        shutil.rmtree(dir[7:], recurse)
        return True

    return False


class FileInfo:
    def __init__(self, path: str):
        self.path = path
        self.name = path.split('/')[-1]
        self.isFile = os.path.isfile(path)
        self.size = 0
        if not os.path.isdir(path[7:]):
            self.size = os.path.getsize(path)

    def __str__(self) -> str:
        return "FileInfo(path='{}', name='{}', size={})".format(self.path, self.name, self.size)

    def __repr__(self) -> str:
        return self.__str__()

    def isDir(self) -> bool:
        return os.path.isdir(self.path[7:])