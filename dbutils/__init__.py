"""
This module provides a local stand-in for the Databricks DBUtils module.
"""
from dbutils import fs

__all__ = ['fs']

def help(): #pylint: disable=redefined-builtin
    """
    adding a stand-in for Databricks help() method
    """
    print('''
    This module provides a local stand-in for the Databricks DBUtils module.

    fs: This module provides a local stand-in for the Databricks DBUtils.fs module.

    others coming soon!
    ''')