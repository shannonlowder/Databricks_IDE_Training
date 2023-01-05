# Databricks_IDE_Training
Let's use VSC to develop our Databricks solutions locally.



# this is the only way I could get dbutils to work locally
try:
    if dbutils:
        print("dbutils is available")
except NameError:
    print("dbutils is not available, use wrapper")
    import dbutils #pylint: disable=import-outside-toplevel