import os.path
import shutil
import uuid

from pyspark import SparkContext
from pyspark.sql import SQLContext
from settings import csvs_dir, mysql_export_dir, namespace

vernacular_strings_output = os.path.join(csvs_dir, "vernacular_strings")


def cleanup():
    shutil.rmtree(vernacular_strings_output, ignore_errors=True)


def extract_vernacular_strings_fields(name):
    name_uuid = uuid.uuid5(namespace, name)
    return "\t".join([str(name_uuid), name])


def main():
    cleanup()

    sc = SparkContext()
    sql_context = SQLContext(sc)
    path = os.path.join(mysql_export_dir, "vernacular_strings.tsv")
    df = sql_context.load(source='com.databricks.spark.csv', header='true', inferSchema='true', path=path,
                          quote="\u0000", delimiter="\t")

    names = df.rdd.map(lambda x: x["name"])

    name_strings = \
        names.map(extract_vernacular_strings_fields) \
             .distinct()

    name_strings.saveAsTextFile(vernacular_strings_output)


if __name__ == "__main__":
    main()
