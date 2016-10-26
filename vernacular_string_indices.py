import os.path
import shutil
import uuid

from pyspark import SparkContext
from pyspark.sql import SQLContext
from settings import csvs_dir, mysql_export_dir, namespace

vernacular_string_indices_output = os.path.join(csvs_dir, "vernacular_string_indices")


def cleanup():
    shutil.rmtree(vernacular_string_indices_output, ignore_errors=True)


def extract_vernacular_strings_fields(row):
    name_uuid = uuid.uuid5(namespace, row["name"])
    return "\t".join([str(row["data_source_id"]), row["taxon_id"], str(name_uuid),
                      row["language"], row["locality"], row["country_code"]])


def main():
    cleanup()

    sc = SparkContext()
    sql_context = SQLContext(sc)
    path = os.path.join(mysql_export_dir, "vernacular_string_indices.tsv")
    df = sql_context.load(source='com.databricks.spark.csv', header='true', inferSchema='true', path=path,
                          quote="\u0000", delimiter="\t")

    df.rdd \
      .map(extract_vernacular_strings_fields) \
      .distinct() \
      .saveAsTextFile(vernacular_string_indices_output)


if __name__ == "__main__":
    main()
