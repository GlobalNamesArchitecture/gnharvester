import json
import os.path
import shutil

from pyspark import SparkContext
from pyspark.sql import SQLContext
from settings import parse_spark, csvs_dir, mysql_export_dir, empty_uuid

name_strings_output = os.path.join(csvs_dir, "name_strings")


def cleanup():
    shutil.rmtree(name_strings_output, ignore_errors=True)


def extract_name_strings_fields(result_json):
    name_string_id = result_json["name_string_id"]
    verbatim = result_json["verbatim"]
    if result_json["parsed"]:
        canonical_name = result_json["canonical_name"]["value"]
        canonical_name_uuid = result_json["canonical_name"]["id"]
        surrogate = str(result_json["surrogate"])
    else:
        canonical_name = ""
        canonical_name_uuid = str(empty_uuid)
        surrogate = "\\N"
    return "\t".join([name_string_id, verbatim, canonical_name_uuid, canonical_name, surrogate])


def main():
    cleanup()

    sc = SparkContext()
    sql_context = SQLContext(sc)
    path = os.path.join(mysql_export_dir, "name_strings.tsv")
    df = sql_context.load(source='com.databricks.spark.csv', header='true', inferSchema='true', path=path, quote="щ",
                          delimiter="\t")

    names = df.rdd.map(lambda x: x["name"])

    name_strings = parse_spark(sc, names) \
        .map(lambda result: json.loads(result)) \
        .map(extract_name_strings_fields) \
        .distinct()

    name_strings.saveAsTextFile(name_strings_output)


if __name__ == "__main__":
    main()
