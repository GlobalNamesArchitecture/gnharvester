import json
import os.path
import shutil

from pyspark import SparkContext
# from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from settings import parse_spark, csvs_dir, mysql_export_dir

output_dir_name_string_indices = os.path.join(csvs_dir, "name_string_indices")


def cleanup():
    shutil.rmtree(output_dir_name_string_indices, ignore_errors=True)


def name_string_index_result(value):
    # name_string_id, name_string_row = value
    # url = name_string_row["url"].replace("\t", " ")
    # return "\t".join([name_string_row["data_source_id"], name_string_id, url])
    ((name_string_id, url), data_source_id) = value
    url = url.replace("\t", " ")
    return "\t".join([str(data_source_id), str(name_string_id), str(url)])


def main():
    cleanup()

    sc = SparkContext()
    # spark = SparkSession(sc)
    sql_context = SQLContext(sc)
    path = os.path.join(mysql_export_dir, "name_string_indices.tsv")
    df = sql_context.load(source='com.databricks.spark.csv', header='true', inferSchema='true', path=path, quote="щ",
                          delimiter="\t")

    # df = spark.read.csv(os.path.join(mysql_export_dir, "name_string_indices.tsv"), header=True, quote="", sep="\t")

    names = df.rdd.map(lambda x: x["name"])
    urls = df.rdd.map(lambda x: x["url"])
    data_source_ids = df.rdd.map(lambda x: x["data_source_id"])

    parsed_names_json = parse_spark(sc, names) \
        .map(lambda result: json.loads(result)["name_string_id"]) \
        .zip(urls).zip(data_source_ids) \
        .map(name_string_index_result)

    parsed_names_json.saveAsTextFile(output_dir_name_string_indices)


if __name__ == "__main__":
    main()
