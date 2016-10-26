import json
import os.path
import shutil
import re

from pyspark import SparkContext
from pyspark.sql import SQLContext
from settings import parse_spark, csvs_dir, mysql_export_dir

output_dir_name_string_indices = os.path.join(csvs_dir, "name_string_indices")


def cleanup():
    shutil.rmtree(output_dir_name_string_indices, ignore_errors=True)


def name_string_index_result(value):
    (((((((((((name_string_id, url), data_source_id), taxon_id), global_id), local_id),
      nomenclatural_code_id), rank), accepted_taxon_id), classification_path),
      classification_path_ids), classification_path_ranks) = value
    url = url.replace("\t", " ")
    out = "\t".join([str(data_source_id), str(name_string_id), str(url), str(taxon_id), str(global_id),
                     str(local_id), str(nomenclatural_code_id), str(rank), str(accepted_taxon_id),
                     str(classification_path), str(classification_path_ids), str(classification_path_ranks)])
    return re.sub("\tNULL\t", "\t\\N\t", out)


def main():
    cleanup()

    sc = SparkContext()
    sql_context = SQLContext(sc)
    path = os.path.join(mysql_export_dir, "name_string_indices.tsv")
    df = sql_context.load(source='com.databricks.spark.csv', header='true', inferSchema='true', path=path,
                          quote="\u0000", delimiter="\t")

    names = df.rdd.map(lambda x: x["name"])
    urls = df.rdd.map(lambda x: x["url"])
    data_source_ids = df.rdd.map(lambda x: x["data_source_id"])
    taxon_id = df.rdd.map(lambda x: x["taxon_id"])
    global_id = df.rdd.map(lambda x: x["global_id"])
    rank = df.rdd.map(lambda x: x["rank"])
    accepted_taxon_id = df.rdd.map(lambda x: x["accepted_taxon_id"])
    classification_path = df.rdd.map(lambda x: x["classification_path"])
    classification_path_ids = df.rdd.map(lambda x: x["classification_path_ids"])
    nomenclatural_code_id = df.rdd.map(lambda x: x["nomenclatural_code_id"])
    local_id = df.rdd.map(lambda x: x["local_id"])
    classification_path_ranks = df.rdd.map(lambda x: x["classification_path_ranks"])

    parsed_names_json = parse_spark(sc, names) \
        .map(lambda result: json.loads(result)["name_string_id"]) \
        .zip(urls).zip(data_source_ids).zip(taxon_id) \
        .zip(global_id).zip(local_id).zip(nomenclatural_code_id).zip(rank) \
        .zip(accepted_taxon_id).zip(classification_path).zip(classification_path_ids) \
        .zip(classification_path_ranks) \
        .map(name_string_index_result)

    parsed_names_json.saveAsTextFile(output_dir_name_string_indices)


if __name__ == "__main__":
    main()
