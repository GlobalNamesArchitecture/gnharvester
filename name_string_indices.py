from __future__ import print_function

import json
import os.path
import shutil

from pyspark import SparkContext
from pyspark.sql import SparkSession
from settings import parse_spark, csvs_dir, mysql_export_dir

output_dir_name_string_indices = os.path.join(csvs_dir, "name_string_indices")


def cleanup():
    shutil.rmtree(output_dir_name_string_indices, ignore_errors=True)


def nullable(value):
    return value if value else 'NULL'


def name_string_index_result(value):
    name_json, rec = value
    output = [
        str(rec['data_source_id'])
        , name_json['name_string_id']
        , nullable(rec['url']).replace('\t', ' ')
        , nullable(rec['taxon_id'])
        , nullable(rec['global_id'])
        , nullable(rec['local_id'])
        , str(nullable(rec['nomenclatural_code_id']))
        , nullable(rec['rank'])
        , nullable(rec['classification_path'])
        , nullable(rec['classification_path_ids'])
        , nullable(rec['classification_path_ranks'])
    ]
    return '\t'.join(output)


def main():
    cleanup()

    sc = SparkContext()
    spark = SparkSession(sc)
    path = os.path.join(mysql_export_dir, "name_string_indices.tsv")

    df = spark.read.csv(path, header=True, inferSchema=True, sep='\t', nullValue='NULL')

    names = df.select('name').rdd.map(lambda r: r['name'])

    parse_spark(sc, names) \
        .map(json.loads) \
        .zip(df.rdd) \
        .map(name_string_index_result) \
        .saveAsTextFile(output_dir_name_string_indices)


if __name__ == "__main__":
    main()
