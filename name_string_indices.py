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
    return [
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


empty_accepted_columns = ['NULL', 'NULL', 'NULL']


def is_synonym(value):
    name_json, rec = value
    return rec['accepted_taxon_id'] != rec['taxon_id']


def add_accepted_data_to_synonym_name(value):
    key, (syn, acpt) = value
    if acpt:
        acpt_json, acpt_rec = acpt
        output_acpt = [
            acpt_rec['taxon_id']
            , nullable(acpt_rec['name'])
            , nullable(acpt_json['name_string_id'])
        ]
    else:
        output_acpt = empty_accepted_columns
    return name_string_index_result(syn) + output_acpt


def add_accepted_data_accepted_name(value):
    return name_string_index_result(value) + empty_accepted_columns


def to_key_value(value):
    name_json, rec = value
    key = (rec['accepted_taxon_id'], rec['data_source_id'])
    return key, value


def join_fields(fields):
    assert len(fields) == 14
    return '\t'.join(fields)


def main():
    cleanup()

    sc = SparkContext()
    spark = SparkSession(sc)
    path = os.path.join(mysql_export_dir, "name_string_indices.tsv")

    df = spark.read.csv(path, header=True, inferSchema=True, sep='\t', nullValue='NULL')

    names = df.select('name').rdd.map(lambda r: r['name'])
    names_json = parse_spark(sc, names) \
        .map(json.loads) \
        .zip(df.rdd)

    synonym_names = names_json.filter(lambda n: is_synonym(n))
    accepted_names = names_json.filter(lambda n: not is_synonym(n))

    synonym_names_with_accepted_columns = synonym_names \
        .map(to_key_value) \
        .leftOuterJoin(accepted_names.map(to_key_value)) \
        .map(add_accepted_data_to_synonym_name)
    accepted_names_with_accepted_columns = accepted_names \
        .map(add_accepted_data_accepted_name)
    sc.union([synonym_names_with_accepted_columns, accepted_names_with_accepted_columns]) \
        .map(join_fields) \
        .saveAsTextFile(output_dir_name_string_indices)


if __name__ == "__main__":
    main()
