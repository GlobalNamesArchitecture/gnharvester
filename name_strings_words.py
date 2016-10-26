import json
import os.path
import shutil

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from settings import parse_spark, csvs_dir, mysql_export_dir


def extract_word_indexes(parsed_name_json, word_target):
    verbatim = parsed_name_json["verbatim"]
    name_string_id = parsed_name_json["name_string_id"]
    words_pos = filter(lambda j: j[0] == word_target, parsed_name_json["positions"])
    words = map(lambda word_pos: verbatim[word_pos[1]:word_pos[2]], words_pos)
    return map(lambda word: "\t".join([word, name_string_id]), words)


def main():
    sc = SparkContext()
    sql_context = SQLContext(sc)

    name_strings_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("normalized", StringType(), False),
        StructField("canonical_form_id", StringType(), False),
        StructField("has_words", StringType(), False),
        StructField("uuid", StringType(), False),
        StructField("has_groups", StringType(), False),
        StructField("surrogate", StringType(), False)])

    path = os.path.join(mysql_export_dir, "name_strings.tsv")
    df = sql_context.read \
        .format('com.databricks.spark.csv') \
        .options(header="true", delimiter="\t", quoteMode="NONE", quote=u"\u0000") \
        .load(path, schema=name_strings_schema)

    names = df.rdd.map(lambda x: x["name"])
    parsed_names_json = parse_spark(sc, names) \
                          .map(lambda result: json.loads(result))

    word_targets = ["author_word", "genus", "uninomial", "year",
                    "approximate_year", "infrageneric_epithet", "specific_epithet"]

    for word_target in word_targets:
        print("processing:", word_target)
        output_dir_word_target = os.path.join(csvs_dir, "word_indexes", word_target)

        shutil.rmtree(output_dir_word_target, ignore_errors=True)

        parsed_names_json \
            .filter(lambda parsed_name: "positions" in parsed_name) \
            .flatMap(lambda parsed_name: extract_word_indexes(parsed_name, word_target)) \
            .distinct() \
            .saveAsTextFile(output_dir_word_target)


if __name__ == "__main__":
    main()
