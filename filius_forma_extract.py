import json
import os.path
import shutil
import re

from pyspark import SparkContext
from pyspark.sql import SQLContext
from settings import parse_spark, csvs_dir, mysql_export_dir

filius_forma_output = os.path.join(csvs_dir, 'filius_forma')


def cleanup():
    shutil.rmtree(filius_forma_output, ignore_errors=True)


def find_filius_forma(result_json):
    if not result_json['parsed']:
        return False
    if len(re.findall(r'\w+', str(result_json['canonical_name']))) < 3:
        return False

    positions = result_json['positions']
    verbatim = result_json['verbatim']
    for idx, pos in enumerate(positions):
        if verbatim[int(pos[1]):int(pos[2])] == 'f.' and \
                (pos[0] == 'author_word_filius' or pos[0] == 'rank'):
            idx_species = idx
            while idx_species >= -1 and positions[idx_species][0] != 'specific_epithet':
                idx_species -= 1
            if idx_species == -1 or idx - idx_species < 2:
                continue
            if verbatim[positions[idx_species + 1][1] - 1] == '(' or \
                    verbatim[positions[idx][2]:positions[idx][2] + 1] == ')':
                continue
            if idx + 1 >= len(positions) or positions[idx + 1][0] != 'infraspecific_epithet':
                continue
            if idx + 2 < len(positions) and positions[idx + 2][0] == 'rank':
                continue
            return True

    return False


def extract_name_string(result_json):
    name_string_id = result_json['name_string_id']
    verbatim = result_json['verbatim']

    canonical_name = result_json['canonical_name']['value']
    canonical_name_uuid = result_json['canonical_name']['id']
    surrogate = str(result_json['surrogate'])

    raw = "\t".join([name_string_id, verbatim, canonical_name_uuid, canonical_name, surrogate])
    json_pretty = json.dumps(result_json, indent=4, separators=(',', ': '))

    return raw + "\n" + json_pretty + "\n\n"


def main():
    cleanup()

    sc = SparkContext()
    sql_context = SQLContext(sc)
    path = os.path.join(mysql_export_dir, "name_strings.tsv")
    df = sql_context.load(source='com.databricks.spark.csv', header='true', inferSchema='true', path=path,
                          quote="\u0000", delimiter="\t")

    names = df.rdd.map(lambda x: x['name'])

    name_strings = parse_spark(sc, names) \
        .map(lambda result: json.loads(result)) \
        .filter(lambda js: find_filius_forma(js)) \
        .map(extract_name_string) \
        .distinct()

    name_strings.saveAsTextFile(filius_forma_output)


if __name__ == '__main__':
    main()
