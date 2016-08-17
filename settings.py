import uuid

csvs_dir = "csvs"

mysql_export_dir = "mysql-export"

namespace = uuid.UUID('90181196-fecf-5082-a4c1-411d4f314cda')
empty_uuid = uuid.uuid5(namespace, "")


def parse_spark(sc, names):
    from pyspark.mllib.common import _py2java, _java2py
    parser = sc._jvm.org.globalnames.parser.spark.Parser()
    result = parser.parse(_py2java(sc, names), False, True)
    return _java2py(sc, result)
