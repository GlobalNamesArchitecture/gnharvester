GnHarvester
===========

Workflow to import data from MySQL to PostgreSQL:

1. export data from MySQL to TSV with ``mysql-export/scripts``
2. run Spark job to convert TSV to PostgreSQL format with:

.. code:: bash

    SPARK_HOME=/path/to/spark-dists/spark-src_1.6.2/ \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.9-src.zip \
    PYSPARK_PYTHON=/path/to/anaconda2/envs/snakes/bin/python \
        /path/to/spark-dists/spark-src_1.6.2/bin/spark-submit \
        --jars "/path/to/scala-2.10/gnparser-spark-python-assembly-0.3.3-SNAPSHOT.jar" \
        --driver-class-path="/path/to/scala-2.10/gnparser-spark-python-assembly-0.3.3-SNAPSHOT.jar" \
        --packages com.databricks:spark-csv_2.10:1.4.0 \
        --executor-memory 20G --driver-memory 20G \
        python_script.py

3. import data with script:

.. code:: bash

    time cat csvs/<<TABLE>>/part-* | psql development -h localhost -U postgres -c "COPY <<TABLE>> FROM STDIN"
