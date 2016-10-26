#!/bin/bash

mysql -B -h $MYSQL_HOST -u$MYSQL_USER --password=$MYSQL_PASS $MYSQL_DB \
    -e "SELECT
          id \
        , REPLACE(REPLACE(REPLACE(name, '\r\n', ' '), '\n', ' '), '\t', ' ') as name \
        , uuid \
        FROM vernacular_strings" 1> ../vernacular_strings.tsv
