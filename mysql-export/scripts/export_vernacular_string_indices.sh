#!/bin/bash

mysql -B -h $MYSQL_HOST -u$MYSQL_USER --password=$MYSQL_PASS $MYSQL_DB \
    -e "SELECT
          data_source_id \
        , vernacular_string_id \
        , REPLACE(REPLACE(REPLACE(vs.name, '\r\n', ' '), '\n', ' '), '\t', ' ') as name \
        , REPLACE(REPLACE(REPLACE(taxon_id, '\r\n', ' '), '\n', ' '), '\t', ' ') as taxon_id \
        , REPLACE(REPLACE(REPLACE(language, '\r\n', ' '), '\n', ' '), '\t', ' ') as language \
        , REPLACE(REPLACE(REPLACE(locality, '\r\n', ' '), '\n', ' '), '\t', ' ') as locality \
        , REPLACE(REPLACE(REPLACE(country_code, '\r\n', ' '), '\n', ' '), '\t', ' ') as country_code \
        FROM vernacular_string_indices AS vsi \
        JOIN vernacular_strings AS vs ON vsi.vernacular_string_id = vs.id" 1> data/mysql-export/vernacular_string_indices.tsv
