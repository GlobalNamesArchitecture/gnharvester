#!/bin/bash

mysql -B -h $MYSQL_HOST -u$MYSQL_USER --password=$MYSQL_PASS $MYSQL_DB \
    -e "SELECT
          id \
        , REPLACE(REPLACE(REPLACE(name, '\r\n', ' '), '\n', ' '), '\t', ' ') as name \
        , REPLACE(REPLACE(REPLACE(normalized, '\r\n', ' '), '\n', ' '), '\t', ' ') as normalized \
        , canonical_form_id \
        , has_words \
        , uuid \
        , has_groups \
        , surrogate \
        FROM name_strings" 1> data/mysql-export/name_strings.tsv
