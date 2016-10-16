#!/bin/bash

mysql -B -h $MYSQL_HOST -u$MYSQL_USER --password=$MYSQL_PASS $MYSQL_DB \
    -e "SELECT \
          nsi.data_source_id \
        , ns.id \
        , REPLACE(REPLACE(REPLACE(name, '\r\n', ' '), '\n', ' '), '\t', ' ') AS name \
        , REPLACE(REPLACE(REPLACE(url, '\r\n', ' '), '\n', ' '), '\t', ' ') AS url \
        , REPLACE(REPLACE(REPLACE(taxon_id, '\r\n', ' '), '\n', ' '), '\t', ' ') AS taxon_id \
        , REPLACE(REPLACE(REPLACE(global_id, '\r\n', ' '), '\n', ' '), '\t', ' ') AS global_id \
        , REPLACE(REPLACE(REPLACE(rank, '\r\n', ' '), '\n', ' '), '\t', ' ') AS rank \
        , REPLACE(REPLACE(REPLACE(accepted_taxon_id, '\r\n', ' '), '\n', ' '), '\t', ' ') AS accepted_taxon_id \
        , REPLACE(REPLACE(REPLACE(classification_path, '\r\n', ' '), '\n', ' '), '\t', ' ') AS classification_path \
        , REPLACE(REPLACE(REPLACE(classification_path_ids, '\r\n', ' '), '\n', ' '), '\t', ' ') AS classification_path_ids \
        , REPLACE(REPLACE(REPLACE(nomenclatural_code_id, '\r\n', ' '), '\n', ' '), '\t', ' ') AS nomenclatural_code_id \
        , nomenclatural_code_id \
        , REPLACE(REPLACE(REPLACE(local_id, '\r\n', ' '), '\n', ' '), '\t', ' ') AS local_id \
        , REPLACE(REPLACE(REPLACE(classification_path_ranks, '\r\n', ' '), '\n', ' '), '\t', ' ') AS classification_path_ranks \
      FROM name_string_indices AS nsi \
      JOIN name_strings AS ns ON nsi.name_string_id = ns.id" 1> ../name_string_indices.tsv
