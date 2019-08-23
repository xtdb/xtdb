#!/usr/bin/env bash

set -e

mkdir -p data
curl https://datasets.imdbws.com/name.basics.tsv.gz -o data/name.basics.tsv.gz
curl https://datasets.imdbws.com/title.akas.tsv.gz -o data/title.akas.tsv.gz
curl https://datasets.imdbws.com/title.crew.tsv.gz -o data/title.crew.tsv.gz
curl https://datasets.imdbws.com/title.episode.tsv.gz -o data/title.episode.tsv.gz
curl https://datasets.imdbws.com/title.principals.tsv.gz -o data/title.principals.tsv.gz
curl https://datasets.imdbws.com/title.ratings.tsv.gz -o data/title.ratings.tsv.gz

gunzip data/*.tsv.gz
