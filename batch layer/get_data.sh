#!/bin/bash

curl https://datasets.imdbws.com/title.basics.tsv.gz | hdfs dfs -put - /tmp/tangn/movie/data/title.tsv.gz

curl https://datasets.imdbws.com/title.crew.tsv.gz | hdfs dfs -put - /tmp/tangn/movie/data/crew.tsv.gz

curl https://datasets.imdbws.com/title.ratings.tsv.gz | hdfs dfs -put - /tmp/tangn/movie/data/rating.tsv.gz
