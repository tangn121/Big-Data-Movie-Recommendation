#!/bin/bash

curl https://datasets.imdbws.com/title.basics.tsv.gz | hdfs dfs -put - /tmp/tangn/movie/data/title.tsv.gz

curl https://datasets.imdbws.com/title.crew.tsv.gz | hdfs dfs -put - /tmp/tangn/movie/data/crew.tsv.gz

curl https://datasets.imdbws.com/title.ratings.tsv.gz | hdfs dfs -put - /tmp/tangn/movie/data/rating.tsv.gz

curl https://datasets.imdbws.com/name.basics.tsv.gz | hdfs dfs -put - /tmp/tangn/movie/data/name.tsv.gz

#unzip file
hdfs dfs -cat /tmp/tangn/movie/data/title.tsv.gz | gunzip | hdfs dfs -put - /tmp/tangn/movie/data/title/title.tsv

hdfs dfs -cat /tmp/tangn/movie/data/crew.tsv.gz | gunzip | hdfs dfs -put - /tmp/tangn/movie/data/crew/crew.tsv

hdfs dfs -cat /tmp/tangn/movie/data/rating.tsv.gz | gunzip | hdfs dfs -put - /tmp/tangn/movie/data/rating/rating.tsv

hdfs dfs -cat /tmp/tangn/movie/data/name.tsv.gz | gunzip | hdfs dfs -put - /tmp/tangn/movie/data/name/name.tsv


hdfs dfs -mkdir /tmp/tangn/movie/data/rotten

hdfs dfs –copyFromLocal /Users/ning/Desktop/Uchicago/Autumn2023/BigData/final_project/rotten_tomato_data.csv /tmp/tangn/movie/data/rotten
