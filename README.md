# Big-Data-Movie-Recommendation
This project is designed for providing movie recommendations based on IMDb and Rotten Tomatoes ratings. 
# Data 
All three datasets are downloaded from https://datasets.imdbws.com/. See documentations for the data files in https://www.imdb.com/interfaces/.
The data is refreshed daily, which provides the opportunity for me to implement a speed layer.

1. title.basics.tsv.gz
2. title.crew.tsv.gz
3. title.ratings.tsv.gz

# Batch Layer
### Get data and save it to HDFS
#### datasource 1: IMDb
#### datasource 2: Rotten tomatoes
batch_layer/get_data.sh
### Move data into ORC files in Hive
那一堆hql文件

# Serving layer


大致思路：
数据源是IMDb和rotten tomatoes
因为IMDb是最知名的movie review网站，数据点最多，但是他的评分基本是audience评分，代表大众的口味
但是rt包含专业评分
所以我想把大众评分和专业评分结合，做一个电影推荐。
当user input一个电影名称的时候，output会是两张表
第一张是IMDb的评分推荐
第二张是rt的评分推荐

关于speed layer：
1. speed layer会只更新rt这张表，everytime new a input, 自动scraping最新的critic review， 但是评分没法变，it doesn't make sense
2. speed layer更新imdb里的评分
