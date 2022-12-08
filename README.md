# Big-Data-Movie-Recommendation

This project is designed for providing movie recommendations based on both public ratings (IMDb) and critics ratings (Rotten Tomatoes). My idea is, once a user finds that they like one movie, they may want to keep watching other movies within the same genre. By searching the movie title, they could first get more detailed information about the movie they like (and submit their own movie ratings to update the public ratings of this movie). Based on the genre of the input movie, they could also receive recommendations about what to watch next from both public perspective and critics perspective.

## How to Use
I have depolyed my web application to loadbalancer webservers. Please follow this link to check my web app.
http://tangn-lb-1756778830.us-east-1.elb.amazonaws.com/

## Data 

Five datasets are used in this project. 
1. 
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
当user input一个电影名称的时候，output会是三张表
第一张是单独放这个input电影的imdb信息（包括评分），但是不涉及ranking，因为有ranking的会不全，所以这个只需要展示电影信息就可以了
第二张是IMDb的评分推荐
第三张是rt的评分推荐

# Speed layer：
1. speed layer会只更新rt这张表，everytime new a input, 自动scraping最新的critic review， 但是评分没法变，it doesn't make sense
2. speed layer更新第一张表里的评分

Step1: Put raw data into message queue
Step2: Speed layer reads from Kafka and updates speed view
