# Big-Data-Movie-Recommendation
This project is designed for providing movie recommendations based on IMDb ratings.
# Data 
All three datasets are downloaded from https://datasets.imdbws.com/. See documentations for the data files in https://www.imdb.com/interfaces/.
The data is refreshed daily, which provides the opportunity for me to implement a speed layer.

1. title.basics.tsv.gz
2. title.crew.tsv.gz
3. title.ratings.tsv.gz

# Batch Layer
### Get data and save it to HDFS
batch_layer/get_data.sh
