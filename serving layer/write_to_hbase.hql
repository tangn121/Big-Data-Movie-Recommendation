create 'tangn_movie', 'movie'

drop table if exists tangn_movie;

create external table tangn_movie(
  title string,
  year smallint,
  genre string,
  rating smallint,
  votes bigint,
  director string,
  writer string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,movie:year,movie:genre,movie:rating,movie:votes,movie:director,movie:writer')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie');

insert overwrite table tangn_movie
select primary_title,
  year, genre, avg_rating, num_votes,
  director_name, writer_name
from tangn_movies_info;


create 'tangn_movie_recom', 'recom'

drop table if exists tangn_movie_recom;

create external table tangn_movie_recom(
  genre string,
  title string,
  year smallint,
  rating smallint,
  votes bigint,
  director string,
  writer string,
  rank bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,recom:title,recom:year,recom:rating,recom:votes,recom:director,recom:writer,recom:rank')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie_recom');

insert overwrite table tangn_movie_recom
select genre, primary_title,
  year, avg_rating, num_votes,
  director_name, writer_name, genre_rank
from tangn_movies_with_rank_10;

create 'tangn_movie_recom_rotten', 'rotten'

drop table if exists tangn_movie_recom_rotten;

create external table tangn_movie_recom_rotten(
  genre string,
  title string,
  year smallint,
  director string,
  writer string,
  rating smallint,
  rank bigint,
  review string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,rotten:title,rotten:year,rotten:director,rotten:writer,rotten:rating,rotten:rank,rotten:review')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie_recom_rotten');

insert overwrite table tangn_movie_recom_rotten
select genre, primary_title,
  year, director_name, writer_name, critic_rating, rotten_rank, critic_review
from tangn_movies_rotten_rank_10;

// create a table for speed layer
create 'tangn_public_ratings', 'user'

drop table if exists tangn_public_ratings;

create external table tangn_public_ratings(
  title string,
  rating smallint,
  votes bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,user:rating#b,user:votes#b')
TBLPROPERTIES ('hbase.table.name' = 'tangn_public_ratings');

insert overwrite table tangn_public_ratings
select primary_title, avg_rating, num_votes
from user_rating;
