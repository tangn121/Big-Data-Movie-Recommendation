create 'tangn_movie', 'movie'

drop table if exists tangn_movie;

create external table tangn_movie(
  title string,
  year smallint,
  genre string,
  ratings bigint,
  votes bigint,
  director string,
  writer string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,movie:year,movie:genre,movie:ratings#b,movie:votes#b,movie:director,movie:writer')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie');

insert overwrite table tangn_movie
select primary_title,
  year, genre, total_ratings, num_votes,
  director_name, writer_name
from tangn_movies_info;


create 'tangn_movie_recom', 'recom'

drop table if exists tangn_movie_recom;

create external table tangn_movie_recom(
  genre string,
  title string,
  year smallint,
  rating float,
  votes bigint,
  director string,
  writer string,
  rank bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,recom:title,recom:year,recom:rating,recom:votes#b,recom:director,recom:writer,recom:rank#b')
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
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,rotten:title,rotten:year,rotten:director,rotten:writer,rotten:rating#b,rotten:rank#b,rotten:review')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie_recom_rotten');

insert overwrite table tangn_movie_recom_rotten
select genre, primary_title,
  year, director_name, writer_name, critic_rating, rotten_rank, critic_review
from tangn_movies_rotten_rank_10;

// create a table for speed layer
create 'tangn_ratings', 'user'

drop table if exists tangn_ratings;

create external table tangn_ratings(
  title string,
  ratings bigint,
  votes bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,user:ratings#b,user:votes#b')
TBLPROPERTIES ('hbase.table.name' = 'tangn_ratings');

insert overwrite table tangn_ratings
select * from tangn_user_ratings;
