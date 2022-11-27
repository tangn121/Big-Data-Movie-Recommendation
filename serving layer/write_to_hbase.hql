create 'tangn_movie', 'movie'

drop table if exists tangn_movie;

create external table tangn_movie(
  id string,
  primary_title string,
  original_title string,
  year smallint,
  genre string,
  average_rating float,
  votes bigint,
  director string,
  writer string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,movie:primary_title,movie:original_title,movie:year,movie:genre,movie:average_rating,movie:votes,movie:director,movie:writer')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie');

insert overwrite table tangn_movie
select movie_id, primary_title, origin_title,
  year, genre, avg_rating, num_votes,
  director_name, writer_name
from tangn_movies_info;


create 'tangn_movie_recom', 'recom'

drop table if exists tangn_movie_recom;

create external table tangn_movie_recom(
  id string,
  primary_title string,
  original_title string,
  year smallint,
  genre string,
  average_rating float,
  votes bigint,
  director string,
  writer string,
  rank bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,recom:primary_title,recom:original_title,recom:year,recom:genre,recom:average_rating,recom:votes,recom:director,recom:writer,recom:rank')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie_recom');

insert overwrite table tangn_movie_recom
select movie_id, primary_title, origin_title,
  year, genre, avg_rating, num_votes,
  director_name, writer_name, genre_rank
from tangn_movies_with_rank_info;
