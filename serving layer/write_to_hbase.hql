create 'tangn_movie', 'movie'

drop table if exists tangn_movie;

create external table tangn_movie(
  primary_title string,
  year smallint,
  genre string,
  average_rating float,
  votes bigint,
  director string,
  writer string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,movie:year,movie:genre,movie:average_rating,movie:votes,movie:director,movie:writer')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie');

insert overwrite table tangn_movie
select primary_title,
  year, genre, avg_rating, num_votes,
  director_name, writer_name
from tangn_movies_info;


create 'tangn_movie_recom', 'recom'

drop table if exists tangn_movie_recom;

create external table tangn_movie_recom(
  primary_title string,
  year smallint,
  genre string,
  average_rating float,
  votes bigint,
  director string,
  writer string,
  rank bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,recom:year,recom:genre,recom:average_rating,recom:votes,recom:director,recom:writer,recom:rank')
TBLPROPERTIES ('hbase.table.name' = 'tangn_movie_recom');

insert overwrite table tangn_movie_recom
select primary_title,
  year, genre, avg_rating, num_votes,
  director_name, writer_name, genre_rank
from tangn_movies_with_rank_info;
