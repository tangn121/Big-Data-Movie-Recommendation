drop table if exists tangn_rating_tsv;
create external table tangn_rating_tsv(
    titleid STRING,
    averagerating FLOAT,
    numvotes BIGINT)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES(
   "separatorChar" = "\t",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/tangn/movie/data/rating'
TBLPROPERTIES("skip.header.line.count"="1");


create table tangn_rating(
    titleid STRING,
    averagerating FLOAT,
    numvotes BIGINT)
    stored as orc;


insert overwrite table tangn_rating
select *
from tangn_rating_tsv
where titleid is not null and averagerating is not null and numvotes is not null;
