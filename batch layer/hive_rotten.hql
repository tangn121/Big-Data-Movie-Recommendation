drop table if exists tangn_rotten_csv;
create external table tangn_rotten_csv(
    primarytitle STRING,
    startyear SMALLINT, 
    critic_rating SMALLINT, 
    critic_review STRING)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES(
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/tangn/movie/data/rotten'
TBLPROPERTIES("skip.header.line.count"="1");

create table tangn_rotten(
    primarytitle STRING,
    startyear SMALLINT, 
    critic_rating SMALLINT, 
    critic_review STRING)
    stored as orc;
    
insert overwrite table tangn_rotten
select primarytitle, startyear, critic_rating, critic_review
from tangn_rotten_csv;
