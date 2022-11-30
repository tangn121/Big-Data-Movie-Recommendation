drop table if exists tangn_rotten_csv;
create external table tangn_rotten_csv(
    startyear SMALLINT, 
    critic_rating SMALLINT, 
    critic_review STRING, 
    primarytitle STRING)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES(
   "separatorChar" = "\t",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location 's3://mpcs53014-tangn/'
TBLPROPERTIES("skip.header.line.count"="1");

create table tangn_rotten(
    startyear SMALLINT, 
    critic_rating SMALLINT, 
    critic_review STRING, 
    primarytitle STRING)
    stored as orc;
insert overwrite table tangn_rotten
select *
from tangn_rotten_csv
where critic_rating is not null;
