drop table if exists tangn_title_tsv;
create external table tangn_title_tsv(
    titleid STRING, 
    titletype STRING, 
    primarytitle STRING, 
    originaltitle STRING, 
    isadult BOOLEAN, 
    startyear SMALLINT, 
    endyear STRING, 
    runtimeminutes BIGINT, 
    genres STRING)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES(
   "separatorChar" = "\t",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/tangn/movie/data/title'
TBLPROPERTIES("skip.header.line.count"="1");


create table tangn_title(
    titleid STRING, 
    titletype STRING, 
    primarytitle STRING, 
    originaltitle STRING, 
    isadult BOOLEAN, 
    startyear SMALLINT, 
    endyear STRING, 
    runtimeminutes BIGINT, 
    genres STRING)
    stored as orc;


insert overwrite table tangn_title
select *
from tangn_title_tsv
where originaltitle is not null and primarytitle is not null 
and startyear is not null and genres is not null;
