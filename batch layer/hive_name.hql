# map the data in Hive
drop table if exists tangn_name_tsv;
create external table tangn_name_tsv(
    nameid STRING,
    primaryname STRING,
    birthyear SMALLINT,
    deathyear STRING,
    primaryprofession STRING,
    knownfortitles STRING)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES(
   "separatorChar" = "\t",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/tangn/movie/data/name'
TBLPROPERTIES("skip.header.line.count"="1");

# create an ORC table

create table tangn_name(
    nameid STRING,
    primaryname STRING,
    birthyear SMALLINT,
    deathyear STRING,
    primaryprofession STRING,
    knownfortitles STRING)
    stored as orc;

# copy the csv table to the orc table
insert overwrite table tangn_name
select *
from tangn_name_tsv
where nameid is not null and primaryname is not null;