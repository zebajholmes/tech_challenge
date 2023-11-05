-- this table will hold the ingested records from the GDELT data source
-- I am assuming that globaleventid will be unique and can be our primary key
-- did not add any other index but made some guesses on posibliities
create table if not exists ingested_records  (
    GLOBALEVENTID varchar(100) primary key,  -- guessing that we would want this indexed
    src_SQLDATE varchar(100),
    --sql_date date,  --might want to add this
    src_EventCode varchar(5), -- guessing that we would want this (and maybe other event codes) indexed
    event varchar(1000),
    src_EventBaseCode varchar(5),
    event_base varchar(1000),
    src_EventRootCode varchar(5),
    event_root varchar(1000),
    src_ActionGeo_FullName varchar(500),
    src_ActionGeo_CountryCode varchar(10),
    county_fips varchar(5),
    county_name varchar(500),
    src_ActionGeo_Lat varchar(100),
    src_ActionGeo_Long varchar(100),
    src_DATEADDED varchar(100),
    --date_added timestamp, --might want to add this
    src_SOURCEURL text,
    geom geometry(Point,4269) -- maybe spatial index on this for downstream use?
);
---create index on public.ingested_records ....(create some indexes)
