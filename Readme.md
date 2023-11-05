<b>Overview</b>

Simple example of a service to ingest data from: http://data.gdeltproject.org/gdeltv2/lastupdate.txt
and load into a postgres table with postgis point geometry,
to see run:
```
docker built -t tech_challenge .
docker run
```
NOTE: no database is provide, you will need to provide one and update the db credentials in the code, docker image postgis/postgis:https://registry.hub.docker.com/r/postgis/postgis/ can be used


<b>Considering the following:</b>


<b>Question: How you might mature the data acquisition?</b>

-it might be better to stream the txt data from the url, I'm not sure how big these files get, maybe at certain times there is more data coming in? 

-perhaps batch processing as an overall approach is not appropriate for this data source, sounds like the data is coming in as a stream.

-deployment: need a deployment for this, need to consider the cloud environment to be used.

-will need some testing


<b>Question: Additional data preparation steps and data quality checks you may want to perform</b>

-The date/timestamp data points should be validated, we may want to reject 
records with bad or impossible dates (future dates, dates out of time window, ect).

-probably want a whole module for checking the raw data


<b>Question: How you might approach triaging or otherwise remediating data with unresolved errors</b>

-records rejected for errors or dirty data could be logged, they could also be loaded to a rejected table in the database, for later review and remediation.
-might introduce logic to clean common issues. Maybe for example some multiple event codes could be normalized to a single event, don’t know enough about the data yet.


<b>Question: How you might better stage the pipeline using data warehousing strategies</b>

-Need some kind of service for storing the database credentials

-Need to model the data based on downstream use requirements, 
for example: we may want the source columns in a separate table. We may want a load timestamp, We may want a table for rejected records, Indexing, ect.

-Metadata, we may want more metadata on the batch, its source (we probably have other sources), may want to auditing tables or logs for load, ect.


<b>Question: Performance considerations</b>

-Bulk load into postgres may be appropriate for this source if we continue with batch processing

-Also Streaming might be a better approach for this source, probably need a better connection to the database for inserting data, connection pool, prepared statement, ect.  Might want to use an ORM.


<b>Question: How you might mature logging, alerting and notifications</b>

-logging to file or maybe a logging service

-more detailed logging

-maybe email or slack notifications for errors or other events.

-Dashboard:  might be appropriate for this source, to monitor load, processing time, errors, visualization, ect.
