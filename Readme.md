Considering the following:



Question: How you might mature the data acquisition?

streaming



Question: Additional data preparation steps and data quality checks you may want to perform

The date/timestame data points should be validated, we may want to reject 
records with bad or impossible dates (future dates, dates out of time window, ect).

Question: How you might approach triaging or otherwise remediating data with unresolved errors

records rejected for errors or dirty data could be logged, they could also be loaded to a rejected table
in the database, for later review and remediation.

<b>Question: How you might better stage the pipeline using data warehousing strategies</b>

Need some kind of service for storing the database credentials
Streaming, might be a better aproch for this source?

Metadata, we may want more metadata on the batch, its source (probably other sources), 
may want to auditing tables or logs for load, ect.

<b>Question: Performance considerations</b>
Bulk load into postgres may be appropriate for this source
also Streaming might be a better aproch for this source

<b>Question: How you might mature logging, alerting and notifications</b>

logging to file: more detailed logging, maybe email or slack ect notifications for errors
or other events.
Dashboard:  might be appropriate for this source, to monitor load, processing time, errors, ect.