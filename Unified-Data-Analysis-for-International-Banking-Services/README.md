# Unified Data Analysis for International Banking Services. 
Final project of Yandex.Practicum Data Engineering course

### Discription

A fintech startup that provides international banking services wants to conduct a unified data analysis. Our task is to combine information from different sources and prepare it for analytics. As a result they need a dashboard with 4 key metrics

### Realization

1. Out of 3 sources (S3, PostgreSQL, Spark Streaming / Kafka), 1 was chosen as the most optimal for future development of data streams,
Postgres is the fastest solution yet easy to scale.
2. Created STAGING-layer with imported data as is, written DAG automating data collection
3. Thought out architecture of DDS layer and reporting layer (CDM)
4. Created DAGs that create tables in the DDS and CDM layers
5. Built a dashboard in Metabase from a table in CDM

### Repository structure

Directory `src` have folders:
- `/src/dags` - AirFlow DAGs, which provide data from source to storage and second DAG, which refresh data mart.
- `/src/sql` - SQL-queries for crating tables in `STAGING`- and `DWH`-layers, plus additional script for creating data mart.
- `/src/img` - exapmle of data and proof of streaming work

### Stek
`python` , `airflow`, `kafka`, `cloud technologies`, `postgresql`, `vertica`

### Project status: 

Done.
