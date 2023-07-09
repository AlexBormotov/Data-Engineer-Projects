# Cloud Technologies Project

### Discription

Fintech sturtup with application that provides the possibility of international bank transfers.
They need the ability to analyze user transactions, and for this they need to create a data warehouse and build a dashboard.

### Realization

0. At this stage, we already have a staging layer (STG), created erlier, with raw data
1. Creating a detailed data layer (DDS) in postgres
2. Using Kafka, configure the filling of the DDS layer with clear data from STG
3. Release the service in Kubernetes
4. Repeat steps 1-3 for the third service that fills the created CDM layer (data layer for the dashboard)
5. Build a dashboard

### Repository structure

Directory `img` have images with result

Directory `solution` have folders:
- `/solution/service_dds` - Here we have files and folders with app that create and fill DDS-layer
  - `/solution/service_dds/app` - Files for Kubernetes
  - `/solution/service_dds/src` - Main service files with data-loader and libraries
- `/solution/service_cdm` - Here we have files and folders with app that create and fill CDM-layer with the same structure as DDS

Directory `stg` have folders with source STG-layer
