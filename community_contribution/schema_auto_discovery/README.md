
# Schema Auto Discovery 

Have Schema Auto Discovery before invoking Ingestion API in Polaris for Batch Ingestion.

## Invocation

```
python3 schema_auto_discovery_and_data_ingestion_v4.py -org 'imply-sa' -reg 'us-east-1' -cloud 'aws' -project 'bd4a31ca-06c2-45bc-a7de-bac8a5320483' -source_type 'azure' -con 'community_contribution_kh' -isURI_or_isObject 'uris' -objects "['azureStorage://schemaautodiscoverydemo/demo/world_country_and_usa_states_latitude_and_longitude_values_1.csv','azureStorage://schemaautodiscoverydemo/demo/world_country_and_usa_states_latitude_and_longitude_values_2.csv']" -fileformat 'csv' -table 'wc-azure' -timestampfield 'created_at'
```

Command Parameters:

```
-org : Polaris Organization Name
-reg : Region of Polaris Project
-cloud : Polaris project cloud deployment
-project : Polaris Project ID
-source_type : s3/azure (Keep it s3 or azure based on where your data files are placed)
-con : s3 or azure connection name that is created in Polaris
-isURI_or_isObject : value can be either 'uris' or 'objects' (please refer the invocation commands
-objects : list of the data sources
-fileformat : data file format (eg. 'csv' or 'nd-json')
-table : Data source name in Polaris
-timestampfield : timestamp field name in your source data that you want to map to __time in Imply data source
```
