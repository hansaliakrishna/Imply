import os
import requests
import json
import argparse
import ast

def create_api_url(org_name, region, cloud_provider, project_id, endpoint):
    """Constructs the API URL based on input parameters."""

    return f"https://{org_name}.{region}.{cloud_provider}.api.imply.io/v1/projects/{project_id}/{endpoint}"

def build_payload(source_type, connection_name, object_name, file_format, input_schema, mappings, is_uri_or_is_file,table_name):
    """Builds the payload for the API request."""

    return {
        "type": "batch",
        "target": {
            "type": "table",
            "tableName": f'{table_name}'
        },
        "createTableIfNotExists": True,
        "source": {
            "type": source_type,
            "connectionName": connection_name,
            is_uri_or_is_file: [object_name],
            "formatSettings": {
                "format": file_format
            },
            "inputSchema": input_schema
        },
        "mappings": mappings
    }

def get_input_schema(org_name, region, cloud_provider, project_id, connection_name, object_name, apikey, timestamp_field_name,source_type):
    """Fetches the input schema from the Data Sampling API."""

    url = create_api_url(org_name, region, cloud_provider, project_id, "sampling/raw")
    print("\nData Sampling API URL:", url)

    object_name = object_name.split("/")[-1]
    
    payload = json.dumps({"source": {"type": source_type, "connectionName": connection_name, "objects": [object_name]}})
    headers = {'Authorization': f'Basic {apikey}', 'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()  # Raise an error for bad responses

        input_schema = response.json()['schema']
        print("\nInput schema discovered successfully!")

        mappings = create_column_mappings(input_schema, timestamp_field_name)
        print("\nColumn mappings created successfully!")

        return input_schema, mappings

    except requests.RequestException as e:
        print("\nError while discovering the Input Schema:", e)
        return None, None

def create_column_mappings(input_schema, timestamp_field_name):
    """Creates column mappings based on the input schema."""

    mappings = []
    for item in input_schema:
        column_name = item['name'] if item['name'] != timestamp_field_name else '__time'
        expression = f'MILLIS_TO_TIMESTAMP("{timestamp_field_name}")' if item['name'] == timestamp_field_name else f'"{item["name"]}"'
        mappings.append({"columnName": column_name, "expression": expression})
    return mappings

def ingest_data(org_name, region, cloud_provider, project_id, connection_name, object_name, file_format, table_name, apikey, input_schema, mappings, is_uri_or_is_file, source_type):
    """Ingests data using the specified parameters."""

    print("\nStart Ingesting Data!")
    url = create_api_url(org_name, region, cloud_provider, project_id, "jobs")
    print("\nBatch Ingestion API URL:", url)

    payload = build_payload(source_type, connection_name, object_name, file_format, input_schema, mappings, is_uri_or_is_file,table_name)
    headers = {'Authorization': f'Basic {apikey}', 'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # Raise an error for bad responses
        print("\nResponse of Ingestion API:", response.json())
    except requests.RequestException as e:
        print("\nError during data ingestion:", e)

def parse_arguments():
    """Parses command-line arguments."""

    parser = argparse.ArgumentParser(description="Auto Discover the Schema from input file placed on Azure")
    parser.add_argument('-org', '--org_name', required=True, help='Organization Name for the API call')
    parser.add_argument('-reg', '--region', required=True, help='Region for the API call')
    parser.add_argument('-cloud', '--cloud_provider', required=True, help='Cloud Provider for the API call')
    parser.add_argument('-project', '--project_id', required=True, help='Polaris Project ID')
    parser.add_argument('-source_type', '--source_type', required=True, help='Provide the source type if it is s3 or azure')
    parser.add_argument('-con', '--connection_name', required=True, help='Azure Connection Name')
    parser.add_argument('-isURI_or_isObject', '--isURI_or_isObject', required=True, help="'uris' if user is passing URIs or 'objects' if user passes file lists")
    parser.add_argument('-objects', '--object_list', required=True, help='Azure file object name')
    parser.add_argument('-fileformat', '--file_format', required=True, help='File format of the data')
    parser.add_argument('-table', '--table_name', required=True, help='Target DataSource Name in Polaris')
    parser.add_argument('-timestampfield', '--timestamp_field_name', required=True, help='Provide the TimeStamp field Name')

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    apikey = os.getenv("POLARIS_API_KEY")

    object_list = ast.literal_eval(args.object_list)
    for object_name in object_list:
        print("\nObject:", object_name)
        input_schema, mappings = get_input_schema(args.org_name, args.region, args.cloud_provider, args.project_id, args.connection_name, object_name, apikey, args.timestamp_field_name, args.source_type)
        #python3 schema_auto_discovery_and_data_ingestion_v2.py -org 'imply-sa' -reg 'us-east-1' -cloud 'aws' -project 'bd4a31ca-06c2-45bc-a7de-bac8a5320483' -azure_con 'kh-azure-connection' -isURI_or_isObject 'uris' -objects "['azureStorage://honeywell1/test/test1/metric-data-sample.json']" -fileformat 'nd-json' -table 'metric-data' -timestampfield 'timeStamp'
        #python3 schema_auto_discovery_and_data_ingestion_v2.py -org 'imply-sa' -reg 'us-east-1' -cloud 'aws' -project 'bd4a31ca-06c2-45bc-a7de-bac8a5320483' -azure_con 'kh-azure-connection' -isURI_or_isObject 'objects' -objects "['metric-data-sample_partial1.json','metric-data-sample_partial2.json']" -fileformat 'nd-json' -table 'metric-data_partial_loading' -timestampfield 'timeStamp'
        #python3 schema_auto_discovery_and_data_ingestion_v2.py -org 'imply-sa' -reg 'us-east-1' -cloud 'aws' -project 'bd4a31ca-06c2-45bc-a7de-bac8a5320483' -azure_con 'kh-azure-connection' -isURI_or_isObject 'objects' -objects "['test1/metric-data-sample.json']" -fileformat 'nd-json' -table 'metric-data_partial_loading' -timestampfield 'timeStamp'
        
        if input_schema and mappings:  # Ensure we have a valid schema and mappings before ingesting data
            ingest_data(args.org_name, args.region, args.cloud_provider, args.project_id, args.connection_name, object_name, args.file_format, args.table_name, apikey, input_schema, mappings, args.isURI_or_isObject, args.source_type)
