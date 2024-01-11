# https://cloud.google.com/blog/products/data-analytics/bigquery-gains-change-data-capture-functionality
 
##################################################################################
# To update your protocol buffer definition with your sample_data.proto file run:
#
#   protoc --python_out=. sample_data.proto
#
##################################################################################
 
def write_json_to_bq(listColumns,x_data_pb2,json_data_file,project_name,dataset_name,table_name):
    
    from google.cloud import bigquery_storage_v1
    from google.cloud.bigquery_storage_v1 import types
    from google.cloud.bigquery_storage_v1 import writer
    from google.protobuf import descriptor_pb2
    import logging
    import json


    # The list of fields (i.e. the table's schema) to search in the given data to write to BigQuery.
    FIELDS_TO_CHECK = listColumns

    # Function to create a batch of row data to be serialized.
    def create_row_data(data):
        row = x_data_pb2
        for field in FIELDS_TO_CHECK:
          # This IF statement is particularly useful when optional fields aren't provided and thus are passed
          # as null values to BigQuery.
          if field in data:
            setattr(row, field, data[field])
        return row.SerializeToString()


    class BigQueryStorageWriteAppend(object):

        # Use the Storage Write API default stream to stream data into BigQuery.
        # This mode uses at-least once delivery
        # The stream name is: projects/{project}/datasets/{dataset}/tables/{table}/_default
        def append_rows_proto2(
            project_id: str, dataset_id: str, table_id: str, data: dict
        ):

            write_client = bigquery_storage_v1.BigQueryWriteClient()
            parent = write_client.table_path(project_id, dataset_id, table_id)
            stream_name = f'{parent}/_default'
            write_stream = types.WriteStream()

            # Create a template with fields needed for the first request.
            request_template = types.AppendRowsRequest()

            # The request must contain the stream name.
            request_template.write_stream = stream_name

            # So that BigQuery knows how to parse the serialized_rows, generate a
            # protocol buffer representation of your message descriptor.
            proto_schema = types.ProtoSchema()
            proto_descriptor = descriptor_pb2.DescriptorProto()
            x_data_pb2.DESCRIPTOR.CopyToProto(proto_descriptor)
            proto_schema.proto_descriptor = proto_descriptor
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.writer_schema = proto_schema
            request_template.proto_rows = proto_data

            # Some stream types support an unbounded number of requests. Construct an
            # AppendRowsStream to send an arbitrary number of requests to a stream.
            append_rows_stream = writer.AppendRowsStream(write_client, request_template)

            # Calls the create_row_data function to append proto2 serialized bytes to the
            # serialized_rows repeated field.
            proto_rows = types.ProtoRows()
            for row in data:
                proto_rows.serialized_rows.append(create_row_data(row))

            # Appends data to the given stream.
            request = types.AppendRowsRequest()
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.rows = proto_rows
            request.proto_rows = proto_data

            append_rows_stream.send(request)

            print(f"Rows to table: '{parent}' have been written.")
 
    with open(json_data_file,'r') as json_file:
        data = json.load(json_file)
    BigQueryStorageWriteAppend.append_rows_proto2(project_name,dataset_name,table_name,data=data) 
    # Change this to your specific BigQuery project, dataset, table details
    return True
 
