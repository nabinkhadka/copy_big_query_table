from google.cloud import bigquery
import gc
import time

client = bigquery.Client.from_service_account_json('/path/to/key/project-id-123abcdef456.json')


DATASET = 'SAMPLE_DATASET'
SOURCE_TABLE = 'source_table'
DESTINATION_TABLE = 'destination_table'
dataset_ref = client.dataset(DATASET)
source_table_ref = dataset_ref.table(SOURCE_TABLE)
source_table = client.get_table(source_table_ref)
destination_table_ref = dataset_ref.table(DESTINATION_TABLE)
destination_table = client.get_table(destination_table_ref)

count = 0
rows = list()
for row in client.list_rows(source_table, max_results=None, start_index=0):
    current_row = dict(zip(row.keys(), row.values()))
    if current_row.get('price_adjusted') == '':
        current_row['price_adjusted'] = None
    rows.append(current_row)
    count += 1
    if count % 1000 == 0:
        response = client.insert_rows(destination_table, rows)
        time.sleep(0.1)
        if response:
            print(response)
        del rows
        gc.collect()
        rows = list()
        print('Completed:' + str(count))

# Add remaining rows
response = client.insert_rows(destination_table, rows)
print(response)
