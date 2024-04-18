import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import bigquery, storage
import os

project_id = os.environ.get("PROJECT_ID")
table_path = os.environ.get("TABLE_PATH")
bucket_name = os.environ.get("BUCKET_NAME")

"""Extrai dados de arquivos .XLSX no storage e escreve no BQ"""
def read_xlsx(event, context):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix='entrada/'):
        if blob.name.endswith('.xlsx'):
            with blob.open("rb") as f:
                df = pd.read_excel(f)
                # Obtém o blob de origem
                blob = bucket.blob(blob.name)
                # Copia o blob para o destino
                name = blob.name.split('/')[-1]
                new_blob_name = f'backup/{name}'
                new_blob = bucket.copy_blob(
                    blob, bucket, new_blob_name
                )
                # Exclui o blob original
                blob.delete()
                pd_gbq.to_gbq(df, table_path, project_id = project_id, if_exists = 'append')
    return None