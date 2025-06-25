from google.cloud import storage, bigquery
import pandas as pd
import io
import os
import pyarrow.parquet as pq
from google.cloud import bigquery, storage
from google.oauth2 import service_account

class BucketService:
    def __init__(self, bucket_name = 'bucket-para-enem'):
        self.bucket_name = bucket_name
        self.client = storage.Client.from_service_account_json('chaves/chaveDeServico.json')
        self.bucket = self.client.bucket(bucket_name)

    def upload_file(self, file_path, upload_path):
        print(f"Enviando {file_path} para o bucket: {self.bucket_name}")
        
        blob_file = self.bucket.blob(upload_path)
        blob_file.upload_from_filename(file_path)
        
        print(f"Arquivo {file_path} enviado com sucesso para {upload_path} no bucket {self.bucket_name}")
    
    def upload_file_from_bytes(self, file_name, file_bytes):
        print(f"Enviando arquivo {file_name} para o bucket: {self.bucket_name}")
        
        blob_file = self.bucket.blob(file_name)
        blob_file.upload_from_string(file_bytes, content_type='application/octet-stream')
        
        print(f"Arquivo {file_name} enviado com sucesso para o bucket {self.bucket_name}")
    
    def get_file(self, file_path):
        print(f"Obtendo o arquivo {file_path} do bucket: {self.bucket_name}")
        
        return self.bucket.blob(file_path)

    def download_file(self, file_name):
        print(f"Fazendo Download do {file_name} do bucket: {self.bucket_name}")
        
        blob_file = self.bucket.blob(file_name)
        blob_file.download_to_filename(f"dados/{file_name.split('/')[-1]}")
        
        print(f"Arquivo {file_name} baixado com sucesso do bucket {self.bucket_name}")

    def listar_arquivos(self, rota=''):
        print(f"Arquivos em: {self.bucket_name}/{rota}")
        return self.client.list_blobs(self.bucket_name, prefix=rota)

    def csv_to_parquet(self, file_path, output_path=None, chunk_size=50000, separador=';'):
        print(f"Convertendo {file_path} de CSV para Parquet")
        
        if output_path is None:
            output_path = '/'.join(file_path.split('/')[0:-1]) + '/parquet/'
        
        if not output_path.endswith('/'):
            output_path += '/'
        
        blob = self.bucket.blob(file_path)
        
        chunk_number = 0
        with blob.open("r", encoding='latin1') as csv_file:
            arquivo_df = pd.read_csv(csv_file, sep=separador, encoding='latin1', chunksize=chunk_size)
            for chunk_df in arquivo_df:
                chunk_number += 1
                parquet_buffer = io.BytesIO()
                chunk_df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                
                base_name = os.path.basename(file_path).replace('.csv', '')
                output_blob_name = f"{output_path}{base_name}_{chunk_number}.parquet"
                
                output_blob = self.bucket.blob(output_blob_name)
                output_blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
                
                print(f"Chunk {chunk_number} salvo como {output_blob_name}")
        
        print(f"Conversão concluída. {chunk_number} chunks foram criados em {output_path}")

    def to_bigQuery(self, dataset_id, path= 'silver/'):
        print(f"Subindo dados para o BigQuery no dataset: {dataset_id}")
        id_projeto = 'celestino-leo'
        credentials = service_account.Credentials.from_service_account_file('chaves/chaveDeServico.json')
        bigquery_client = bigquery.Client(project=id_projeto, credentials=credentials)
        
        client = bigquery.Client.from_service_account_json('chaves/chaveDeServico.json')
        
        blobs = list(self.listar_arquivos(path))
        blobs_por_diretorio = {}
        
        for blob in blobs:
            if not blob.name.endswith('.parquet'):
                continue
            
            folder = blob.name.split('/')[1]
            if blobs_por_diretorio.get(folder) is None:
                blobs_por_diretorio[folder] = []
            blobs_por_diretorio[folder].append(f"gs://{self.bucket_name}/{blob.name}")
            
        for folder, blobs in blobs_por_diretorio.items():
            print(f"Processando blobs na pasta: {folder}")
            
            dataset_ref = client.dataset(dataset_id).table(folder)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            load_job = bigquery_client.load_table_from_uri(
                blobs, dataset_ref, job_config=job_config
            )
            
            load_job.result()
            print(f"Pasta {folder} carregada com sucesso no BigQuery.")
