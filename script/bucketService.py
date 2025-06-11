from google.cloud import storage
import pandas as pd
import io
import os
import pyarrow.parquet as pq

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

    def csv_to_parquet(self, file_path, output_path=None, chunk_size=50000):
        print(f"Convertendo {file_path} de CSV para Parquet")
        
        if output_path is None:
            output_path = '/'.join(file_path.split('/')[0:-1]) + '/parquet/'
        
        if not output_path.endswith('/'):
            output_path += '/'
        
        blob = self.bucket.blob(file_path)
        
        chunk_number = 0
        with blob.open("r", encoding='latin1') as csv_file:
            arquivo_df = pd.read_csv(csv_file, sep=';', encoding='latin1', chunksize=chunk_size)
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

    def bronze_to_silver(self):

        bronze_prefix = 'bronze/parquet/'
        silver_prefix = 'silver/'

        aluno_cols = ['NU_INSCRICAO', 'TP_FAIXA_ETARIA', 'TP_SEXO', 'TP_COR_RACA', 'TP_NACIONALIDADE']
        estado_cols = ['NU_INSCRICAO', 'SG_UF_ESC']
        nota_cols = ['NU_INSCRICAO', 'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']

        blobs = list(self.client.list_blobs(self.bucket_name, prefix=bronze_prefix))
        parquet_blobs = [blob for blob in blobs if blob.name.endswith('.parquet')]

        for blob in parquet_blobs:
            print(f"Processando {blob.name}")

            parquet_bytes = blob.download_as_bytes()
            df = pd.read_parquet(io.BytesIO(parquet_bytes))

            tables = [
                ('aluno', aluno_cols),
                ('estado', estado_cols),
                ('nota', nota_cols)
            ]

            for table_name, columns in tables:
                df_table = df[columns].copy()
                output_path = f"{silver_prefix}{table_name}/"
                os.makedirs('/tmp', exist_ok=True)
                local_file = f"/tmp/{os.path.basename(blob.name)}"
                df_table.to_parquet(local_file, index=False)
                output_blob_name = f"{output_path}{os.path.basename(blob.name)}"
                output_blob = self.bucket.blob(output_blob_name)
                with open(local_file, "rb") as f:
                    output_blob.upload_from_file(f, content_type='application/octet-stream')
                print(f"Arquivo {output_blob_name} criado na silver.")

        print("Conversão bronze para silver concluída.")
        
    def aprimorar_dados(self):
        print("Aprimoramento de dados iniciado.")
        aluno_cols = ['NU_INSCRICAO', 'TP_FAIXA_ETARIA', 'TP_SEXO', 'TP_COR_RACA', 'TP_NACIONALIDADE']
        estado_cols = ['NU_INSCRICAO', 'SG_UF_ESC']
        nota_cols = ['NU_INSCRICAO', 'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO']
        
        blobs = self.listar_arquivos('bronze/parquet/')
        for blob in blobs:
            dados = blob.download_as_bytes()
            
            df = pd.read_parquet(io.BytesIO(dados))
            df = df[df['IN_TREINEIRO'] == 0]

            tables = [
                ('aluno', aluno_cols),
                ('estado', estado_cols),
                ('nota', nota_cols)
            ]

            for table_name, columns in tables:
                df_table = df[columns].copy()
                output_path = f"silver/{table_name}/"
                os.makedirs('/tmp', exist_ok=True)
                local_file = f"/tmp/{os.path.basename(blob.name)}"
                df_table.to_parquet(local_file, index=False)
                output_blob_name = f"{output_path}{os.path.basename(blob.name)}"
                output_blob = self.bucket.blob(output_blob_name)
                with open(local_file, "rb") as f:
                    output_blob.upload_from_file(f, content_type='application/octet-stream')
                print(f"Arquivo {output_blob_name} criado na silver.")
