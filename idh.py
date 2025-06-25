from script.bucketService import BucketService
import io
import pandas as pd
import sys

bucket_s = BucketService(bucket_name="idh_ibge")

def etapa1():
    bucket_s.upload_file('dados/data.csv', 'bronze/csv/idh.csv')

def etapa2():
    bucket_s.csv_to_parquet('bronze/csv/idh.csv', output_path='bronze/parquet/', separador=',', encoding='utf-8')

def etapa3():
    arquivos_b = list(bucket_s.listar_arquivos('bronze/parquet/'))

    def salvarParquet(df, path):
        output_blob_name = f"silver/{path}"
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        

        output_blob = bucket_s.bucket.blob(output_blob_name)
        output_blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
        print(f"{output_blob_name} salvo no bucket.")

    for i in range(0, len(arquivos_b)):
        blob = arquivos_b[i]
        blob_bytes = blob.download_as_bytes()
        parquet = pd.read_parquet(io.BytesIO(blob_bytes))
        
        salvarParquet(parquet, f"idh/idh_{i + 1}.parquet")

def etapa4():
    id_dataset = 'idh_ibge'
    bucket_s.to_bigQuery(id_dataset)
    

if __name__ == "__main__":
    etapa1()
    etapa2()
    etapa3()
    etapa4()
