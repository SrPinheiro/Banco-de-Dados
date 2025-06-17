from script.bucketService import BucketService
import io
import pandas as pd
import uuid

# Segundo bucket:
bucket_s = BucketService(bucket_name=" bucket-para-enem")

def etapa1():
    bucket_s.upload_file('dados/microdados_enem.csv', 'bronze/csv/microdados_enem.csv')

def etapa2():
    bucket_s.csv_to_parquet('bronze/csv/microdados_enem.csv', output_path='bronze/parquet/', separador=';')

def etapa3():
    arquivos_b = list(bucket_s.listar_arquivos('bronze/parquet/'))

    gender = {}
    grades = {}
    races = {}
    locales = {}
    school_types = {}

    def getDataId(value, dicionario):
        dataID = dicionario.get(value, None)
        if dataID == None:
            dataID = len(dicionario) + 1
            dicionario[value] = dataID
        
        return dataID

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
        
        students = parquet[['NU_INSCRICAO', 'TP_FAIXA_ETARIA', 'TP_SEXO', 'TP_NACIONALIDADE', 'SG_UF_ESC', 'TP_COR_RACA', 'TP_LOCALIZACAO_ESCOLA', 'TP_ESCOLA']].copy()
        



def etapa4():
    id_dataset =  'desempenho_alunos'
    bucket_s.to_bigQuery(id_dataset)
    

if __name__ == "__main__":
    etapa3()
    etapa4()
