from script.bucketService import BucketService
import io
import pandas as pd
import uuid

bucket_s = BucketService(bucket_name="bucket-para-enem")

def etapa1():
    bucket_s.upload_file('dados/microdados_enem.csv', 'bronze/csv/microdados_enem.csv')

def etapa2():
    bucket_s.csv_to_parquet('bronze/csv/microdados_enem.csv', output_path='bronze/parquet/', separador=';')

def etapa3():
    arquivos_b = list(bucket_s.listar_arquivos('bronze/parquet/'))

    states = {}

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
        
        colStudents = ['NU_INSCRICAO', 'TP_FAIXA_ETARIA', 'TP_SEXO', 'TP_NACIONALIDADE', 'SG_UF_ESC']
        colStates = ['SG_UF_ESC']
        colScore = ['NU_INSCRICAO', 'NU_NOTA_CN',"NU_NOTA_CH","NU_NOTA_LC","NU_NOTA_MT","NU_NOTA_REDACAO" ]
        
        parquet.dropna(subset=[*colStudents, *colStates, *colScore], inplace=True)
        
        students = parquet[colStudents].copy()
        students['STATE_ID'] = parquet['SG_UF_ESC'].apply(lambda x: getDataId(x, states))
        score = parquet[colScore].copy()
        
        salvarParquet(students, f"students/students{i + 1}.parquet")
        salvarParquet(score, f"score/score{i + 1}.parquet")
        
        
        
    dfStates = pd.DataFrame(list(states.items()), columns=['Name', 'Id'])
    salvarParquet(dfStates, f"states/states.parquet")
    
def etapa4():
    id_dataset =  'enem_2023_gold'
    bucket_s.to_bigQuery(id_dataset)
    

if __name__ == "__main__":
    # etapa3()
    etapa4()
