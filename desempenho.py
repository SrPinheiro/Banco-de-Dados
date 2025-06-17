from script.bucketService import BucketService
import io
import pandas as pd
import uuid

# Segundo bucket:
bucket_s = BucketService(bucket_name="desempenho_alunos")

def etapa1():
    bucket_s.upload_file('dados/desempenho_alunos.csv', 'bronze/csv/desempenho_alunos.csv')

def etapa2():
    bucket_s.csv_to_parquet('bronze/csv/desempenho_alunos.csv', output_path='bronze/parquet/', separador=',')

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
        parquet['Id'] = [str(uuid.uuid4()) for _ in range(len(parquet))]

        estudantes = parquet[['Id', 'Age', 'ParentSupport', 'PartTimeJob', 'InternetAccess']].copy()
        estudantes['Gender_id'] = parquet.apply(lambda row: getDataId(row['Gender'], gender), axis=1)
        estudantes['Grades_id'] = parquet.apply(lambda row: getDataId(row['Grade'], grades), axis=1)
        estudantes['Races_id'] = parquet.apply(lambda row: getDataId(row['Race'], races), axis=1)
        estudantes['Locales_id'] = parquet.apply(lambda row: getDataId(row['Locale'], locales), axis=1)
        estudantes['School_types_id'] = parquet.apply(lambda row: getDataId(row['SchoolType'], school_types), axis=1)
        
        desempenho = parquet[['Id','TestScore_Math', 'TestScore_Reading', 'TestScore_Science', 'StudyHours']].copy()
        desempenho.rename(columns={
            'Id': 'User_id'
        }, inplace=True)
        desempenho['StudyHours'] = desempenho.apply(lambda row: float(row['StudyHours']) * (24/4) if row['StudyHours'] else 0, axis=1)

        salvarParquet(estudantes, f"students/students{i + 1}.parquet")
        salvarParquet(desempenho, f"performances/performance{i + 1}.parquet")

    dfgender = pd.DataFrame(list(gender.items()), columns=['Name', 'Id'])
    dfgrades = pd.DataFrame(list(grades.items()), columns=['Name', 'Id'])
    dfraces = pd.DataFrame(list(races.items()), columns=['Name', 'Id'])
    dflocales = pd.DataFrame(list(locales.items()), columns=['Name', 'Id'])
    dfschool_types = pd.DataFrame(list(school_types.items()), columns=['Name', 'Id'])

    tnames = ["genders", "grades", "races", "locales", "school_types"]
    tables = [dfgender, dfgrades, dfraces, dflocales, dfschool_types]

    for i in range(len(tables)):
        salvarParquet(tables[i], f"{tnames[i]}/{tnames[i]}.parquet")


def etapa4():
    id_dataset =  'desempenho_alunos'
    bucket_s.to_bigQuery(id_dataset)
    

if __name__ == "__main__":
    etapa3()
    etapa4()
