from script.bucketService import BucketService
import io
import pandas as pd

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
        
        colStudents = ['NU_INSCRICAO', 'TP_FAIXA_ETARIA', 'TP_ESCOLA']
        colStates = ['SG_UF_ESC']
        colScore = ['NU_INSCRICAO', 'NU_NOTA_CN',"NU_NOTA_CH","NU_NOTA_LC","NU_NOTA_MT","NU_NOTA_REDACAO" ]
        
        parquet.dropna(subset=[*colStudents, *colStates, *colScore], inplace=True)
        
        faixa_etaria = {
            1: "-17 anos",
            2: "17 anos",
            3: "18 anos",
            4: "19 anos",
            5: "20 anos",
            6: "21 anos",
            7: "22 anos",
            8: "23 anos",
            9: "24 anos",
            10: "25 anos",
            11: "26 - 30 anos",
            12: "31 - 35 anos",
            13: "36 - 40 anos",
            14: "41 - 45 anos",
            15: "46 - 50 anos",
            16: "51 - 55 anos",
            17: "56 - 60 anos",
            18: "61 - 65 anos",
            19: "66 - 70 anos",
            20: "70+ anos"
        }
        
        escola = {
            1: "Pública",
            2: "Pública",
            3: "Privada"
        }
        
        students = parquet[colStudents].copy()
        students['STATE_ID'] = parquet['SG_UF_ESC'].apply(lambda x: getDataId(x, states))
        students['TP_FAIXA_ETARIA'] = parquet['TP_FAIXA_ETARIA'].apply(lambda x: faixa_etaria.get(int(x), "Sem Informação"))
        students['TP_ESCOLA'] = parquet['TP_ESCOLA'].apply(lambda x: escola.get(int(x), "Sem Informação"))
        
        score = parquet[colScore].copy()
        
        salvarParquet(students, f"students/students{i + 1}.parquet")
        salvarParquet(score, f"score/score{i + 1}.parquet")
    
    
    estados = {
        "AC": "Acre",
        "AL": "Alagoas",
        "AP": "Amapá",
        "AM": "Amazonas",
        "BA": "Bahia",
        "CE": "Ceará",
        "DF": "Distrito Federal",
        "ES": "Espírito Santo",
        "GO": "Goiás",
        "MA": "Maranhão",
        "MT": "Mato Grosso",
        "MS": "Mato Grosso do Sul",
        "MG": "Minas Gerais",
        "PA": "Pará",
        "PB": "Paraíba",
        "PR": "Paraná",
        "PE": "Pernambuco",
        "PI": "Piauí",
        "RJ": "Rio de Janeiro",
        "RN": "Rio Grande do Norte",
        "RS": "Rio Grande do Sul",
        "RO": "Rondônia",
        "RR": "Roraima",
        "SC": "Santa Catarina",
        "SP": "São Paulo",
        "SE": "Sergipe",
        "TO": "Tocantins"
    }

    for sigla in list(states.keys()):
        value = states[sigla]
        states[estados.get(sigla.upper())] = value
        del states[sigla]

    dfStates = pd.DataFrame(list(states.items()), columns=['Name', 'Id'])
    salvarParquet(dfStates, f"states/states.parquet")
    
def etapa4():
    id_dataset = 'enem_2023_gold'
    bucket_s.to_bigQuery(id_dataset)
    

if __name__ == "__main__":
    etapa3()
    etapa4()
