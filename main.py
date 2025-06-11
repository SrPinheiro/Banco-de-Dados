from script.bucketService import BucketService


bucket_s = BucketService()

# bucket.listar_arquivos('bronze/csv/')
# bucket_s.csv_to_parquet('bronze/csv/microdados_enem.csv', output_path='bronze/parquet/')
bucket_s.aprimorar_dados()
# dados = bucket_s.listar_arquivos('bronze/parquet/')
# for dado in dados:
#     print(dado.name)

