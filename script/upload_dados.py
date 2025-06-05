from google.cloud import storage

client = storage.Client.from_service_account_json('chaves/chaveDeServico.json')

nome_do_bucket = 'bucket-para-enem'
arquivo_para_upload = 'dados/MICRODADOS_ENEM_2023.csv'
nome_do_destino = 'bronze/csv/microdados_enem.csv'

bucket = client.bucket(nome_do_bucket)
blob = bucket.blob(nome_do_destino)

print('-------- Enviando arquivo ----------')
blob.upload_from_filename(arquivo_para_upload)
print('-------- Arquivo enviado ----------')
