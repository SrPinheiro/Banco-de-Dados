import pandas as pd
import os

csv_path = "dados/MICRODADOS_ENEM_2023.csv"
output_parquet_dir = "parquet_chunks"
output_csv_path = "MICRODADOS_ENEM_2023_saida.csv"
output_parquet_path = "MICRODADOS_ENEM_2023_saida.parquet"

usar_chunks = False
salvar_csv = True
usar_parquet = False   
chunksize = 50000
usar_amostra = True
amostra_linhas = 50000
csv_sep = ';'
csv_encoding = 'latin1'

print(f"Convertendo CSV inteiro para vários arquivos Parquet em chunks de {chunksize} linhas...")

os.makedirs(output_parquet_dir, exist_ok=True)

leitor_chunks = pd.read_csv(csv_path, sep=csv_sep, encoding=csv_encoding, chunksize=chunksize)
numero_chunk = 1

for chunk in leitor_chunks:
    parquet_path = os.path.join(output_parquet_dir, f"MICRODADOS_ENEM_2023_chunk_{numero_chunk}.parquet")
    chunk.to_parquet(parquet_path, index=False)
    print(f"Chunk {numero_chunk} salvo em {parquet_path}")
    numero_chunk += 1

print("Conversão em chunks concluída!")