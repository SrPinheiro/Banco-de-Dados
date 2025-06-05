# 🧠 Análise de Dados com Arquitetura Bronze, Silver e Gold

Este repositório tem o objetivo de realizar uma análise estruturada de dados utilizando a abordagem em camadas **Bronze, Silver e Gold** de pipelines de dados modernos.

---

## 🎯 Objetivo

O principal objetivo deste trabalho é:

(escolher objetivo e colocar aqui)

para isso, vamos aplicar os conceitos:

- Aplicar conceitos de **engenharia de dados** e **modelagem de banco de dados**.
- Estruturar e processar dados em múltiplas camadas de qualidade:
  - **Bronze**: dados brutos, sem tratamento.
  - **Silver**: dados limpos, normalizados e com tipos corrigidos.
  - **Gold**: dados prontos para análise e visualização (dashboards, relatórios etc).
- Utilizar ferramentas adequadas para **armazenar**, **consultar** e **visualizar** dados.

---

## Estrutura do Projeto

```bash
dados/
 ┣ dado.csv
chaves/
 ┣ chaveJson.json
scripts/
 ┣ upload_data.sql
 ┣ bronze_to_silver.sql
 ┣ silver_to_gold.sql

```

## Estrutura do DataLake

```bash
 dataLake/
 ┣  bronze/
 ┣  silver/
 ┗  gold/
```

## Instalar projeto:

- configurar ambiente virtual, execute o comando:

```bash
python -m venv env
```

- acessar o ambiente virtual:

```bash
# Windows
.\env\Scripts\Activate.ps1

# Linux
source env\bin\Activate
```

- instalar dependencias:

```bash
pip install -r requirements.txt
```

- agora é so rodar os scripts e ser feliz

```bash
python script/upload_dados.py
```
