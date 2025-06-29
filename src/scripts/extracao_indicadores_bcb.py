import requests
import json
import pandas as pd
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, last, month, year, lit
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import glob

#Funcao para execucao completa da camada bronze
def executa_carga_completa_bronze(indicador_nome, indicador_info, bronze_path, ano_inicio_carga, ano_fim_carga):
    
    #Para um determinado indicador, itera de ANO_INICIO_CARGA até o ano atual.
    #Verifica se o arquivo daquele ano existe na camada Bronze.
    #Se não existir, executa uma carga para o ano completo.
    
    print(f"\n--- [Carga completa Bronze] Processando indicador: {indicador_nome.upper()} ---")

    # Loop para cada ano no intervalo definido
    for ano in range(ano_inicio_carga, ano_fim_carga + 1):
        print(f"  Verificando ano: {ano}")

        # 1. Monta o nome e o caminho do arquivo
        nome_arquivo = f"{indicador_info['prefixo_arquivo']}{ano}.json"
        caminho_arquivo_anual = os.path.join(bronze_path, nome_arquivo)

        # 2. Verifica se o arquivo do ano em questao existe
        if os.path.exists(caminho_arquivo_anual):
            print(f"    Arquivo '{nome_arquivo}' já existe. Pulando.")
            continue # Se existe, pula para o próximo ano

        # 3. Se nao existe, executa a carga
        print(f"    Arquivo não encontrado. Iniciando carga para o ano de {ano}.")
        
        # Define o período de 1 de janeiro a 31 de dezembro do ano corrente necessario para a url
        data_inicio_ano = f'01/01/{ano}'
        data_fim_ano = f'31/12/{ano}'

        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{indicador_info['id_serie']}/dados?"
            f"formato=json&dataInicial={data_inicio_ano}&dataFinal={data_fim_ano}"
        )
        
        response = requests.get(url)
        print(f'**********{response}**********')

        if response.status_code == 200:
            dados = response.json()
            if not dados: # Verifica se a API retornou uma lista vazia
                print(f"    A API não retornou dados para o ano de {ano}. Nenhum arquivo será criado.")
                continue

            print(f"    Sucesso! {len(dados)} registros encontrados para {ano}.")

            # 4. Salva os dados no arquivo anual
            os.makedirs(bronze_path, exist_ok=True)
            with open(caminho_arquivo_anual, 'w', encoding='utf-8') as f:
                json.dump(dados, f, ensure_ascii=False, indent=4)
            
            print(f"    Dados de {ano} salvos com sucesso em '{nome_arquivo}'")
        else:
            print(f"    Erro ao buscar dados para o ano de {ano}. Status Code: {response.status_code}")

    print(f"--- Fim do processamento para: {indicador_nome.upper()} ---")


#Funcao de transformacao de bronze para silver em parquet 
def executa_carga_completa_silver_para_parquet(spark, indicador_nome, indicador_info, bronze_path, silver_path):
    
    #Carrega JSON da camada Bronze, aplica transformações
    #e salva o resultado em formato Parquet na camada Silver.
    
    print(f"\n--- [Carga completa Silver para Parquet] Processando indicador: {indicador_nome.upper()} ---")
    
    # Lista os arquivos JSON na camada Bronze
    caminho_padrao_bronze = os.path.join(bronze_path, f"{indicador_info['prefixo_arquivo']}*.json")
    lista_de_arquivos = glob.glob(caminho_padrao_bronze) #Equivalente: ls ./data/bronze/selic_*.json

    if not lista_de_arquivos:
        print(f"  Nenhum arquivo encontrado para o padrão '{caminho_padrao_bronze}'. Pulando transformação.")
        return False # Retorna False para indicar que não há nada a carregar

    print(f"  Lendo {len(lista_de_arquivos)} arquivos da camada Bronze.")
    
    # Define o schema para garantir a leitura correta dos dados
    schema_esperado = StructType([
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True)
    ])
    
    # Lê os dados usando o schema definido e salva em um dataframe
    df_bronze = spark.read.option("multiline", "true").schema(schema_esperado).json(lista_de_arquivos)
    
    # Transformacao de tipo dos campos data e valor
    df_bronze_transformado = df_bronze.withColumn("data", to_date(col("data"), "dd/MM/yyyy")) \
                               .withColumn("valor", col("valor").cast(DoubleType()))
    
    # Transformacao especifica para cada indicador
    if indicador_nome == 'selic':
        print("  Aplicando agregação mensal para SELIC.")
        df_bronze_transformado = df_bronze_transformado.withColumn("ano", year(col("data"))) \
                                  .withColumn("mes", month(col("data"))) \
                                  .groupBy("ano", "mes") \
                                  .agg(last("valor").alias("valor_mensal")) \
                                  .orderBy("ano", "mes")
        df_final = df_bronze_transformado.withColumn("indicador", lit("SELIC"))
    elif indicador_nome == 'ipca':
        print("  Renomeando colunas para IPCA.")
        df_bronze_transformado = df_bronze_transformado.withColumnRenamed("valor", "valor_mensal")
        df_final = df_bronze_transformado.withColumn("indicador", lit("IPCA"))
    

    print("  Schema final transformado:")
    df_final.printSchema()
    
    # Salva o resultado em formato Parquet na camada Silver
    caminho_escrita_silver = os.path.join(silver_path, indicador_nome)
    print(f"  Gravando dados em Parquet em: {caminho_escrita_silver}")
    df_final.write.mode("overwrite").parquet(caminho_escrita_silver)
    
    return True # Retorna True para indicar que a próxima etapa pode ser executada

#Funcao 
def executa_carga_completa_silver_para_postgres(spark, indicador_nome, indicador_info, silver_path):
    
    #Função 2: Lê os dados em formato Parquet da camada Silver
    #e os carrega para uma tabela no banco de dados PostgreSQL.

    print(f"\n--- [Carga completa Silver para Postgre] Carregando {indicador_nome.upper()} para o PostgreSQL ---")
    
    caminho_leitura_silver = os.path.join(silver_path, indicador_nome)
    
    # Lê os dados em Parquet da camada Silver
    print(f"  Lendo dados Parquet de: {caminho_leitura_silver}")
    df_silver = spark.read.parquet(caminho_leitura_silver)

    # Configurações de conexão com o banco de dados
    pg_properties = {"user": "user_silver", "password": "password_silver", "driver": "org.postgresql.Driver"}
    pg_url = "jdbc:postgresql://db_silver:5432/silver_db"

    # Escreve o DataFrame na tabela do PostgreSQL
    print(f"  Gravando dados na tabela '{indicador_info['nome_tabela_pg']}'")
    df_silver.write.jdbc(
        url=pg_url,
        table=indicador_info['nome_tabela_pg'],
        mode="overwrite",
        properties=pg_properties
    )
    print("  Dados carregados no PostgreSQL com sucesso.")

def cria_spark_session():
    spark = SparkSession.builder \
        .appName("PipelineIndicadoresEconomicos") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    return spark
