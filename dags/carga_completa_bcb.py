import sys
import os
from datetime import datetime

from airflow.decorators import dag, task

# Obtém o diretório da DAG e sobe um nível para a raiz do projeto
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Agora podemos importar as funções do seu script que está em src/scripts/
from src.scripts.extracao_indicadores_bcb import (
    executa_carga_completa_bronze,
    executa_carga_completa_silver_para_parquet,
    executa_carga_completa_silver_para_postgres,
    cria_spark_session
)

# Os caminhos devem ser os usados dentro do Docker
BRONZE_PATH = '/opt/airflow/data/bronze'
SILVER_PATH = '/opt/airflow/data/silver'
ANO_INICIO_CARGA = 2015
ANO_FIM_CARGA = datetime.now().year

INDICADORES = {
    'selic': {
        'id_serie': '11', 'prefixo_arquivo': 'selic_', 'nome_tabela_pg': 'taxa_selic_mensal'
    },
    'ipca': {
        'id_serie': '433', 'prefixo_arquivo': 'ipca_', 'nome_tabela_pg': 'taxa_ipca_mensal'
    }
}

# Definição da DAG
@dag(
    dag_id='dag_carga_completa_bcb',
    default_args={'owner': 'luciano', 'start_date': datetime(2024, 6, 9)},
    description='DAG da carga completa manual dos indicadores bcb.',
    schedule_interval=None,
    catchup=False,
    tags=['carga_completa', 'manual', 'bcb'],
)
def dag_carga_completa_bcb():
    """
    ### DAG de Carga Completa (Versão Refatorada)
    Esta DAG orquestra o pipeline completo importando a lógica de negócio
    do diretório `src/scripts`.
    """

    # --- Tarefa 1: Extração para a Camada Bronze ---
    @task
    def tarefa_carga_bronze():
        for nome, info in INDICADORES.items():
            # Chama a função importada
            executa_carga_completa_bronze(nome, info, BRONZE_PATH, ANO_INICIO_CARGA, ANO_FIM_CARGA)
    
    # --- Tarefa 2: Processamento Silver (Bronze -> Parquet) ---
    @task
    def tarefa_silver_para_parquet():
        resultados = []

        #Cria Spark Session
        spark = cria_spark_session()

        for nome, info in INDICADORES.items():
            # Chama a função importada
            sucesso = executa_carga_completa_silver_para_parquet(spark, nome, info, BRONZE_PATH, SILVER_PATH)
            resultados.append(sucesso)
        # Se qualquer uma das transformações falhar, a tarefa falha
        if not all(resultados):
            raise ValueError("Uma ou mais transformações para Parquet falharam.")
        spark.stop()

    # --- Tarefa 3: Carregamento para o Banco de Dados (Parquet -> Postgres) ---
    @task
    def tarefa_silver_para_postgres():
        #Cria Spark Session
        spark = cria_spark_session()

        for nome, info in INDICADORES.items():
            # Chama a função importada
            executa_carga_completa_silver_para_postgres(spark, nome, info, SILVER_PATH)
        spark.stop()
    
    tarefa_carga_bronze() >> tarefa_silver_para_parquet() >> tarefa_silver_para_postgres()

# Instancia a DAG
dag_carga_completa_bcb()