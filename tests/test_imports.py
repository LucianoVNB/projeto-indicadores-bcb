
def test_dag_import():
    """
    Verifica se o módulo da DAG pode ser importado sem erros de sintaxe.
    """
    try:
        from dags import carga_completa_bcb
    except Exception as e:
        assert False, f"Falha ao importar a DAG: {e}"

def test_script_functions_import():
    """
    Verifica se as funções de lógica de negócio podem ser importadas.
    """
    try:
        from src.scripts.extracao_indicadores_bcb import (
            executa_carga_completa_bronze,
            executa_carga_completa_silver_para_parquet,
            executa_carga_completa_silver_para_postgres,
            cria_spark_session
        )
    except ImportError as e:
        assert False, f"Falha ao importar funções do script: {e}"