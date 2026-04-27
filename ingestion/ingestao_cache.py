import os
import pyodbc
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
from faker import Faker
from google.cloud import bigquery

# ==================== CONFIGURAÇÕES DE LOG E AMBIENTE ====================
load_dotenv()
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CHUNK_SIZE = 50000 

# ==================== FUNÇÕES DE TRATAMENTO E CARGA ====================

def anonimizar_lote(df):
    """
    Função temporariamente desativada para teste de integridade.
    Retorna o DataFrame original sem alterações.
    """
    
    # for col in df.columns:
    #     if col.lower() in COLUNAS_SENSIVEIS:
    #         df[col] = df[col].apply(lambda x: fake.name() if pd.notnull(x) else x)
    return df

def obter_intervalo_datas(cursor, tabela, coluna_data):
    """Busca as datas mínima e máxima para processamento mensal."""
    try:
        cursor.execute(f"SELECT MIN({coluna_data}), MAX({coluna_data}) FROM {tabela}")
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Erro ao buscar datas na tabela {tabela}: {e}")
        return None, None

def carregar_lote_bq(client, df, tabela_destino, modo_escrita):
    """Faz o streaming do DataFrame para o BigQuery com confirmação."""
    dataset_id = os.getenv('GCP_DATASET_RAW')
    project_id = os.getenv('GCP_PROJECT_ID')
    table_id = f"{project_id}.{dataset_id}.{tabela_destino}"
    
    job_config = bigquery.LoadJobConfig(write_disposition=modo_escrita)
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        logger.info(f"    Lote de {len(df)} linhas enviado para {tabela_destino} ({modo_escrita})")
    except Exception as e:
        logger.error(f"   Falha no carregamento do BigQuery: {e}")
        raise

# ==================== ORQUESTRADOR DE JOBS ====================

def processar_job(job, bq_client, cache_conn):
    tabela_origem = job['tabela_origem']
    tabela_destino = job['tabela_destino']
    logger.info(f" INICIANDO JOB: {tabela_origem} -> {tabela_destino}")
    
    cursor = cache_conn.cursor()
    modo_escrita = "WRITE_TRUNCATE" 
    total_linhas_job = 0

    try:
        # CENÁRIO A: Processamento Mensal (Tabelas Fato / Grandes)
        if job.get('particionamento_mensal'):
            col_data = job['coluna_filtro']
            data_min, data_max = obter_intervalo_datas(cursor, tabela_origem, col_data)
            
            if not data_min or not data_max:
                logger.warning(f" Tabela {tabela_origem} vazia ou sem datas. Pulando...")
                return

            meses = pd.date_range(start=data_min, end=data_max, freq='MS')
            for inicio_mes in meses:
                fim_mes = inicio_mes + pd.offsets.MonthEnd(1)
                query = f"SELECT * FROM {tabela_origem} WHERE {col_data} >= ? AND {col_data} <= ?"
                
                df_iter = pd.read_sql(query, cache_conn, params=[inicio_mes, fim_mes], chunksize=CHUNK_SIZE)
                
                for chunk in df_iter:
                    chunk = anonimizar_lote(chunk)
                    chunk['extracted_at'] = datetime.now().isoformat()
                    
                    # Normalização
                    chunk = chunk.astype(str).replace(['None', 'nan', 'NaN'], None)
                    
                    carregar_lote_bq(bq_client, chunk, tabela_destino, modo_escrita)
                    total_linhas_job += len(chunk)
                    modo_escrita = "WRITE_APPEND"
                
                logger.info(f"    Mês {inicio_mes.strftime('%Y-%m')} concluído.")

        # CENÁRIO B: Carga Direta (Tabelas Dimensão ou menores)
        else:
            df_iter = pd.read_sql(f"SELECT * FROM {tabela_origem}", cache_conn, chunksize=CHUNK_SIZE)
            
            for chunk in df_iter:
                chunk = anonimizar_lote(chunk)
                chunk['extracted_at'] = datetime.now().isoformat()
                
                # Normalização
                chunk = chunk.astype(str).replace(['None', 'nan', 'NaN'], None)
                
                carregar_lote_bq(bq_client, chunk, tabela_destino, modo_escrita)
                total_linhas_job += len(chunk)
                modo_escrita = "WRITE_APPEND"

        logger.info(f" SUCESSO: {tabela_destino} finalizada com {total_linhas_job:,} linhas.")
        print("-" * 60)

    except Exception as e:
        logger.error(f" Falha crítica no Job {tabela_destino}: {e}")

# ==================== DEFINIÇÃO DA MALHA DE DADOS ====================

JOBS_CONFIG = [
    {'tabela_origem': 'dw_fat.Cliente', 'tabela_destino': 'clientes'},
    {'tabela_origem': 'dw_cad.Produto', 'tabela_destino': 'produtos'},
    {'tabela_origem': 'dw_fat.Representante', 'tabela_destino': 'representantes'},
    {'tabela_origem': 'dw_ped.Pedido', 'tabela_destino': 'pedidos'}
]

if __name__ == "__main__":
    logger.info("Iniciando Motor de Ingestão (Modo Teste - Sem Faker): Caché -> BigQuery RAW")
    
    client_bq = bigquery.Client()
    dsn_cache = os.getenv('CACHE_DSN')
    
    try:
        with pyodbc.connect(f'DSN={dsn_cache}', autocommit=True) as conn:
            for config in JOBS_CONFIG:
                processar_job(config, client_bq, conn)
        logger.info(" PROCESSO GERAL FINALIZADO.")
    except Exception as e:
        logger.error(f" Erro de conexão com o banco de origem: {e}")
