import os
import pyodbc
import pandas as pd
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from faker import Faker
from google.cloud import bigquery

# ==================== LOG AND ENVIRONMENT SETTINGS ====================
load_dotenv()
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Resilience Settings
MAX_RETRIES = 3
RETRY_DELAY = 10 
CHUNK_SIZE_DEFAULT = 20000 

fake = Faker('pt_BR')

# ==================== SUPPORT FUNCTIONS ====================

def conectar_cache():
    """Create a new fresh connection with InterSystems Caché."""
    dsn = os.getenv('CACHE_DSN')
    return pyodbc.connect(f'DSN={dsn}', autocommit=True)

def anonimizar_lote(df):
    """Anonymizes sensitive columns (PII) for LGPD compliance."""
    colunas_sensíveis = ['nome', 'cliente_nome', 'contato', 'documento']
    for col in df.columns:
        if col.lower() in colunas_sensíveis:
            df[col] = df[col].apply(lambda x: fake.name() if pd.notnull(x) else x)
    return df

def carregar_lote_bq(client, df, tabela_destino, modo_escrita):
    dataset_id = os.getenv('GCP_DATASET_RAW')
    project_id = os.getenv('GCP_PROJECT_ID')
    table_id = f"{project_id}.{dataset_id}.{tabela_destino}"
    
    job_config = bigquery.LoadJobConfig(write_disposition=modo_escrita)
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        logger.info(f"   Batch of {len(df)} rows sent to {tabela_destino} ({modo_escrita})")
    except Exception as e:
        logger.error(f"    BigQuery load failure: {e}")
        raise

# ==================== JOB ORCHESTRATOR ====================

def executar_job_com_retry(job_config, bq_client):
    """Try to execute the job of a table, with support for retries and isolation."""
    tabela = job_config['tabela_destino']
    
    for tentativa in range(1, MAX_RETRIES + 1):
        conn = None
        try:
            logger.info(f" >>> STARTING JOB: {job_config['tabela_origem']} (Attempt {tentativa}/{MAX_RETRIES})")
            
            # ISOLATION: Opens a new connection for each attempt
            conn = conectar_cache()
            
            processar_extracao(job_config, bq_client, conn)
            
            logger.info(f" SUCCESS: {tabela} successfully completed.")
            return True

        except Exception as e:
            logger.warning(f" Attempt {tentativa} failed for {tabela}: {str(e)}")
            if tentativa < MAX_RETRIES:
                logger.info(f" Waiting {RETRY_DELAY}s to retry...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f" !!! CRITICAL: Job {tabela} failed after {MAX_RETRIES} attempts.")
        finally:
            if conn:
                conn.close()
    return False

def processar_extracao(job, bq_client, cache_conn):
    """Internal logic for reading and sending by chunks."""
    tabela_origem = job['tabela_origem']
    tabela_destino = job['tabela_destino']
    modo_escrita = "WRITE_TRUNCATE"
    total_linhas = 0
    
    # Define specific chunk size or use the default
    chunk_size = job.get('chunk_size', CHUNK_SIZE_DEFAULT)

    query = f"SELECT * FROM {tabela_origem}"
    
    df_iter = pd.read_sql(query, cache_conn, chunksize=chunk_size)
    
    for chunk in df_iter:

        chunk = anonimizar_lote(chunk)
        
        chunk['extracted_at'] = datetime.now().isoformat()
        
        chunk = chunk.astype(str).replace(['None', 'nan', 'NaN'], None)
        
        carregar_lote_bq(bq_client, chunk, tabela_destino, modo_escrita)
        
        total_linhas += len(chunk)
        modo_escrita = "WRITE_APPEND" 

# ==================== DEFINITION OF THE DATA MESH ====================

JOBS_CONFIG = [
    {'tabela_origem': 'dw_fat.Cliente', 'tabela_destino': 'clientes'},
    {'tabela_origem': 'dw_cad.Produto', 'tabela_destino': 'produtos'},
    {'tabela_origem': 'dw_fat.Representante', 'tabela_destino': 'representantes'},
    {'tabela_origem': 'dw_ped.Pedido', 'tabela_destino': 'pedidos'},
    # Critical/large tables with smaller chunk size to avoid timeout
    {'tabela_origem': 'dw_cre.ContasReceber', 'tabela_destino': 'contas_receber', 'chunk_size': 10000},
    {'tabela_origem': 'dw_est.EstoqueProduto', 'tabela_destino': 'estoque_produto', 'chunk_size': 10000}
]

if __name__ == "__main__":
    logger.info("Starting Ingestion Engine: Caché -> BigQuery RAW")
    start_time = time.time()
    
    client_bq = bigquery.Client()
    
    jobs_com_erro = 0
    for config in JOBS_CONFIG:
        sucesso = executar_job_com_retry(config, client_bq)
        if not sucesso:
            jobs_com_erro += 1
            
    end_time = time.time()
    logger.info(f" GENERAL PROCESS COMPLETED. Duration: {round((end_time - start_time)/60, 2)} min.")
    if jobs_com_erro > 0:
        logger.error(f" Attention: {jobs_com_erro} jobs failed completely.")