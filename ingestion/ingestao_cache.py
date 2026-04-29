import os
import pyodbc
import pandas as pd
import logging
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

CHUNK_SIZE = 50000 

# ==================== TREATMENT AND LOAD FUNCTIONS ====================

def anonimizar_lote(df):
    """
    Function temporarily disabled for integrity testing.
    Returns the original DataFrame unchanged.
    """
    
    # for col in df.columns:
    #     if col.lower() in COLUNAS_SENSIVEIS:
    #         df[col] = df[col].apply(lambda x: fake.name() if pd.notnull(x) else x)
    return df

def obter_intervalo_datas(cursor, tabela, coluna_data):
    """Search for the minimum and maximum dates for monthly processing."""
    try:
        cursor.execute(f"SELECT MIN({coluna_data}), MAX({coluna_data}) FROM {tabela}")
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Error fetching dates from the table {tabela}: {e}")
        return None, None

def carregar_lote_bq(client, df, tabela_destino, modo_escrita):
    """Streams the DataFrame to BigQuery with confirmation."""
    dataset_id = os.getenv('GCP_DATASET_RAW')
    project_id = os.getenv('GCP_PROJECT_ID')
    table_id = f"{project_id}.{dataset_id}.{tabela_destino}"
    
    job_config = bigquery.LoadJobConfig(write_disposition=modo_escrita)
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        logger.info(f"    Lot of {len(df)} lines sent to {tabela_destino} ({modo_escrita})")
    except Exception as e:
        logger.error(f"   BigQuery loading failure: {e}")
        raise

# ==================== Job Orchestrator ====================

def processar_job(job, bq_client, cache_conn):
    tabela_origem = job['tabela_origem']
    tabela_destino = job['tabela_destino']
    logger.info(f" STARTING JOB: {tabela_origem} -> {tabela_destino}")
    
    cursor = cache_conn.cursor()
    modo_escrita = "WRITE_TRUNCATE" 
    total_linhas_job = 0

    try:
        # SCENARIO A: Monthly Processing (Fact / Large Tables)
        if job.get('particionamento_mensal'):
            col_data = job['coluna_filtro']
            data_min, data_max = obter_intervalo_datas(cursor, tabela_origem, col_data)
            
            if not data_min or not data_max:
                logger.warning(f" Table {tabela_origem} empty or without dates. Skipping...")
                return

            meses = pd.date_range(start=data_min, end=data_max, freq='MS')
            for inicio_mes in meses:
                fim_mes = inicio_mes + pd.offsets.MonthEnd(1)
                query = f"SELECT * FROM {tabela_origem} WHERE {col_data} >= ? AND {col_data} <= ?"
                
                df_iter = pd.read_sql(query, cache_conn, params=[inicio_mes, fim_mes], chunksize=CHUNK_SIZE)
                
                for chunk in df_iter:
                    chunk = anonimizar_lote(chunk)
                    chunk['extracted_at'] = datetime.now().isoformat()
                    
                    # Normalization
                    chunk = chunk.astype(str).replace(['None', 'nan', 'NaN'], None)
                    
                    carregar_lote_bq(bq_client, chunk, tabela_destino, modo_escrita)
                    total_linhas_job += len(chunk)
                    modo_escrita = "WRITE_APPEND"
                
                logger.info(f"    Month {inicio_mes.strftime('%Y-%m')} concluded.")

        # SCENARIO B: Direct Load (Dimension Tables or smaller)
        else:
            df_iter = pd.read_sql(f"SELECT * FROM {tabela_origem}", cache_conn, chunksize=CHUNK_SIZE)
            
            for chunk in df_iter:
                chunk = anonimizar_lote(chunk)
                chunk['extracted_at'] = datetime.now().isoformat()
                
                # Normalization
                chunk = chunk.astype(str).replace(['None', 'nan', 'NaN'], None)
                
                carregar_lote_bq(bq_client, chunk, tabela_destino, modo_escrita)
                total_linhas_job += len(chunk)
                modo_escrita = "WRITE_APPEND"

        logger.info(f" SUCCESS: {tabela_destino} completed with {total_linhas_job:,} lines.")
        print("-" * 60)

    except Exception as e:
        logger.error(f" Critical failure in the Job {tabela_destino}: {e}")

# ==================== DEFINITION OF THE DATA MESH ====================

JOBS_CONFIG = [
    {'tabela_origem': 'dw_fat.Cliente', 'tabela_destino': 'clientes'},
    {'tabela_origem': 'dw_cad.Produto', 'tabela_destino': 'produtos'},
    {'tabela_origem': 'dw_fat.Representante', 'tabela_destino': 'representantes'},
    {'tabela_origem': 'dw_ped.Pedido', 'tabela_destino': 'pedidos'}
]

if __name__ == "__main__":
    logger.info("Starting Ingestion Engine: Caché -> BigQuery RAW")
    
    client_bq = bigquery.Client()
    dsn_cache = os.getenv('CACHE_DSN')
    
    try:
        with pyodbc.connect(f'DSN={dsn_cache}', autocommit=True) as conn:
            for config in JOBS_CONFIG:
                processar_job(config, client_bq, conn)
        logger.info(" GENERAL PROCESS COMPLETED.")
    except Exception as e:
        logger.error(f" Connection error with the source database: {e}")
