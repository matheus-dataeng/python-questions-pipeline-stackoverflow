import requests
import pandas as pd 
import logging as log
from pathlib import Path
import os 
from dotenv import load_dotenv 
from src.s3_loader import upload_s3

logger = log.getLogger(__name__)
load_dotenv()

def extract() -> pd.DataFrame:
    
    logger.info("Extraindo dados") 
    
    try:   
        url = os.getenv("API_URL")
        
        if not url:
            logger.error("Variavel não definida no .env")
            return pd.DataFrame()
        
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Erro na requisição: {response.status_code}")
            return pd.DataFrame()
            
        data = response.json()
        
        if not data:
            logger.error("Nenhum dado retornado")
            return pd.DataFrame()
        
        if "items" not in data:
            logger.error("Coluna 'items' não encontrada")
            return pd.DataFrame()
        
        logger.info("Transformando para dataframe")
        df = pd.json_normalize(data["items"])
        logger.info(f"Transformação concluída / Colunas: {df.shape[1]}, Linhas: {len(df)}")
        
        return df 
            
    except Exception as erro:
        logger.error(f"Erro ao transformar para dataframe: {erro}")
        raise
        
def load_bronze_datalake(df: pd.DataFrame) -> None:
    logger.info("Iniciando carga no data lake bronze")
    
    bronze_path = Path("data_lake/bronze/analise_dificuldade_programacao_bruto.parquet")
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_parquet(bronze_path, index=False) 
        upload_s3(bronze_path, "bronze/analise_dificuldade_programacao_bruto.parquet") 
        logger.info("Carga realizada no data lake bronze")
    
    except Exception as erro:
        logger.error(f"Falha ao carregar no data lake bronze: {erro}")
        raise
            