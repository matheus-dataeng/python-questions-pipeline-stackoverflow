import pandas as pd 
import logging as log 
from sqlalchemy import create_engine, text
import os 
from dotenv import load_dotenv

logger = log.getLogger(__name__)
load_dotenv()

def load(
    df_usuarios : pd.DataFrame,
    df_tempo : pd.DataFrame,
    df_tags : pd.DataFrame,
    df_perguntas : pd.DataFrame,
    fato_perguntas : pd.DataFrame,
    df_bridge :pd.DataFrame
    
) -> None:
    
    user = os.getenv("DB_USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    dbname = os.getenv("DBNAME")
    
    if not all([user, password, host, port, dbname]):
        logger.error("Variaveis não definidas no .env")
        raise ValueError("Variaveis não definidas")
    
    try:
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(url)
        
        table_dim_usuario = os.getenv("TABLE_DIM_USUARIOS")
        table_dim_tempo = os.getenv("TABLE_DIM_TEMPO")
        table_dim_tags = os.getenv("TABLE_DIM_TAGS")
        table_dim_perguntas = os.getenv("TABLE_DIM_PERGUNTAS")
        table_fato_perguntas = os.getenv("TABLE_FATO_PERGUNTAS")
        table_bridge_tags = os.getenv("TABLE_BRIDGE_TAGS")
        
        if not all([
            table_dim_usuario,
            table_dim_tempo,
            table_dim_tags,
            table_dim_perguntas,
            table_fato_perguntas,
            table_bridge_tags 
            
        ]):
            logger.error("Variaveis não definidas no .env")
            raise ValueError("Variaveis não definidas")
        
        tabelas_config = [
                {
                    "df": df_usuarios,
                    "tabela": table_dim_usuario,
                    "coluna_id" : "id_usuario"
                },
                
                {
                    "df": df_tempo,
                    "tabela" : table_dim_tempo,
                    "coluna_id" : "id_tempo"
                },
                
                {
                    "df" : df_tags,
                    "tabela" : table_dim_tags,
                    "coluna_id" : "id_tags"
                },
                
                {
                    "df" : df_perguntas,
                    "tabela" : table_dim_perguntas,
                    "coluna_id" : "id_pergunta"
                },
                
                {
                    "df" : fato_perguntas,
                    "tabela" : table_fato_perguntas,
                    "coluna_id" : "id_pergunta"
                },
                
                {
                    "df" : df_bridge,
                    "tabela" : table_bridge_tags,
                    "coluna_id" : "id_pergunta"

                }
             ]
        
        with engine.begin() as conn:
        
            for item in tabelas_config:
                df = item["df"]
                tabela = item["tabela"]
                coluna_id = item["coluna_id"]
   
                ids = df[coluna_id].dropna().astype(int).unique().tolist()
                
                result = conn.execute(
                    text(f'DELETE FROM {tabela} WHERE {coluna_id} = ANY(:ids)'),
                    {"ids" : ids}
                )
                
                logger.info(f"Registros removidos da tabela {tabela}: {result.rowcount}")
            
                df.to_sql(name = tabela, con = conn, index = False, chunksize = 1000, if_exists = "append")
                logger.info(f"Tabela carregada: {tabela} / Colunas: {df.shape[1]}, Linhas: {len(df)}")
                
    except Exception as e:
        logger.error(f"Falha no load: {e}")
        raise