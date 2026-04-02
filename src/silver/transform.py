import pandas as pd
import logging as log
import html
from pathlib import Path

logger = log.getLogger(__name__)

def columns(df: pd.DataFrame) -> pd.DataFrame:
    
    if df.empty:
        logger.warning("Dataframe vazio para renomear colunas")
        return df 
     
    try:
        df.columns = df.columns.str.strip()
        
        df = df[[
            "tags",	
            "is_answered", 
            "view_count",	
            "answer_count",	
            "score",	
            "last_activity_date",	
            "creation_date",
            "question_id",	
            "content_license",	
            "title",	
            "owner.account_id",	
            "owner.reputation",	
            "owner.user_id",	
            "owner.user_type",		
            "owner.display_name",		
            "last_edit_date",	
            "owner.accept_rate"
        ]]
        
        rename_columns = {
            "tags": "tags",
            "is_answered": "respondida",
            "view_count": "quantidade_visualizacoes",
            "answer_count": "quantidade_respostas",
            "score": "pontuacao",
            "last_activity_date": "data_ultima_atividade",
            "creation_date": "data_criacao",
            "question_id": "id_pergunta",
            "content_license": "licenca_conteudo",
            "title": "titulo",
            "owner.account_id": "id_conta_usuario",
            "owner.reputation": "reputacao_usuario",
            "owner.user_id": "id_usuario",
            "owner.user_type": "tipo_usuario",
            "owner.display_name": "nome_usuario",
            "last_edit_date": "data_ultima_edicao",
            "owner.accept_rate": "taxa_aceitacao_usuario"
        }
        
        
        df.rename(columns=rename_columns, inplace=True)
        logger.info(f"Colunas transformadas: {df.shape[1]}")
        
        return df 
    
    except Exception as erro:
        logger.error(f"Erro ao transformar colunas: {erro}")
        raise

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    
    if df.empty:
        logger.warning("Dataframe vazio para validação dos dados")
        return df 
    try:
        date_cols = ["data_criacao", "data_ultima_atividade", "data_ultima_edicao"]
        
        for columns_date in date_cols:
            if columns_date in df.columns:
                df[columns_date] = pd.to_datetime(df[columns_date], unit= "s", errors= "coerce")
                           
                null_dates = int(df[columns_date].isna().sum())
            
                if null_dates:
                    logger.warning(f"{columns_date} invalido: {null_dates}")
             
        numeric_types = ["quantidade_visualizacoes", "quantidade_respostas", "pontuacao", "reputacao_usuario"]
        
        for cols_numeric in numeric_types:
            if cols_numeric in df.columns:
                df[cols_numeric] = pd.to_numeric(df[cols_numeric], errors= "coerce")
            
                null_numerics = int(df[cols_numeric].isna().sum())
                zero_numerics = int((df[cols_numeric]==0).sum())
            
                if null_numerics:
                    logger.warning(f"{cols_numeric} invalidos: {null_numerics}")
                
                if zero_numerics:
                    logger.warning(f"{cols_numeric} zerados: {zero_numerics}")
                    
        cols_str = ["titulo", "nome_usuario", "tipo_usuario"] 
        
        for columns_str in cols_str:
            if columns_str in df.columns:
                df[columns_str] = df[columns_str].fillna("").apply(html.unescape)
                df[columns_str] = df[columns_str].str.title()
        
        cols_int = [
            "quantidade_visualizacoes", 
            "quantidade_respostas", 
            "pontuacao",
            "reputacao_usuario",
            "id_conta_usuario", 
            "id_usuario", 
            "id_pergunta"
        ]

        for columns_int in cols_int:
            if columns_int in df.columns:
                df[columns_int] = df[columns_int].fillna(0).astype("int64")
        
        logger.info("Validações concluidas")
        
        return df 
            
    except Exception as erro:
        logger.error(f"Erro na validação dos dados: {erro}")
        raise 

def transform(df:pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando transformações")
    
    df = columns(df)
    df = validate_data(df)
    
    logger.info(f"Transformações realizadas / Colunas: {df.shape[1]}, Linhas: {len(df)}")
    
    return df 
    
def load_silver_datalake(df:pd.DataFrame) -> None:
    logger.info("Iniciando carga no data lake silver")
    
    silver_path = Path("data_lake/silver/analise_dificuldade_programacao_tratado.parquet")
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_parquet(silver_path, index= False)
        logger.info("Carga realizada no data lake silver")
    
    except Exception as erro:
        logger.error(f"Erro ao carrega no data lake silver: {erro}")
        raise 