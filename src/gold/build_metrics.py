import pandas as pd
import logging as log 
from pathlib import Path

logger = log.getLogger(__name__)

def usuario(df:pd.DataFrame) -> pd.DataFrame:
    colunas_usuarios = [
        "id_usuario",
        "id_conta_usuario",
        "nome_usuario",
        "tipo_usuario",
        "reputacao_usuario",
        "taxa_aceitacao_usuario"
    ]
    
    df_usuarios = df[colunas_usuarios].drop_duplicates().reset_index(drop= True)
    logger.info(f"Criação dim_usuario / Colunas: {df_usuarios.shape[1]}, Linhas: {len(df_usuarios)}")
    return df_usuarios

def tempo(df:pd.DataFrame) -> pd.DataFrame:
    df_tempo = df[[
        "data_criacao"
    ]].drop_duplicates().reset_index(drop=True)
    
    df_tempo["id_tempo"] = df_tempo.index +1
    
    df_tempo["ano"] = df_tempo["data_criacao"].dt.year
    df_tempo["mes"] = df_tempo["data_criacao"].dt.month
    df_tempo["dia"] = df_tempo["data_criacao"].dt.day
    df_tempo["hora"] = df_tempo["data_criacao"].dt.hour
    
    dias_semana = {
        "Monday" : "Segunda-Feira",
        "Tuesday" : "Terça-Feira",
        "Wednesday" : "Quarta-Feira",
        "Thursday" : "Quinta-Feira",
        "Friday" : "Sexta-Feira",
        "Saturday" : "Sabado",
        "Sunday" : "Domingo"
    }
    
    df_tempo["dia_semana"] = df_tempo["data_criacao"].dt.day_name().replace(dias_semana)
    
    df_tempo = df_tempo[[
        "id_tempo",
        "data_criacao",
        "ano",
        "mes",
        "dia",
        "hora",
        "dia_semana"
    ]]
    
    logger.info(f"Criação dim_tempo / Colunas: {df_tempo.shape[1]}, Linhas: {len(df_tempo)}")
    return df_tempo

def tags(df: pd.DataFrame) -> pd.DataFrame:
    df_tags = df[["tags"]].explode("tags")
    df_tags = df_tags.drop_duplicates().reset_index(drop=True)
    
    df_tags["id_tags"] = df_tags.index +1
    
    df_tags = df_tags[["id_tags","tags"]]
    
    logger.info(f"Criação dim_tags / Colunas: {df_tags.shape[1]}, Linhas: {len(df_tags)}")
    return df_tags

def bridge_tags(df: pd.DataFrame, df_tags: pd.DataFrame) -> pd.DataFrame:
    df_bridge = df[["id_pergunta", "tags"]].explode("tags")
    
    df_bridge = df_bridge.merge(
        df_tags,
        on=["tags"],
        how="left"
    )
    
    df_bridge = df_bridge[["id_pergunta", "id_tags"]].drop_duplicates().reset_index(drop=True)
    
    logger.info(f"Criação bridge_tags / Colunas: {df_bridge.shape[1]}, Linhas: {len(df_bridge)}")
    return df_bridge
    
def perguntas(df: pd.DataFrame) -> pd.DataFrame:
    colunas_perguntas = [
        "id_pergunta",
        "titulo",
        "licenca_conteudo"
    ]
    
    df_perguntas = df[colunas_perguntas].drop_duplicates().reset_index(drop=True)
    
    logger.info(f"Criação dim_perguntas / Colunas: {df_perguntas.shape[1]}, Linhas: {len(df_perguntas)}")
    return df_perguntas

def fato(
    df:pd.DataFrame,
    df_usuarios: pd.DataFrame,
    df_tempo: pd.DataFrame,
    df_perguntas: pd.DataFrame
) -> pd.DataFrame:
    
    fato_perguntas = df.merge(
        df_usuarios,
        on = ["id_usuario"],
        how= "left"
    )
    
    fato_perguntas = fato_perguntas.merge(
        df_tempo,
        on = ["data_criacao",],
        how= "left"
    )
    
    fato_perguntas = fato_perguntas.merge(
        df_perguntas,
        on = ["id_pergunta"],
        how= "left"
    )
    
    colunas_fato = [
        "id_pergunta",
        "id_usuario",
        "id_tempo",
        "quantidade_visualizacoes",
        "quantidade_respostas",
        "pontuacao",
        "respondida"
    ]
    
    fato_perguntas = fato_perguntas[colunas_fato].drop_duplicates().reset_index(drop= True)
    
    logger.info(f"Criação fato_perguntas / Colunas: {fato_perguntas.shape[1]}, Linhas: {len(fato_perguntas)}")
    return fato_perguntas

def build_metrics(df:pd.DataFrame) -> dict[str, pd.DataFrame]:
    logger.info("Iniciando criação das metricas")
    
    df_usuarios = usuario(df)
    df_tempo = tempo(df)
    df_tags = tags(df)
    df_perguntas = perguntas(df)
    fato_perguntas = fato(
        df,
        df_usuarios,
        df_tempo,
        df_perguntas
    )
    df_bridge = bridge_tags(df, df_tags)
    
    return {
        "dim_usuario" : df_usuarios,
        "dim_tempo" : df_tempo,
        "dim_tags" : df_tags,
        "dim_perguntas" : df_perguntas,
        "fato_perguntas" : fato_perguntas,
        "bridge_tags" : df_bridge
    }

def salvar_datalake(df:pd.DataFrame, nome_tabela:str) -> None:

    path = Path(f"data_lake/gold/{nome_tabela}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_parquet(path, index=False)
        logger.info(f"{nome_tabela} salva em {path}")
    
    except Exception as e:
        logger.error(f"Erro ao salvar {nome_tabela}: {e}")

def load_gold_datalake(tabelas: dict[str, pd.DataFrame]) -> None:
    logger.info("Iniciando carga no data lake gold")
    
    try:
        for nome_tabela, df_tabela in tabelas.items():
                salvar_datalake(df_tabela, nome_tabela)
                
        logger.info("Carga finalizada")
        
    except Exception as e:
        logger.error(f"Erro ao carregar metricas no data lake gold: {e}")
        raise 