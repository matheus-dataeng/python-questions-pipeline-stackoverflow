from src.silver.transform import columns, validate_data
import pandas as pd 
import pytest

def test_columns():
    df = pd.DataFrame({
        "tags": ["exemplo"],
        "is_answered": [True],
        "view_count": [100],
        "answer_count": [2],
        "score": [10],
        "last_activity_date": [1234567890],
        "creation_date": [1234567890],
        "question_id": [1],
        "content_license": ["exemplo"],
        "title": ["teste"],
        "owner.account_id": [123],
        "owner.reputation": [1000],
        "owner.user_id": [999],
        "owner.user_type": ["exemplo"],
        "owner.display_name": ["exemplo"],
        "last_edit_date": [1234567890],
        "owner.accept_rate": [80]
    })

    resultado = columns(df)
    
    assert "quantidade_visualizacoes" in resultado.columns
    assert "titulo" in resultado.columns
    assert "view_count" not in resultado.columns
         
def test_columns_dataframe_vazio():
    df = pd.DataFrame()
    
    resultado = columns(df)
    
    assert resultado.empty

def test_validar_dados_linhas_invalidas():
    df = pd.DataFrame({
        "titulo" : ["teste"],
        "quantidade_visualizacoes" : [10],
        "id_usuario" : [1]
    })
    
    resultado = validate_data(df)
    
    assert resultado["titulo"].dtype == "str"
    assert resultado["quantidade_visualizacoes"].dtype == "int64"
    assert resultado["id_usuario"].dtype == "int64"
    
def test_validar_dataframe_vazio():
    df = pd.DataFrame()
    
    resultado = validate_data(df)
    
    assert resultado.empty