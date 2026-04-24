from src.gold.build_metrics import usuario, tempo, tags, perguntas, bridge_tags, fato 
import pytest 
import pandas as pd

def test_usuario():
    df = pd.DataFrame({
        "id_usuario" : [1, 1],
        "id_conta_usuario" : [2, 2],
        "nome_usuario" : ["nome" ,"nome"],
        "tipo_usuario" : ["exemplo", "exemplo"],
        "reputacao_usuario" : [100, 100],
        "taxa_aceitacao_usuario" : [70, 70]
    })
    
    resultado = usuario(df)
    
    assert not resultado.empty
    assert len(resultado) == 1

    assert list(resultado.columns) == [
        "id_usuario",
        "id_conta_usuario",
        "nome_usuario",
        "tipo_usuario",
        "reputacao_usuario",
        "taxa_aceitacao_usuario"
    ]

def test_tempo():
    df = pd.DataFrame({
        "data_criacao" : pd.to_datetime([
            "2025-01-02 20:00:00",
            "2025-01-02 20:00:00",
            "2025-01-10 20:35:00"
        ])
    })
    
    resultado = tempo(df)
    
    assert not resultado.empty
    assert len(resultado) == 2

    assert list(resultado.columns) == [
        "id_tempo",
        "data_criacao",
        "ano",
        "mes",
        "dia",
        "hora",
        "dia_semana"
    ]
    
def test_tags():
    df = pd.DataFrame({
        "id_tags" : [1, 1],
        "tags" : [
            ["python", "pandas", "query"],
            ["python", "sql", "numpy"]
        ]
    })  
    
    resultado = tags(df)
    
    #garante que não esta vazio
    assert not resultado.empty
    
    #remove duplicados(python aparece 2x)
    assert len(resultado) == 5
    
    #colunas corretas
    assert list(resultado.columns) == ["id_tags", "tags"]
    
    #valores esperados
    assert set(resultado["tags"]) == {"python", "pandas", "query", "sql", "numpy"}
    
    #ids sequencia
    assert list(resultado["id_tags"]) == [1, 2, 3, 4, 5]
    
def test_bridge_tags():
    df = pd.DataFrame({
        "id_pergunta" : [1, 2],
        "tags": [
            ["python", "sql"],
            ["pandas", "numpy"],
            
        ]     
    })
    
    df_tags = pd.DataFrame({
        "id_tags" : [1, 2, 3, 4],
        "tags" : ["python", "sql", "pandas", "numpy"]
    })
    
    resultado = bridge_tags(df, df_tags)
    
    ids = {
    (1, 1),
    (1, 2),
    (2, 3),
    (2, 4)
}

    resultado_set = set(zip(resultado["id_pergunta"], resultado["id_tags"]))

    assert resultado_set == ids    
    
    assert not resultado.empty
    assert len(resultado) == 4
    assert list(resultado.columns) == ["id_pergunta", "id_tags"]
    
def test_perguntas():
    df = pd.DataFrame({
        "id_pergunta" : [1, 1, 2],
        "titulo" : ["titulo", "titulo", "subtitulo"],
        "licenca_conteudo" : ["exemplo", "exemplo", "ok"]
    })
    
    resultado = perguntas(df)
    
    assert not resultado.empty 
    assert len(resultado) == 2
    
    assert list(resultado.columns) == [
        "id_pergunta",
        "titulo",
        "licenca_conteudo"
    ]
    
    assert set(resultado["id_pergunta"]) == {1, 2}
    assert set(resultado["titulo"]) == {"titulo", "subtitulo"}

def test_fato():
    df = pd.DataFrame({
        "id_pergunta" : [1, 1],
        "id_usuario" : [1, 1],
        "data_criacao" : pd.to_datetime(["2025-01-05", "2025-01-05"]),
        "quantidade_visualizacoes" : [1, 1],
        "quantidade_respostas" : [1, 1],
        "pontuacao" : [1, 1],
        "respondida" : [False, False]
    })
    
    df_usuarios = pd.DataFrame({
        "id_usuario" : [1],
        "nome_usuario" : ["nome"]
    })
    
    df_tempo = pd.DataFrame({
        "id_tempo" : [1],
        "data_criacao" : pd.to_datetime(["2025-01-05"])
    })
    
    df_perguntas = pd.DataFrame({
        "id_pergunta" : [1],
        "licenca_conteudo" : ["CC BY-SA 4.0"]
    })
    
    resultado = fato(df, df_usuarios, df_tempo, df_perguntas)
    
    assert not resultado.empty
    assert len(resultado) == 1
    assert list(resultado.columns) == [
        "id_pergunta",
        "id_usuario",
        "id_tempo",
        "quantidade_visualizacoes",
        "quantidade_respostas",
        "pontuacao",
        "respondida"
    ]