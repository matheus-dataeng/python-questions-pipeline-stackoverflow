import logging as log 
from sqlalchemy import text 
from fastapi import APIRouter, HTTPException, Depends
from app.dependencies import get_db


logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/perguntas")

def perguntas_geral(db= Depends(get_db)):
    
    try:
        result = db.execute(text("SELECT * FROM dim_perguntas"))
        logger.info("Consulta realizada na tabela dim_tempo")
        return result.mappings().all()
    
    except Exception as e:
        logger.error(f"Erro ao consultar tabela dim_perguntas: {e}")
        raise HTTPException(status_code=500, detail="Erro ao consultar dados")
        

@router.get("/perguntas/sem-resposta")

def perguntas_sem_reposta(db= Depends(get_db)):
    
    try:
        query = (
            '''
            SELECT 
                perg.id_pergunta,
                perg.titulo,
                fat.quantidade_respostas
            FROM fato_perguntas AS fat
            JOIN dim_perguntas AS perg
	            ON perg.id_pergunta = fat.id_pergunta
            WHERE fat.quantidade_respostas = 0
            '''
        )

        result = db.execute(text(query))
        logger.info("Consulta realizada a perguntas sem reposta na tabela dim_perguntas")
        return result.mappings().all()
    
    except Exception as e:
        logger.error("Erro ao 'consultar perguntas sem repostas'")
        raise HTTPException(status_code=500, detail="Erro ao consultar 'perguntas sem repostas'")

        
@router.get("/perguntas/mais-vistas")

def perguntas_mais_vistas(db= Depends(get_db)):
    
    try:
        query = (
        '''
        SELECT 
            perg.id_pergunta,
            perg.titulo,
            fat.quantidade_visualizacoes
        FROM fato_perguntas AS fat
        JOIN dim_perguntas AS perg
	        ON perg.id_pergunta = fat.id_pergunta
        ORDER BY fat.quantidade_visualizacoes DESC
        
        '''
        )
        
        result = db.execute(text(query))
        logger.info("Consulta realizada a perguntas mais visualizadas")
        return result.mappings().all()
    
    except Exception as e:
        logger.error("Erro ao consultar 'perguntas mais visualizadas'")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar 'perguntas mais visualizadas'")

