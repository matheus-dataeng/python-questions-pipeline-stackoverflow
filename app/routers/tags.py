import logging as log 
from fastapi import Depends, HTTPException, APIRouter
from sqlalchemy import text 
from app.dependencies import get_db


logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/tags")

def get_tags(db = Depends(get_db)):
    try:
        result = db.execute(text("SELECT * FROM dim_tags"))
        logger.info("Consulta realizada na tabela dim_tags")
        return result.mappings().all()
    
    except Exception as e:
        logger.error(f"Falha ao consultar dados na tabela dim_tags: {e}")
        raise HTTPException(status_code=500, detail="Erro ao consultar dados na tabela dim_tags")

@router.get("/tags/mais-frequentes")

def get_tags_frequentes(db = Depends(get_db)):
    try:
        query = (
        '''
        SELECT 	
            tag.tags,
            COUNT(DISTINCT bri.id_pergunta) AS total_pergunta
        FROM bridge_tags AS bri
        JOIN dim_tags AS tag
	        ON tag.id_tags = bri.id_tags
        GROUP BY tag.tags
        ORDER BY COUNT(bri.id_pergunta) DESC
        
        '''
        )
        
        result = db.execute(text(query))
        logger.info("Consulta realizada as tags com perguntas mais frequentes")
        return result.mappings().all()
    
    except Exception as e:
        logger.error(f"Falha ao consultar perguntas mais frequentes por tags: {e}")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar dados das perguntas mais frequentes por tags")

