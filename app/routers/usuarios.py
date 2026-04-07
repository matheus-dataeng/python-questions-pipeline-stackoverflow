import logging as log 
from app.dependencies import get_db
from fastapi import Depends, HTTPException, APIRouter
from sqlalchemy import text 

logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/usuarios")

def get_usuarios(db = Depends(get_db)):
    try:
        result = db.execute(text("SELECT * FROM dim_usuario"))
        logger.info("Consulta realizada na tabela dim_usuario")
        return result.mappings().all()
    
    except Exception as e:
        logger.error(f"Falha ao consultar dados na tabela dim_usuario: {e}")
        raise HTTPException(status_code= 500, detail="Erro ao consultar dados na tabela dim_usuario")

@router.get("/usuarios/mais-ativos")

def get_usuarios_ativos(db = Depends(get_db)):
    try:
        query = (
            '''
            SELECT 
                use.nome_usuario,
                use.tipo_usuario,
	            COUNT(DISTINCT fat.id_pergunta) AS total_pergunta
            FROM dim_usuario AS use
            JOIN fato_perguntas AS fat
	            ON fat.id_usuario = use.id_usuario
            GROUP BY use.nome_usuario, use.tipo_usuario
            ORDER BY total_pergunta DESC  
            '''
        )
        
        result = db.execute(text(query))
        logger.info("Consulta realizada a usuarios mais ativos")
        return result.mappings().all()
    
    except Exception as e:
        logger.error(f"Erro ao consultar usuarios mais ativos: {e}")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar usuarios mais ativos")