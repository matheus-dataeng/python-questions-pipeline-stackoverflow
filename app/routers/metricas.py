import logging as log 
from app.dependencies import get_db
from sqlalchemy import text
from fastapi import Depends, HTTPException, APIRouter

logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/metricas/por-ano")

def get_ano(ano: int, db = Depends(get_db)):
    try: 
        query = (
            '''
            SELECT 
                temp.dia,
                temp.mes,
                temp.ano,
                temp.dia_semana,
                perg.titulo,
                COUNT(DISTINCT fat.id_pergunta) AS total_perguntas
            FROM fato_perguntas AS fat
            JOIN dim_tempo AS temp
                ON fat.id_tempo = temp.id_tempo
            JOIN dim_perguntas AS perg
                ON fat.id_pergunta = perg.id_pergunta
            WHERE temp.ano = :ano
            GROUP BY temp.dia, temp.mes, temp.ano, temp.dia_semana, perg.titulo
            
            '''
        )
        
        result = db.execute(text(query), {"ano" : ano})
        metricas_ano = result.mappings().all()
        
        if not metricas_ano:
            logger.error("Registro não encontrado")
            raise HTTPException(status_code= 404, detail=f"Registro de {ano} não encontrado")
        
        logger.info(f"Consulta aos registros de {ano} consultados com sucesso")
        return metricas_ano
    
    except HTTPException:
        raise 
    
    except Exception as e:
        logger.error(f"Erro ao consultar registro de {ano}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar registro de {ano}")

@router.get("/metricas/por-tag")

def get_perguntas_tag(db = Depends(get_db)):
    try:
        query = (
            '''
            SELECT 
                tag.tags,
                COUNT(DISTINCT bri.id_pergunta) AS total_perguntas
            FROM bridge_tags AS bri
            JOIN dim_tags AS tag
                ON bri.id_tags = tag.id_tags
            GROUP BY tag.tags
            '''
        )
        
        result = db.execute(text(query))
        logger.info("Consulta realizada a contagem de perguntas agrupada por tag")
        return result.mappings().all()

    except Exception as e:
        logger.error("Falha ao consultar dados de contagem de perguntas por tag")
        raise HTTPException(status_code= 500, detail="Erro ao consultar dados de contagem de perguntas por tag")
