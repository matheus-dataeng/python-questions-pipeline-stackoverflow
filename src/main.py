import logging as log 
from utils.logger_config import setup_log
from bronze.extract import extract, load_bronze_datalake
from silver.transform import transform, load_silver_datalake
from gold.build_metrics import build_metrics, load_gold_datalake
from gold.load import load

logger = log.getLogger(__name__)
setup_log()

def main() -> None:
    try:
        logger.info("Iniciando Pipeline")
        
        df_bronze = extract()
        load_bronze_datalake(df_bronze)
        
        df_silver = transform(df_bronze)
        load_silver_datalake(df_silver)
        
        metrics = build_metrics(df_silver)
        load_gold_datalake(metrics)
        
        load(
            metrics["dim_usuario"],
            metrics["dim_tempo"],
            metrics["dim_tags"],
            metrics["dim_perguntas"],
            metrics["fato_perguntas"],
            metrics["bridge_tags"]
        )
        
        logger.info("Pipeline finalizado")
    
    except Exception as e:
        logger.error(f"Erro no pipeline: {e}")
        raise
    
if __name__ == "__main__":
    main()
    