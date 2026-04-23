from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 
import logging as log 

logger = log.getLogger(__name__)
load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

if not all([user, password, host, port, dbname]):
    logger.error("Variaveis não definidas no .env")
    raise ValueError("Variaveis não definidas no .env")

try:
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(url)
    SessionLocal = sessionmaker(autocommit= False, autoflush= False, bind= engine)
    logger.info("Conexão estabelecida ✅")

except Exception as e:
    logger.error(f"Erro na conexão :/") 