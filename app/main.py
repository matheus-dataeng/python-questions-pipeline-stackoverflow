import logging as log 
from fastapi import FastAPI
from src.utils.logger_config import setup_log
from app.routers.perguntas import router as perguntas_router
from app.routers.tags import router as tags_router
from app.routers.usuarios import router as usuarios_router
from app.routers.metricas import router as metricas_router

setup_log()
logger = log.getLogger(__name__)

app = FastAPI(
    title="StackOverflow Analytics API",
    description="Pipeline de dados que coleta perguntas sobre Python do Stack Overflow e expõe métricas via API REST",
    version="1.0.0"
)

app.include_router(
    perguntas_router,
    tags=["Perguntas ❓"]
)

app.include_router(
    tags_router,
    tags=["Tags 🏷️"]
)

app.include_router(
    usuarios_router,
    tags=["Usuarios 👤"]
)

app.include_router(
    metricas_router,
    tags=["Metricas 📊"]
)