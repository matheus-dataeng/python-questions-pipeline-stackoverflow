# VARIÁVEIS
VENV_NAME=analise_programacao
PYTHON_BIN=python3
PYTHON=$(VENV_NAME)/bin/python
PIP=$(VENV_NAME)/bin/pip
UVICORN=$(VENV_NAME)/bin/uvicorn

# DOCKER 

# Sobe todos os containers
up:
	docker-compose up

# Builda e sobe
build:
	docker-compose up --build

# Para os containers
down:
	docker-compose down

# Para e remove volumes (reset completo)
down-v:
	docker-compose down -v

# Status dos containers
ps:
	docker-compose ps

# Próximas vezes — só sobe container que ja existe
start:
	docker-compose start

# Para sem destruir
stop:
	docker-compose stop

# LOCAL 

# Cria o ambiente virtual Python
venv:
	$(PYTHON_BIN) -m venv $(VENV_NAME)

# Instala dependências
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

# Atualiza requirements.txt
freeze:
	$(PIP) freeze > requirements.txt

# Roda o pipeline localmente
run-pipeline:
	$(PYTHON) src/main.py

# Sobe a API localmente
run-api:
	$(UVICORN) app.main:app --reload --port 8000

# Remove arquivos temporários do Python
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete