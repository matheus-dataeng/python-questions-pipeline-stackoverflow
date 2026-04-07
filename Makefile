# Definição do ambiente virtual
VENV_NAME=analise_programacao
PYTHON_BIN=python3
PYTHON=$(VENV_NAME)/bin/python
PIP=$(VENV_NAME)/bin/pip
UVICORN=$(VENV_NAME)/bin/uvicorn

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

# Rodar o Pipeline
run-pipeline:
	$(PYTHON) src/main.py

# Rodar API
run-api:
	$(UVICORN) app.main:app --reload --port 8000

# Remove arquivos temporários do Python
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete