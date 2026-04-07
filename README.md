# 🐍 Python Stack Overflow — Analytics Pipeline

Pipeline ETL que coleta perguntas sobre Python do Stack Overflow, processa os dados em um data lake com arquitetura bronze/silver/gold e carrega um modelo estrela no PostgreSQL para análise via API REST.

---

## 🏗️ Arquitetura

```
Stack Overflow API
       ↓
  [ Bronze ]  →  dado bruto em Parquet
       ↓
  [ Silver ]  →  dado limpo e tratado em Parquet
       ↓
  [ Gold ]    →  star schema em Parquet + PostgreSQL
       ↓
  [ FastAPI ] →  endpoints REST para consumo
```

---

## 🗂️ Estrutura do Projeto

```
├── app/
│   ├── db/
│   │   └── database.py         # Conexão com o banco
│   ├── routers/
│   │   ├── perguntas.py        # Rotas de perguntas
│   │   ├── tags.py             # Rotas de tags
│   │   ├── usuarios.py         # Rotas de usuários
│   │   └── metricas.py         # Rotas de métricas
│   ├── dependencies.py         # Injeção de dependência (get_db)
│   └── main.py                 # Inicialização da API
├── src/
│   ├── bronze/
│   │   └── extract.py          # Extração da API e carga bronze
│   ├── silver/
│   │   └── transform.py        # Transformação e carga silver
│   ├── gold/
│   │   ├── build_metrics.py    # Modelagem dimensional e carga gold
│   │   └── load.py             # Carga no PostgreSQL
│   ├── utils/
│   │   └── logger_config.py    # Configuração de logs
│   └── main.py                 # Orquestrador do pipeline
├── sql/
│   └── script_tabelas.sql      # DDL das tabelas no PostgreSQL
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── .env.example
├── requirements.txt
└── Makefile
```

---

## 🗃️ Modelagem do Banco

Modelo estrela com bridge table para relação muitos-para-muitos entre perguntas e tags.

```
dim_usuario     → dados do usuário que fez a pergunta
dim_tempo       → data e hora da pergunta
dim_tags        → tecnologias relacionadas à pergunta
dim_perguntas   → título e licença da pergunta
fato_perguntas  → métricas: visualizações, respostas, pontuação
bridge_tags     → relacionamento entre perguntas e tags
```

---

## 🚀 Como Rodar o Projeto

### Pré-requisitos

- Python 3.10+
- PostgreSQL 

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/stackoverflow-analytics-pipeline
cd stackoverflow-analytics-pipeline
```

### 2. Crie e ative o ambiente virtual

```bash
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Configure o `.env`

```bash
cp .env.example .env
```

Preencha o `.env` com suas credenciais:

```dotenv
# URL da API
API_URL=https://api.stackexchange.com/2.3/questions/unanswered?order=desc&sort=activity&tagged=python&site=stackoverflow

# Credenciais do banco
DB_USER=postgres
PASSWORD=sua_senha
HOST=localhost
PORT=5432
DBNAME=nome_banco

# Tabelas
TABLE_DIM_USUARIOS=dim_usuario
TABLE_DIM_TEMPO=dim_tempo
TABLE_DIM_TAGS=dim_tags
TABLE_DIM_PERGUNTAS=dim_perguntas
TABLE_FATO_PERGUNTAS=fato_perguntas
TABLE_BRIDGE_TAGS=bridge_tags
```

### 5. Crie as tabelas no banco

Execute o script SQL no seu PostgreSQL:

```bash
psql -h localhost -U postgres -d stackoverflow_dw -f sql/script_tabelas.sql
```

### 6. Rode o pipeline

```bash
python src/main.py
```

### 7. Suba a API

```bash
uvicorn app.main:app --reload
```

---

## 🔌 Endpoints da API

### Perguntas
| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/perguntas` | Lista todas as perguntas |
| GET | `/perguntas/sem-resposta` | Perguntas sem nenhuma resposta |
| GET | `/perguntas/mais-vistas` | Perguntas ordenadas por visualizações |

### Tags
| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/tags` | Lista todas as tags |
| GET | `/tags/mais-frequentes` | Tags com mais perguntas associadas |

### Usuários
| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/usuarios` | Lista todos os usuários |
| GET | `/usuarios/mais-ativos` | Usuários com mais perguntas |

### Métricas
| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/metricas/por-ano` | Volume de perguntas por ano |
| GET | `/metricas/por-tag` | Contagem de perguntas agrupada por tag |

A documentação interativa da API está disponível em `http://localhost:8000/docs` após subir a aplicação.

---

## 🛠️ Stack

- **Python** — linguagem principal
- **Pandas** — transformação dos dados
- **FastAPI** — API REST
- **SQLAlchemy** — ORM e conexão com banco
- **PostgreSQL** — banco de dados
- **Parquet** — armazenamento em cada camada do data lake
- **python-dotenv** — gerenciamento de variáveis de ambiente

---

## 🔮 Próximos Passos

- [ ] Migrar data lake local para **AWS S3**
- [ ] Migrar banco para **AWS RDS**
- [ ] Deploy da API com **AWS API Gateway**
- [ ] Orquestrar pipeline com **Apache Airflow**
- [ ] Frontend para consumo da API

---

## 👥 Autores

Desenvolvido por **Matheus Meneses** e **Gabriel Sena**.
