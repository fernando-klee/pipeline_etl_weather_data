# 🌤️ Pipeline ETL de Dados Meteorológicos

Pipeline ETL completo para extração, transformação e carga de dados meteorológicos, utilizando **Apache Airflow** para orquestração, **Python** para processamento e **PostgreSQL** como destino final.

---

## 📋 Sumário

- [Sobre o Projeto](#-sobre-o-projeto)
- [Arquitetura](#-arquitetura)
- [Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [Pré-requisitos](#-pré-requisitos)
- [Configuração do Ambiente](#️-configuração-do-ambiente)
  - [1. Clonar o Repositório](#1-clonar-o-repositório)
  - [2. Configurar o PostgreSQL](#2-configurar-o-postgresql)
  - [3. Configurar o OpenWeatherMap](#3-configurar-o-openweathermap)
  - [4. Configurar o Airflow com Docker](#4-configurar-o-airflow-com-docker)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Como Executar](#️-como-executar)
- [Detalhes do Pipeline](#-detalhes-do-pipeline)
- [Variáveis de Ambiente](#-variáveis-de-ambiente)
- [Troubleshooting](#-troubleshooting)
- [Aprendizados](#-aprendizados)
- [Licença](#-licença)

---

## 🎯 Sobre o Projeto

Este projeto implementa um **pipeline ETL (Extract, Transform, Load)** que consome dados meteorológicos da API [OpenWeatherMap](https://openweathermap.org/), realiza a transformação e limpeza dos dados com **Pandas**, e persiste as informações em um banco **PostgreSQL**.

O pipeline é orquestrado pelo **Apache Airflow** rodando em containers Docker, utilizando a **TaskFlow API** (`@task`) para definição das DAGs.

### Objetivos de aprendizado

- ✅ Estruturar um pipeline ETL modular e de fácil manutenção
- ✅ Orquestrar tarefas com Apache Airflow (TaskFlow API)
- ✅ Containerizar o ambiente com Docker Compose
- ✅ Trabalhar com APIs externas e tratamento de JSON
- ✅ Transformar dados com Pandas
- ✅ Persistir dados em PostgreSQL com SQLAlchemy

---

## 🏗️ Arquitetura

```
┌────────────────────┐
│  OpenWeatherMap    │
│       API          │
└─────────┬──────────┘
          │  (JSON)
          ▼
┌────────────────────┐
│   EXTRACT          │  → Requisição HTTP
│   (Python/Requests)│
└─────────┬──────────┘
          ▼
┌────────────────────┐
│   TRANSFORM        │  → Limpeza, normalização
│   (Pandas)         │     e conversão de tipos
└─────────┬──────────┘
          ▼
┌────────────────────┐
│   LOAD             │  → SQLAlchemy + psycopg2
│   (PostgreSQL)     │
└────────────────────┘

     Orquestrado por Apache Airflow (Docker)
```

---

## 🛠️ Tecnologias Utilizadas

| Camada            | Tecnologia                          |
|-------------------|-------------------------------------|
| Linguagem         | Python 3.12+                        |
| Orquestração      | Apache Airflow 3.1.7                |
| Containerização   | Docker + Docker Compose             |
| Banco de Dados    | PostgreSQL 14+                      |
| Manipulação Dados | Pandas                              |
| Conexão DB        | SQLAlchemy + psycopg2               |
| Gerenciador deps  | UV                                  |
| API Externa       | OpenWeatherMap                      |
| Editor            | VS Code                             |

---

## ✅ Pré-requisitos

Antes de começar, garanta que você possui instalado em sua máquina:

- **Python 3.12+** — [Download](https://www.python.org/downloads/)
- **UV** (gerenciador de pacotes) — [Instalação](https://docs.astral.sh/uv/getting-started/installation/)
- **PostgreSQL 14+** — via [Homebrew](https://brew.sh/) (macOS) ou instalador oficial
- **Docker Desktop** — [Download](https://www.docker.com/products/docker-desktop/)
- **VS Code** com extensões:
  - Python
  - Docker
- **Conta no OpenWeatherMap** com API Key ativa — [Criar conta](https://home.openweathermap.org/users/sign_up)

---

## ⚙️ Configuração do Ambiente

### 1. Clonar o Repositório

```bash
git clone https://github.com/<seu-usuario>/<seu-repo>.git
cd <seu-repo>
```

### 2. Configurar o PostgreSQL

Acesse o PostgreSQL local e crie a role, o banco e conceda os privilégios:

```sql
-- Criar usuário dedicado ao pipeline
CREATE ROLE weather_user WITH LOGIN PASSWORD 'sua_senha_forte';

-- Criar banco de dados
CREATE DATABASE weather_data OWNER weather_user;

-- Conceder todos os privilégios
GRANT ALL PRIVILEGES ON DATABASE weather_data TO weather_user;

-- Conectar ao banco e conceder privilégios no schema
\c weather_data
GRANT ALL ON SCHEMA public TO weather_user;
```

### 3. Configurar o OpenWeatherMap

1. Crie uma conta em [openweathermap.org](https://openweathermap.org/)
2. Acesse **API Keys** no painel de usuário
3. Gere uma nova chave e aguarde a ativação (pode levar alguns minutos)

### 4. Configurar o Airflow com Docker

Baixe o arquivo oficial do Docker Compose:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.7/docker-compose.yaml'
```

Crie os diretórios necessários e configure as variáveis de ambiente:

```bash
mkdir -p ./dags ./logs ./plugins ./config ./src ./data
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Crie um arquivo `.env` com suas credenciais (veja a seção [Variáveis de Ambiente](#-variáveis-de-ambiente)).

Inicialize o Airflow:

```bash
docker compose up airflow-init
```

Suba os serviços:

```bash
docker compose up -d
```

Acesse a UI do Airflow em: **http://localhost:8080**  
Usuário e senha padrão: `airflow` / `airflow`

---

## 📁 Estrutura do Projeto

```
.
├── dags/
│   └── weather_dag.py           # Definição da DAG (TaskFlow API)
├── src/
│   ├── extract.py               # Extração de dados da API
│   ├── transform.py             # Transformação com Pandas
│   └── load.py                  # Carga no PostgreSQL
├── data/                        # Dados temporários (opcional)
├── config/
├── logs/
├── plugins/
├── docker-compose.yaml
├── pyproject.toml               # Dependências (UV)
├── .env                         # Variáveis de ambiente (não versionar!)
├── .gitignore
└── README.md
```

---

## ▶️ Como Executar

### Execução via Airflow

1. Acesse a UI do Airflow: **http://localhost:8080**
2. Localize a DAG `weather_pipeline`
3. Ative a DAG (toggle no canto esquerdo)
4. Clique em **Trigger DAG** para executar manualmente
5. Acompanhe os logs de cada task (extract → transform → load)

### Execução local (sem Airflow)

```bash
# Instalar dependências
uv sync

# Executar pipeline manualmente
python -m src.extract
python -m src.transform
python -m src.load
```

### Verificar os dados no PostgreSQL

```bash
psql -U weather_user -d weather_data -c "SELECT * FROM weather LIMIT 10;"
```

---

## 🔄 Detalhes do Pipeline

A DAG é definida utilizando a **TaskFlow API**:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="weather_pipeline",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "weather"],
)
def weather_pipeline():

    @task
    def extract():
        # Requisição à API OpenWeatherMap
        ...

    @task
    def transform(raw_data):
        # Limpeza com Pandas
        ...

    @task
    def load(df):
        # Carga no PostgreSQL
        ...

    raw = extract()
    transformed = transform(raw)
    load(transformed)

weather_pipeline()
```

### Etapas

| Etapa         | Descrição                                                                    |
|---------------|------------------------------------------------------------------------------|
| **Extract**   | Requisição HTTP à API do OpenWeatherMap retornando JSON com dados climáticos |
| **Transform** | Uso do Pandas para normalizar JSON, tratar nulos e converter tipos           |
| **Load**      | Ingestão no PostgreSQL via SQLAlchemy + psycopg2                             |

---

## 🔐 Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com base no exemplo abaixo:

```env
# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# OpenWeatherMap
OPENWEATHER_API_KEY=sua_chave_api_aqui
OPENWEATHER_CITY=Sao Paulo
OPENWEATHER_UNITS=metric

# PostgreSQL
POSTGRES_USER=weather_user
POSTGRES_PASSWORD=sua_senha_forte
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=weather_data
```

> ⚠️ **Importante:** adicione `.env` ao seu `.gitignore` para não vazar credenciais.

---

## 🐛 Troubleshooting

**Erro de conexão com o PostgreSQL dentro do container**  
Use `host.docker.internal` em vez de `localhost` no `POSTGRES_HOST`.

**API Key retorna 401**  
Chaves recém-criadas no OpenWeatherMap podem demorar até 10 minutos para ativar.

**Airflow não reconhece a DAG**  
Verifique se a pasta `dags/` está corretamente mapeada no `docker-compose.yaml` e se não há erros de importação nos logs.

**Permissão negada no `logs/`**  
Execute `sudo chown -R $(id -u):$(id -g) logs/` ou ajuste o `AIRFLOW_UID` no `.env`.

---

## 🎓 Aprendizados

Durante este projeto foram consolidados os seguintes conhecimentos:

- Construção de pipelines ETL modulares em Python
- Orquestração com Airflow usando boas práticas (TaskFlow API)
- Containerização de ambientes com Docker Compose
- Integração com APIs REST externas
- Modelagem e carga de dados em PostgreSQL
- Gerenciamento de dependências com UV

---

## 📄 Licença

Este projeto é de uso livre para fins educacionais. Sinta-se à vontade para fork, estudar e adaptar.

---


<p align="center">Feito com ☕ e 🐍 por <a href="https://github.com/fernando-klee">Fernando K</a></p>

---
---

# 📎 Arquivos Auxiliares

## `.gitignore`

```gitignore
# Ambiente virtual
.venv/
venv/
env/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
dist/
*.egg-info/
.eggs/

# UV
uv.lock

# Variáveis de ambiente (NUNCA versionar)
.env
.env.*

# Airflow
logs/
airflow.cfg
airflow.db
airflow-webserver.pid
standalone_admin_password.txt

# Dados temporários
data/
*.csv
*.parquet
*.json

# IDE
.vscode/
.idea/
*.swp
*.swo

# Sistema Operacional
.DS_Store
Thumbs.db

# Docker
*.pid
```

---

## `pyproject.toml`

```toml
[project]
name = "weather-etl-pipeline"
version = "0.1.0"
description = "Pipeline ETL de dados meteorológicos com Airflow, Pandas e PostgreSQL"
readme = "README.md"
requires-python = ">=3.12"
authors = [
    { name = "Seu Nome", email = "seu-email@exemplo.com" }
]
license = { text = "MIT" }
keywords = ["etl", "airflow", "pandas", "postgresql", "weather", "data-engineering"]
classifiers = [
    "Programming Language :: Python :: 3.12",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
]

dependencies = [
    "apache-airflow>=2.9.0",
    "pandas>=2.2.0",
    "requests>=2.32.0",
    "sqlalchemy>=2.0.0",
    "psycopg2-binary>=2.9.9",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "ruff>=0.6.0",
    "pytest>=8.0.0",
    "pytest-cov>=5.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "W", "UP", "B"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
```

---

## `.env.example`

```env
# ================================
# Airflow
# ================================
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# ================================
# OpenWeatherMap
# ================================
OPENWEATHER_API_KEY=sua_chave_api_aqui
OPENWEATHER_CITY=Sao Paulo
OPENWEATHER_UNITS=metric
OPENWEATHER_LANG=pt_br

# ================================
# PostgreSQL
# ================================
POSTGRES_USER=weather_user
POSTGRES_PASSWORD=sua_senha_forte
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=weather_data
```

---

## Estrutura Completa de Arquivos

```
weather-etl-pipeline/
├── dags/
│   ├── __init__.py
│   └── weather_dag.py
├── src/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── data/
│   └── .gitkeep
├── config/
│   └── .gitkeep
├── logs/
│   └── .gitkeep
├── plugins/
│   └── .gitkeep
├── docker-compose.yaml
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

---

<p align="center">Feito com ☕ e 🐍 por <a href="https://github.com/fernando-klee">Fernando K</a></p>
