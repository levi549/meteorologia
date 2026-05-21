# 🌩️ Meteorologia — Plataforma de Análise e Previsão de Anomalias Climáticas

> Pipeline ELT orientado a objetos com Arquitetura Medallion, Machine Learning e Agente de IA para monitoramento, classificação e previsão de anomalias climáticas com cálculo de impacto populacional.

---

## 📋 Sumário

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
  - [Arquitetura Medallion](#arquitetura-medallion-camadas-de-processamento)
  - [Fluxo de Dados](#fluxo-de-dados-end-to-end)
  - [Componentes Principais](#componentes-principais)
- [Estrutura de Pastas](#-estrutura-de-pastas)
- [Stack Tecnológica](#-stack-tecnológica)
- [Pipeline ELT](#-pipeline-elt)
- [Machine Learning](#-machine-learning)
- [Agente de IA](#-agente-de-ia)
- [Instalação](#-instalação)
- [Configuração](#-configuração)

---

## 🌐 Visão Geral

Este projeto é uma plataforma de ponta a ponta para monitoramento e análise de anomalias climáticas. Ele integra múltiplas fontes de dados meteorológicos, processa-os através de uma pipeline ELT estruturada com **Arquitetura Medallion**, aplica modelos de **Machine Learning** para classificação de alertas e previsão de eventos, calcula o **impacto populacional** de anomalias climáticas e expõe tudo via uma interface interativa com um **Agente de IA**.

### Capacidades Principais

- Ingestão automatizada de dados de múltiplas fontes via Airflow
- Processamento em camadas (Raw → Silver → Gold) com dbt
- Rotulação de dados via clustering ML (3 níveis de alerta)
- Cálculo matemático de impacto populacional por anomalia
- Previsão de anomalias climáticas e gravidade com ML supervisionado
- Interface gráfica com Agente de IA para consultas em linguagem natural

---

## 🏛️ Arquitetura

### Arquitetura Medallion — Camadas de Processamento

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                              │
│  [ API OpenWeather ]  [ Outras APIs ]  [ Arquivos / Streams ]       │
└────────────────────────────┬────────────────────────────────────────┘
                             │  Ingestão via Airflow DAGs
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥉  CAMADA RAW  (Bronze)       dbt/raw/                            │
│  • Dados brutos sem transformação                                   │
│  • Preservação fiel da fonte original                               │
│  • Schema: raw_*                                                    │
└────────────────────────────┬────────────────────────────────────────┘
                             │  Limpeza, normalização, deduplicação
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥈  CAMADA SILVER          dbt/silver_gold/  (models silver)       │
│  • Dados limpos e padronizados                                      │
│  • Tipagem correta, valores nulos tratados                          │
│  • Enriquecimento básico e joins entre fontes                       │
│  • Schema: silver_*                                                 │
└────────────────────────────┬────────────────────────────────────────┘
                             │  Agregação, métricas, features ML
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥇  CAMADA GOLD            dbt/silver_gold/  (models gold)         │
│  • Dados prontos para consumo analítico                             │
│  • Features de ML calculadas                                        │
│  • Métricas de impacto populacional                                 │
│  • Schema: gold_*                                                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
               ┌─────────────┴──────────────┐
               ▼                            ▼
┌──────────────────────────┐  ┌─────────────────────────────────────┐
│   🤖 ML — Clustering     │  │   🤖 ML — Classificação / Previsão  │
│   (Rotulação de alertas) │  │   (Gravidade da anomalia)           │
│                          │  │                                     │
│  • Regressão + 3 Clusters│  │  • Supervised Learning              │
│  • Nível 1: Baixo risco  │  │  • Input: dados OpenWeather         │
│  • Nível 2: Moderado     │  │  • Output: tipo + gravidade         │
│  • Nível 3: Alto risco   │  │                                     │
└──────────────────────────┘  └─────────────────────────────────────┘
               │                            │
               └─────────────┬──────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│           📊 CÁLCULO DE IMPACTO POPULACIONAL                        │
│                                                                     │
│   Impacto = f(área_anomalia, densidade_pop, nível_alerta, gravidade)│
│                                                                     │
│  • Cruza dados climáticos com dados populacionais                  │
│  • Estima população afetada por região/município                   │
│  • Gera score de criticidade por zona geográfica                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              🧠 AGENTE DE IA  (Interface Gráfica)                   │
│                                                                     │
│  • Consome os dados Gold + resultados ML                           │
│  • Responde perguntas em linguagem natural                         │
│  • Aciona a pipeline de previsão via API OpenWeather               │
│  • Exibe alertas, gravidade e impacto ao usuário                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Fluxo de Dados End-to-End

```
Fontes Externas
     │
     │  HTTP / Streams
     ▼
┌─────────────┐     ┌──────────────────────────────────────────┐
│   Airflow   │────▶│  DAG: ingestão → raw → silver → gold     │
│   (Orquest.)│     │  Agendamento, retry, monitoramento       │
└─────────────┘     └──────────────────┬───────────────────────┘
                                       │ dbt run / dbt test
                                       ▼
                           ┌──────────────────────┐
                           │    Data Warehouse     │
                           │  raw / silver / gold  │
                           └──────────┬───────────┘
                                      │
                    ┌─────────────────┼──────────────────┐
                    ▼                 ▼                   ▼
             ┌──────────┐    ┌──────────────┐   ┌──────────────────┐
             │Clustering│    │  Previsão ML │   │Impacto Popul.    │
             │3 alertas │    │  (OpenWeather│   │(fórmula matemát.)│
             └──────────┘    │   API input) │   └──────────────────┘
                             └──────────────┘
                                      │
                                      ▼
                          ┌───────────────────────┐
                          │    Agente de IA        │
                          │  Interface Gráfica     │
                          │  Linguagem Natural     │
                          └───────────────────────┘
```

---

### Componentes Principais

| Componente | Responsabilidade | Tecnologia |
|---|---|---|
| **Ingestão** | Coleta dados de múltiplas fontes externas | Airflow DAGs + Python (OOP) |
| **Transformação Raw→Silver** | Limpeza, normalização e tipagem | dbt (models `raw/`) |
| **Transformação Silver→Gold** | Agregações, features e métricas | dbt (models `silver_gold/`) |
| **Clustering ML** | Rotula dados em 3 níveis de alerta | Python (Regressão + K-Means) |
| **Previsão ML** | Prevê tipo e gravidade da anomalia | Python (Supervisionado) |
| **Impacto Populacional** | Calcula população afetada | Python (fórmula matemática) |
| **Agente de IA** | Interface conversacional com o sistema | Python + LLM + UI |
| **Orquestração** | Agenda e monitora toda a pipeline | Apache Airflow |

---

## 📁 Estrutura de Pastas

```
meteorologia/
│
├── .venv/                        # Ambiente virtual Python
│
├── dags/                         # DAGs do Apache Airflow
│   └── *.py                      # Definição de pipelines e agendamentos
│
├── data/                         # Dados locais / artefatos intermediários
│
├── dbt/                          # Projeto dbt — transformações SQL
│   ├── raw/                      # 🥉 Modelos da camada Bronze (dados brutos)
│   └── silver_gold/              # 🥈🥇 Modelos Silver (limpeza) e Gold (analítico)
│
├── dbt_packages/                 # Dependências do dbt (dbt_utils, etc.)
│
├── github/
│   └── workflow/                 # CI/CD — GitHub Actions
│
├── logs/                         # Logs de execução (Airflow, pipeline)
│
├── src/                          # Código-fonte principal
│   ├── __pycache__/
│   └── class_file.py             # Classes Python (OOP) — entidades do domínio
│
├── .env                          # Variáveis de ambiente (não versionado)
├── .gitignore
├── .python-version               # Versão do Python (gerenciada pelo uv)
├── dbt_project.yml               # Configuração do projeto dbt
├── main.py                       # Entrypoint principal da aplicação
├── pyproject.toml                # Configuração do projeto Python (uv/pip)
├── README.md
└── uv.lock                       # Lock file de dependências (uv)
```

---

## 🛠️ Stack Tecnológica

| Categoria | Tecnologia | Função |
|---|---|---|
| **Orquestração** | Apache Airflow | Agendamento e monitoramento da pipeline ELT |
| **Transformação** | dbt (Data Build Tool) | Modelagem SQL em camadas Medallion |
| **Linguagem** | Python 3.x | Pipeline, OOP, ML, Agente |
| **ML — Clustering** | scikit-learn | Rotulação de alertas (3 clusters) |
| **ML — Previsão** | scikit-learn / outro | Previsão de anomalia e gravidade |
| **API Climática** | OpenWeather API | Dados meteorológicos em tempo real |
| **Agente IA** | LLM + Python | Interface conversacional |
| **Gerenc. Deps.** | uv | Gerenciamento rápido de dependências Python |
| **CI/CD** | GitHub Actions | Automação de testes e deploy |

---

## ⚙️ Pipeline ELT

A pipeline segue o padrão **ELT (Extract → Load → Transform)** com Programação Orientada a Objetos:

### 1. Extract (Extração)
- Classes em `src/class_file.py` encapsulam conectores para cada fonte
- Airflow DAGs disparam as extrações em horários configurados
- Dados brutos são persistidos na camada **Raw** sem modificação

### 2. Load (Carga)
- Dados extraídos são carregados diretamente no Data Warehouse
- Preservação do schema original para rastreabilidade

### 3. Transform (Transformação via dbt)
- **Raw → Silver:** limpeza, cast de tipos, tratamento de nulos, deduplicação
- **Silver → Gold:** joins entre fontes, cálculo de features, métricas agregadas
- Testes de qualidade de dados embutidos nos modelos dbt

---

## 🤖 Machine Learning

### Modelo 1 — Clustering para Rotulação de Alertas

Utiliza regressão combinada com agrupamento para classificar automaticamente os registros climáticos em **3 níveis de alerta**:

| Cluster | Nível | Descrição |
|---|---|---|
| 0 | 🟢 Baixo | Condições normais ou variações leves |
| 1 | 🟡 Moderado | Anomalia detectada, atenção recomendada |
| 2 | 🔴 Alto | Anomalia severa, alerta ativo |

### Modelo 2 — Previsão de Anomalias

Modelo supervisionado alimentado pelos dados da **API OpenWeather** que:
- Identifica o **tipo de anomalia** climática em curso
- Estima a **gravidade** com base nos padrões históricos
- É acionado pelo Agente de IA sob demanda

---

## 📐 Cálculo de Impacto Populacional

Após a classificação ML, é aplicada uma fórmula matemática para estimar o impacto humano da anomalia:

```
Impacto = Σ (área_afetada_i × densidade_populacional_i × fator_alerta × fator_gravidade)
```

onde:
- `área_afetada_i` — extensão geográfica da anomalia por zona
- `densidade_populacional_i` — habitantes por km² na região
- `fator_alerta` — peso derivado do cluster ML (0 a 1, crescente)
- `fator_gravidade` — score do modelo de previsão (0 a 1, crescente)

O resultado é um **índice de criticidade** por região, usado para priorizar alertas.

---

## 🧠 Agente de IA

O agente é exposto via **interface gráfica interativa** e atua como ponto de entrada para o usuário final:

- Consulta dados da camada Gold em linguagem natural
- Aciona o modelo de previsão com dados da OpenWeather API em tempo real
- Exibe níveis de alerta, previsão de anomalia, gravidade e impacto populacional
- Responde perguntas sobre histórico climático e tendências

```
Usuário
  │
  │  "Qual o risco de chuva intensa em SP esta semana?"
  ▼
┌─────────────────────────────┐
│        Agente de IA         │
│                             │
│  1. Busca dados Gold        │
│  2. Chama API OpenWeather   │
│  3. Executa modelo ML       │
│  4. Calcula impacto pop.    │
│  5. Gera resposta           │
└─────────────────────────────┘
  │
  │  "Alerta Moderado (Cluster 1). Precipitação prevista:
  │   85mm/h. Impacto estimado: 1.2M habitantes na RMSP."
  ▼
Usuário
```

---

## 🚀 Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/meteorologia.git
cd meteorologia

# Instale as dependências com uv
uv sync

# Configure as variáveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais

# Instale os pacotes dbt
dbt deps

# Inicialize o Airflow
airflow db init
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@example.com
```

---

## 🔧 Configuração

Edite o arquivo `.env` com as seguintes variáveis:

```env
# API OpenWeather
OPENWEATHER_API_KEY=sua_chave_aqui

# Data Warehouse
DB_HOST=localhost
DB_PORT=5432
DB_NAME=meteorologia
DB_USER=seu_usuario
DB_PASSWORD=sua_senha

# LLM (Agente de IA)
LLM_API_KEY=sua_chave_aqui
LLM_MODEL=nome_do_modelo

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
```

---

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.