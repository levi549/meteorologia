<div align="center">

# 🌩️ Climate Anomaly Intelligence Platform

**Plataforma end-to-end para monitoramento, análise e previsão de anomalias climáticas**

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.10+-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://getdbt.com)
[![PySpark](https://img.shields.io/badge/PySpark-4.1+-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Supabase](https://img.shields.io/badge/Supabase-Postgres-3ECF8E?style=flat-square&logo=supabase&logoColor=white)](https://supabase.com)

</div>

---

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura do Sistema](#-arquitetura-do-sistema)
- [Medallion Architecture](#-medallion-architecture-camadas-de-dados)
- [Pipeline de Machine Learning](#-pipeline-de-machine-learning)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Tecnologias](#-tecnologias)
- [Como Executar](#-como-executar)
- [Capacidades Principais](#-capacidades-principais)

---

## 🌐 Visão Geral

A **Climate Anomaly Intelligence Platform** é uma solução de dados ponta a ponta para detecção, classificação e previsão de anomalias climáticas. O projeto integra múltiplas fontes de dados meteorológicos, processa-os por uma pipeline ELT com **Arquitetura Medallion**, aplica modelos de **Machine Learning** para rotulação e previsão de alertas, e expõe tudo via uma interface conversacional com um **Agente de IA**.

```
Fontes de Dados → EL (Python/POO) → Supabase Raw → dbt Silver → PySpark KMeans
     → Gold → dbt → ML Supervisionado → LLM Interface
```

---

## 🏗️ Arquitetura do Sistema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORQUESTRAÇÃO — Apache Airflow                       │
└─────────────────────┬─────────────────────┬───────────────────┬────────────┘
                      │                     │                   │
          ┌───────────▼──────────┐          │          ┌────────▼──────────┐
          │   CAMADA DE INGESTÃO │          │          │  CAMADA DE SAÍDA  │
          │   (Extract & Load)   │          │          │   (Serving)       │
          │                      │          │          │                   │
          │  ┌────────────────┐  │          │          │  ┌─────────────┐  │
          │  │  class_file.py │  │          │          │  │ LLM Chatbot │  │
          │  │  (POO por fonte│  │          │          │  │  + Agente   │  │
          │  │   de dados)    │  │          │          │  │    de IA    │  │
          │  └───────┬────────┘  │          │          │  └──────┬──────┘  │
          │          │           │          │          │         │         │
          │  Múltiplas fontes:   │          │          │  ┌──────▼──────┐  │
          │  · APIs Meteo        │          │          │  │   ML Model  │  │
          │  · OpenWeather API   │          │          │  │(Supervisio- │  │
          │  · Dados históricos  │          │          │  │  nado)      │  │
          └───────────┬──────────┘          │          │  └─────────────┘  │
                      │                     │          └────────────────────┘
                      ▼                     │
          ┌───────────────────────┐         │
          │     SUPABASE          │         │
          │  (PostgreSQL managed) │         │
          │  ┌─────────────────┐  │         │
          │  │  Camada RAW     │◄─┘         │
          │  │  (dados brutos) │            │
          │  └────────┬────────┘            │
          └───────────┼───────────────────┘ │
                      │                     │
    ┌─────────────────▼─────────────────────▼──────────────────────────────┐
    │                   MEDALLION ARCHITECTURE — Processamento              │
    │                                                                       │
    │   RAW ──── dbt ────► SILVER ──── PySpark ────► GOLD ──── dbt ──────► │
    │   (Supabase)         (Silver)    (KMeans)       (Gold)   (Final)      │
    └───────────────────────────────────────────────────────────────────────┘
```

---

## 🥇 Medallion Architecture — Camadas de Dados

A pipeline segue a **Arquitetura Medallion** com três camadas progressivas de qualidade e transformação:

### 🔴 Camada RAW (Bronze)
> **Tecnologia:** Supabase (PostgreSQL) | **Responsável:** Python com POO

- Dados ingeridos diretamente das fontes externas sem transformação
- Cada classe em `src/class_file.py` representa uma fonte de dados (Orientação a Objetos)
- Dados chegam com estrutura original, incluindo nulos, duplicatas e inconsistências
- Armazenados no **Supabase** via `supabase-py`

```python
# Exemplo do padrão POO para ingestão
class WeatherSourceExtractor:
    def extract(self) -> pd.DataFrame: ...
    def load_to_raw(self) -> None: ...
```

---

### 🥈 Camada SILVER
> **Tecnologia:** dbt-postgres | **Localização:** `dbt/silver_gold/`

Transformações aplicadas pelo **dbt** a partir da camada RAW. O objetivo desta camada é entregar dados limpos e com variáveis temporais devidamente codificadas para o job PySpark.

**1. Tratamento de Nulos — Imputação por Mediana**

Valores ausentes nas variáveis numéricas são substituídos pela **mediana** da coluna, estratégia robusta a outliers climáticos que preserva a distribuição sem distorcer os dados extremos:

```sql
-- Exemplo de modelo dbt Silver
SELECT
    COALESCE(temperatura, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temperatura)
                          OVER ()) AS temperatura,
    COALESCE(precipitacao, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY precipitacao)
                           OVER ()) AS precipitacao,
    COALESCE(umidade, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY umidade)
                      OVER ()) AS umidade
FROM {{ ref('raw_weather') }}
```

**2. Encoding Cíclico de Datas**

Mês e outras variáveis temporais são transformados em componentes **seno e cosseno** para que o modelo capture a ciclicidade corretamente — dezembro e janeiro, por exemplo, ficam próximos no espaço vetorial:

```sql
-- Encoding cíclico do mês no dbt
SELECT
    *,
    SIN(2 * PI() * EXTRACT(MONTH FROM data) / 12) AS mes_sin,
    COS(2 * PI() * EXTRACT(MONTH FROM data) / 12) AS mes_cos
FROM imputado
```

> **Por que seno/cosseno?** Uma feature `mês = 12` sem transformação está numericamente longe de `mês = 1`, mas climaticamente são meses adjacentes. O encoding cíclico corrige essa distorção para os modelos de ML.

Esses dados limpos e com datas codificadas servem como input para o **job PySpark**.

---

### 🥇 Camada GOLD
> **Tecnologia:** PySpark 4.1 + dbt-postgres | **Localização:** `pysaprk_jobs/kmeans_job.py`

O job PySpark é responsável por **três etapas em sequência**: feature engineering, clustering KMeans e preparação final do dataset para o treinamento supervisionado.

**Fluxo completo do job PySpark:**

```
Silver (dados limpos + mes_sin/cos)
        │
        ▼
┌─────────────────────────┐
│  1. Feature Engineering │
│  · Z-Score por variável │
│  · Normalização (MinMax │
│    ou Standard Scaler)  │
│  · Montagem do vetor    │
│    de features p/ KMeans│
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│  2. PySpark KMeans      │    ← Distribuído, escalável
│     k = 3 clusters      │
│  = 3 níveis de alerta   │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│  3. Prep. para ML       │
│  · Une cluster label    │
│    aos dados originais  │
│  · Remove colunas       │
│    intermediárias       │
│  · Dataset final        │
│    rotulado e pronto    │
│    para treino superv.  │
└──────────┬──────────────┘
           │
           ▼
  Escrita na Camada GOLD
  (Supabase via PySpark)
           │
           ▼
  dbt finaliza
  (agregações, views,
   cálculo de impacto pop.)
```

**Feature Engineering no job PySpark:**

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# 1. Z-Score distribuído por variável
for col in feature_cols:
    mean_val = df.select(mean(col)).first()[0]
    std_val  = df.select(stddev(col)).first()[0]
    df = df.withColumn(f"{col}_zscore", (col(col) - mean_val) / std_val)

# 2. Montagem do vetor de features
assembler = VectorAssembler(
    inputCols=[f"{c}_zscore" for c in feature_cols] + ["mes_sin", "mes_cos"],
    outputCol="features"
)

# 3. KMeans k=3
kmeans = KMeans(k=3, featuresCol="features", predictionCol="alert_cluster")
model  = kmeans.fit(df_assembled)
df_labeled = model.transform(df_assembled)

# 4. Preparo final para ML supervisionada
df_final = (df_labeled
    .drop("features", *[f"{c}_zscore" for c in feature_cols])
    .withColumnRenamed("alert_cluster", "nivel_alerta"))
```

**Níveis de alerta gerados pelo KMeans:**

| Cluster | Nível | Descrição |
|---|---|---|
| 0 | 🟢 Baixo | Condições climáticas dentro do normal |
| 1 | 🟡 Moderado | Desvios significativos identificados |
| 2 | 🔴 Severo | Anomalia crítica com alto impacto potencial |

---

## 🤖 Pipeline de Machine Learning

```
                     FASE 1: Rotulação (Não-supervisionado)
                     ┌────────────────────────────────────┐
                     │                                    │
  Dados Silver  ───► │  PySpark KMeans (k=3)              │ ──► Dados Rotulados
  (features       │  │  · Z-Score calculado               │     (Gold Layer)
   engineered)    │  │  · Sazonalidade cíclica enc.       │
                  │  │  · 3 clusters = 3 alert levels     │
                  │  └────────────────────────────────────┘
                  │
                  │           FASE 2: Previsão (Supervisionado)
                  │           ┌──────────────────────────────────────┐
                  │           │                                      │
                  └─────────► │  ML Supervisionado (src/ML.py)       │
                              │  · Input: dados rotulados (Gold)     │
                              │  · Target 1: tipo de alerta          │
                              │  · Target 2: nome da anomalia        │
                              │  · Inference: OpenWeather API (live) │
                              └──────────────────┬───────────────────┘
                                                 │
                                                 ▼
                                    ┌─────────────────────────┐
                                    │   Interface LLM         │
                                    │   · Chatbot conversac.  │
                                    │   · Chama OpenWeather   │
                                    │   · Chama ML model      │
                                    │   · Responde em lng.    │
                                    │     natural             │
                                    └─────────────────────────┘
```

### Separação de responsabilidades: Silver × PySpark Job

| Etapa | Onde ocorre | O que faz |
|---|---|---|
| Imputação de nulos | dbt Silver | Substitui `NULL` pela mediana da coluna |
| Encoding cíclico | dbt Silver | Gera `mes_sin` e `mes_cos` a partir da data |
| Z-Score | PySpark Job | Calcula desvio padrão distribuído por variável |
| Normalização | PySpark Job | Padroniza escala para o KMeans convergir |
| Clustering | PySpark Job | KMeans k=3 → rótulo `nivel_alerta` |
| Dataset final | PySpark Job | Remove colunas intermediárias, monta treino supervisionado |

---

## 📁 Estrutura do Projeto

```
climate-anomaly-platform/
│
├── dags/                          # DAGs do Apache Airflow
│   └── *.py                       # Pipelines orquestrados
│
├── data/                          # Dados locais / fixtures
│
├── dbt/                           # Projeto dbt
│   ├── raw/                       # Modelos da camada RAW
│   └── silver_gold/               # Modelos Silver e Gold
│       └── *.sql                  # Transformações e agregações
│
├── dbt_packages/                  # Dependências dbt
│
├── pysaprk_jobs/                  # Jobs PySpark distribuídos
│   └── kmeans_job.py              # Clustering KMeans (k=3)
│
├── src/                           # Código-fonte principal
│   ├── class_file.py              # Classes POO por fonte de dados (EL)
│   └── ML.py                      # Modelos supervisionados
│
├── logs/                          # Logs de execução
│   └── dbt.log
│
├── .env                           # Variáveis de ambiente
├── dbt_project.yml                # Configuração dbt
├── main.py                        # Entrypoint da aplicação
├── pyproject.toml                 # Dependências do projeto
└── README.md
```

---

## 🛠️ Tecnologias

| Camada | Tecnologia | Versão | Função |
|---|---|---|---|
| Orquestração | Apache Airflow | Latest | Agendamento e controle da pipeline |
| Armazenamento | Supabase (PostgreSQL) | `supabase>=2.30.0` | Data Warehouse gerenciado |
| Transformação | dbt-postgres | `>=1.10.0` | ELT — camadas Silver e Gold |
| Clustering | PySpark | `>=4.1.2` | KMeans distribuído (3 alertas) |
| ML Supervisionado | scikit-learn / src/ML.py | — | Classificação de anomalias |
| Ingestão | Python (POO) | 3.11+ | Extract & Load por fonte |
| Config | python-dotenv | `>=1.2.2` | Gestão de variáveis de ambiente |
| APIs externas | requests | `>=2.34.2` | OpenWeather + outras fontes |
| Interface | LLM + Agente de IA | — | Chatbot em linguagem natural |

---

## 🚀 Como Executar

### Pré-requisitos

- Python 3.11+
- Java 11+ (para PySpark)
- Conta Supabase configurada
- Chaves de API: OpenWeather e demais fontes

### Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/climate-anomaly-platform.git
cd climate-anomaly-platform

# Instale as dependências com uv
uv sync

# Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com suas credenciais
```

### Configuração

```bash
# Configure o projeto dbt
cd dbt
dbt deps
dbt debug

# Volte à raiz
cd ..
```

### Executando a Pipeline

```bash
# Execução manual completa
python main.py

# Ou via Airflow (recomendado para produção)
airflow dags trigger climate_pipeline_dag
```

### Execução por etapa

```bash
# 1. Ingestão (Extract & Load)
python src/class_file.py

# 2. Transformação Silver (dbt)
cd dbt && dbt run --select silver_gold

# 3. Clustering PySpark
spark-submit pysaprk_jobs/kmeans_job.py

# 4. Finalização Gold (dbt)
cd dbt && dbt run --select gold_final

# 5. Treinamento do modelo ML
python src/ML.py --mode train

# 6. Interface LLM
python main.py --interface chat
```

---

## ⚡ Capacidades Principais

- **Ingestão multi-fonte automatizada** — classes Python independentes (POO) por fonte, orquestradas via Airflow
- **Pipeline ELT estruturada** — Raw → Silver → Gold com dbt e PySpark
- **Clustering ML distribuído** — KMeans com PySpark gerando 3 níveis de alerta (Baixo / Moderado / Severo)
- **Feature engineering robusto** — Z-Score, encoding cíclico de sazonalidade (`mês_sin/cos`), tratamento de nulos
- **Previsão supervisionada** — modelo treinado nos dados rotulados para prever tipo de alerta e nome da anomalia em tempo real
- **Cálculo de impacto populacional** — estimativa matemática do número de pessoas afetadas por cada anomalia
- **Interface conversacional** — LLM Chatbot com Agente de IA que chama a OpenWeather API + modelo ML para responder em linguagem natural

---

<div align="center">

**Desenvolvido com foco em escalabilidade, reprodutibilidade e qualidade de dados.**

</div>