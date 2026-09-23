# ⚡ Modern Data Lakehouse Lab

Um laboratório completo e funcional de **Data Lakehouse Open Source**, integrando as tecnologias mais modernas do ecossistema de dados para armazenamento em tabelas abertas, governança de metadados no estilo Git, processamento distribuído em camadas e consultas analíticas de alta performance com interface executiva interativa.

---

## 🛠️ Stack Tecnológica

- **Storage de Objetos**: [MinIO](https://min.io/) (compatível com Amazon S3)
- **Formato de Tabelas**: [Apache Iceberg](https://iceberg.apache.org/) (transações ACID, Time Travel e evolução de schema)
- **Catálogo de Metadados**: [Project Nessie](https://projectnessie.org/) (Iceberg REST Catalog com versionamento tipo Git: branches, tags e commits)
- **Motor de Processamento**: [Apache Spark](https://spark.apache.org/) 3.x (PySpark + Jupyter Notebook)
- **Motor de Consultas Analíticas**: [Trino](https://trino.io/) (Distributed SQL Query Engine com suporte a HTTP e HTTPS/TLS)
- **Dashboard & Aplicação**: [Streamlit](https://streamlit.io/) + [Plotly](https://plotly.com/) (com formatação pt-BR e console SQL interativo)
- **Consumo SQL / BI**: [DBeaver](https://dbeaver.io/) / Clientes JDBC/ODBC

---

## 🏛️ Arquitetura da Solução

O projeto implementa o padrão de **Arquitetura Medalhão** (*Medallion Architecture*):

```mermaid
flowchart TD
    subgraph Storage["Camada de Armazenamento (S3 Object Storage)"]
        MINIO["MinIO (:9000 / :9001)<br/>Buckets: raw, warehouse"]
    end

    subgraph Catalog["Catálogo & Governança (Git-like)"]
        NESSIE["Project Nessie (:19120)<br/>Iceberg REST Catalog (Branch: main)"]
    end

    subgraph Processing["Camada de Processamento Distribuído"]
        SPARK["Apache Spark + PySpark (:8080 / :8888)<br/>Iceberg Extensions + S3FileIO"]
    end

    subgraph QueryEngine["Camada de Consulta Analítica (SQL)"]
        TRINO["Trino Coordinator (:8081 HTTP / :8443 HTTPS)<br/>Iceberg Connector"]
    end

    subgraph Presentation["Camada de Apresentação & Consumo"]
        STREAMLIT["Streamlit Dashboard (:8501)"]
        DBEAVER["DBeaver / BI Tools"]
        JUPYTER["Jupyter Notebook (:8888)"]
    end

    CSV["Dados Brutos (Faker CSV)"] -->|Ingestão| MINIO
    SPARK -->|Lê arquivos brutos| MINIO
    SPARK -->|Grava metadados / commits| NESSIE
    SPARK -->|Grava tabelas Parquet| MINIO
    TRINO -->|Resolve metadados| NESSIE
    TRINO -->|Lê dados otimizados| MINIO
    JUPYTER --> SPARK
    STREAMLIT -->|Python Trino DBAPI| TRINO
    DBEAVER -->|Trino JDBC (SSL / HTTP)| TRINO
```

### Mapeamento de Portas e Serviços

| Serviço | Porta Host | Porta Container | Protocolo | Descrição |
| :--- | :--- | :--- | :--- | :--- |
| **MinIO API** | `9000` | `9000` | HTTP | API de armazenamento compatível com S3 |
| **MinIO Console** | `9001` | `9001` | HTTP | Interface Web de gerenciamento de buckets |
| **Project Nessie** | `19120` | `19120` | HTTP | API REST do Catálogo Iceberg |
| **Spark Master UI** | `8080` | `8080` | HTTP | Monitoramento do cluster Apache Spark |
| **Jupyter Notebook**| `8888` | `8888` | HTTP | Ambiente interativo para pipelines PySpark |
| **Trino HTTP** | `8081` | `8080` | HTTP | Endpoint HTTP e console Web do Trino |
| **Trino HTTPS (TLS)**| `8443` | `8443` | HTTPS | Endpoint seguro com Keystore PKCS12 |
| **Streamlit App** | `8501` | `8501` | HTTP | Dashboard analítico executivo em tempo real |

---

## 📂 Estrutura de Diretórios

```text
datalake-lab/
├── docker-compose.yml              # Orquestração de todos os serviços (MinIO, Nessie, Spark, Trino)
├── app.py                          # Dashboard analítico em Streamlit lendo direto do Trino
├── gerar_dataset.py                # Script gerador de dataset sintético (Faker)
├── README.md                       # Documentação principal do repositório
├── ROTEIRO_DATALAKE.md             # Guia detalhado de instalação, configuração e validações
├── data/
│   └── raw/
│       └── vendas_detalhadas.csv   # Dataset bruto de vendas gerado
├── notebooks/
│   └── pipeline_vendas.ipynb       # Notebook do pipeline Medalhão (Bronze -> Silver -> Gold)
├── spark-conf/
│   └── spark-defaults.conf         # Configurações do Apache Spark (Nessie + S3FileIO)
└── trino/
    ├── config.properties           # Configuração do coordenador Trino (HTTP e HTTPS)
    ├── keystore.p12                # Certificado PKCS12 para comunicação segura TLS/HTTPS
    └── catalog/
        └── iceberg.properties      # Conector Iceberg do Trino apontando para Nessie e MinIO
```

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
- **Docker** e **Docker Compose** instalados e em execução.
- **Python 3.10+** (recomendado uso de ambiente virtual `venv`).

### 1. Clonar o Repositório e Criar Ambiente Virtual

```bash
git clone <url-do-repositorio>
cd datalake-lab

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # ou: pip install streamlit trino pandas plotly faker
```

### 2. Iniciar os Serviços do Lakehouse

```bash
docker compose up -d
```

Aguarde alguns segundos para que todos os contêineres atinjam o estado saudável. Para verificar:

```bash
docker compose ps
```

### 3. Gerar Dados Sintéticos de Vendas

```bash
python3 gerar_dataset.py
```
> O arquivo `vendas_detalhadas.csv` será gerado no diretório `data/raw/`.

### 4. Executar o Pipeline de Dados no Apache Spark

Acesse o **Jupyter Notebook** no seu navegador:
👉 [http://localhost:8888](http://localhost:8888)

Abra o notebook em `notebooks/` e execute as células para processar as três camadas:

1. **Camada Bronze** (`nessie.bronze.vendas`): Ingestão direta do CSV bruto para tabela Iceberg sem transformação.
2. **Camada Silver** (`nessie.silver.vendas`): Tipagem forte de colunas, higienização, deduplicação por `id_venda` e particionamento físico por `estado` (UF).
3. **Camada Gold** (`nessie.gold.faturamento_por_categoria`): Agregação analítica de total de pedidos e faturamento consolidado por estado e categoria.

*(Opcional)* Você também pode executar comandos diretamente via `spark-sql`:
```bash
docker exec -it spark-iceberg spark-sql -e "SELECT * FROM nessie.gold.faturamento_por_categoria LIMIT 10;"
```

### 5. Iniciar o Dashboard Executivo Streamlit

```bash
streamlit run app.py --server.port 8501
```

Acesse o dashboard em:
👉 **[http://localhost:8501](http://localhost:8501)**

---

## 📊 Recursos do Dashboard Streamlit

O painel [app.py](file:///home/glauber.araujo.227/Documentos/Projects/datalake-lab/app.py) foi construído para conectar e ler dados em tempo real do Trino:

- **Conexão Dinâmica**: Alternância na barra lateral entre **HTTP (8081)** e **HTTPS/TLS (8443)**.
- **Padrão Numérico Brasileiro**: Formatação completa com **ponto `.` como separador de milhar** e **vírgula `,` para decimais** (`R$ 1.234.567,89`).
- **Padrão de Datas**: Datas no formato estrito **`dd/mm/aaaa`** no seletor de período, gráficos, tooltips e tabelas.
- **Cartões de Métricas (KPIs)**: Receita Total, Volume de Pedidos, Ticket Médio, Itens Vendidos e Clientes Únicos.
- **Gráficos Interativos (Plotly)**:
  - Evolução mensal de faturamento (gráfico de área com preenchimento).
  - Distribuição percentual de receita por categoria de produto (donut).
  - Faturamento consolidado por produto (barras horizontais).
  - Ranking dos Top 10 Estados em vendas.
- **Análise Regional (Camada Gold)**: Cruzamento de dados agregados por UF e categoria.
- **Explorador da Camada Silver**: Tabela detalhada de transações com filtro e botão para **exportação em CSV**.
- **Console Interativo SQL**: Terminal integrado para executar qualquer query SQL no Trino e inspecionar schemas e tabelas.

---

## 🔌 Conexão via DBeaver / Clientes SQL

Você pode explorar o catálogo Iceberg no Trino utilizando o **DBeaver**:

### Opção A: Conexão Segura com SSL (HTTPS) - Recomendada
- **Driver**: Trino
- **Host**: `localhost`
- **Port**: `8443`
- **Database / Catalog**: `iceberg`
- **Schema**: `gold` (ou `silver`, `bronze`)
- **Username**: `admin`
- **SSL**: Marcado (*Use SSL*)
- **Driver properties**:
  - `SSLVerification`: `NONE` *(necessário devido ao certificado autoassinado)*

### Opção B: Conexão Padrão sem SSL (HTTP)
- **Driver**: Trino
- **Host**: `localhost`
- **Port**: `8081`
- **Database / Catalog**: `iceberg`
- **Schema**: `gold`
- **Username**: `admin`
- **SSL**: Desmarcado

---

## 🛠️ Comandos Úteis do Cluster

### Consultar o Trino via CLI dentro do contêiner
```bash
# Via HTTPS
docker exec -it trino trino --server https://localhost:8443 --insecure --execute "SHOW SCHEMAS FROM iceberg;"

# Consulta de teste na camada Gold
docker exec -it trino trino --server https://localhost:8443 --insecure --execute "SELECT * FROM iceberg.gold.faturamento_por_categoria LIMIT 5;"
```

### Verificar Buckets no MinIO
```bash
docker run --rm --network datalake-lab_default --entrypoint /bin/sh minio/mc:latest \
  -c "mc alias set minio http://minio:9000 admin password123 && mc ls minio"
```

### Checar Saúde da API do Catálogo Nessie
```bash
curl -s http://localhost:19120/api/v1/config
```

### Visualizar Logs dos Serviços
```bash
docker compose logs -f trino
docker compose logs -f spark-iceberg
```

### Parar e Reiniciar o Cluster
```bash
# Parar os serviços mantendo os volumes de dados
docker compose stop

# Reiniciar
docker compose start

# Destruir contêineres mantendo os dados
docker compose down
```

---

## 📖 Documentação Adicional

Para uma explicação detalhada sobre a solução dos problemas de inicialização, configuração manual de certificados SSL, parametrização do conector Iceberg e evidências de validação passo a passo, consulte o [ROTEIRO_DATALAKE.md](file:///home/glauber.araujo.227/Documentos/Projects/datalake-lab/ROTEIRO_DATALAKE.md).

---

*Desenvolvido como ambiente de referência para arquiteturas modernas de Data Lakehouse.*
