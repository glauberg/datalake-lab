import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
import trino
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuração global de formato numérico no Plotly (vírgula decimal e ponto de milhar)
pio.templates["plotly_dark"].layout.separators = ",."

# Funções auxiliares de formatação pt-BR
def fmt_moeda(val):
    if pd.isna(val) or val is None:
        return "R$ 0,00"
    return f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_inteiro(val):
    if pd.isna(val) or val is None:
        return "0"
    return f"{int(val):,}".replace(",", ".")

def fmt_decimal(val):
    if pd.isna(val) or val is None:
        return "0,00"
    return f"{float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Configuração da Página
st.set_page_config(
    page_title="Lakehouse Analytics | Trino & Iceberg",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilização CSS personalizada
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 18px;
        color: white;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .metric-title {
        font-size: 0.85rem;
        font-weight: 500;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #38bdf8;
    }
</style>
""", unsafe_allow_html=True)


# Função para criar conexão com o Trino
def get_trino_connection(host, port, user, catalog, schema, protocol="http", verify_ssl=False):
    kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "catalog": catalog,
        "schema": schema,
        "http_scheme": protocol
    }
    if protocol == "https":
        kwargs["verify"] = verify_ssl
    return trino.dbapi.connect(**kwargs)


# Execução de consultas com cache do Streamlit
@st.cache_data(ttl=60, show_spinner=False)
def execute_query(query, host, port, user, catalog, schema, protocol, verify_ssl):
    conn = get_trino_connection(host, port, user, catalog, schema, protocol, verify_ssl)
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(data, columns=columns)


# Sidebar - Configurações de Conexão
TRINO_LOGO_URL = "https://upload.wikimedia.org/wikipedia/commons/5/57/Trino-logo-w-bk.svg"
st.sidebar.image(TRINO_LOGO_URL, width=200)
st.sidebar.title("Configurações do Lakehouse")

with st.sidebar.expander("🔌 Conexão Trino", expanded=False):
    trino_host = st.text_input("Host", value="localhost")
    conn_type = st.selectbox("Protocolo / Porta", ["HTTP (8081)", "HTTPS / SSL (8443)"])
    
    if "HTTPS" in conn_type:
        trino_port = 8443
        trino_proto = "https"
        verify_ssl = False
    else:
        trino_port = 8081
        trino_proto = "http"
        verify_ssl = False
        
    trino_user = st.text_input("Usuário", value="streamlit_analyst")
    trino_catalog = st.text_input("Catálogo", value="iceberg")

# Testar conexão
conn_status = st.sidebar.empty()
try:
    test_df = execute_query(
        "SELECT count(*) as qtd FROM iceberg.silver.vendas",
        trino_host, trino_port, trino_user, trino_catalog, "silver", trino_proto, verify_ssl
    )
    conn_status.success(f"Conectado ao Trino ({trino_proto.upper()}:{trino_port})")
except Exception as e:
    conn_status.error(f"Erro na conexão com Trino: {e}")
    st.error(f"Não foi possível conectar ao Trino em {trino_proto}://{trino_host}:{trino_port}.\n\nDetalhes do erro: {e}")
    st.stop()


# Carregar dados principais para os filtros
@st.cache_data(ttl=60, show_spinner="Carregando dados do Data Lake...")
def load_base_data():
    q = """
    SELECT 
        id_venda,
        cliente_nome,
        cliente_email,
        estado,
        produto,
        categoria,
        quantidade,
        CAST(preco_unitario AS DOUBLE) AS preco_unitario,
        data_venda,
        CAST(valor_total AS DOUBLE) AS valor_total
    FROM iceberg.silver.vendas
    ORDER BY data_venda DESC
    """
    df = execute_query(q, trino_host, trino_port, trino_user, trino_catalog, "silver", trino_proto, verify_ssl)
    df['data_venda'] = pd.to_datetime(df['data_venda'])
    return df

df_vendas = load_base_data()

# Filtros na Sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("Filtros de Negócio")

# Filtro de Data
min_date = df_vendas['data_venda'].min().date()
max_date = df_vendas['data_venda'].max().date()
date_range = st.sidebar.date_input(
    "Período de Vendas",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    format="DD/MM/YYYY"
)

# Filtro de Categoria
categorias_disponiveis = sorted(df_vendas['categoria'].unique().tolist())
categorias_selecionadas = st.sidebar.multiselect(
    "Categorias",
    options=categorias_disponiveis,
    default=categorias_disponiveis
)

# Filtro de Estado
estados_disponiveis = sorted(df_vendas['estado'].unique().tolist())
estados_selecionados = st.sidebar.multiselect(
    "Estados (UF)",
    options=estados_disponiveis,
    default=estados_disponiveis
)

# Aplicar filtros
if len(date_range) == 2:
    start_date, end_date = date_range
    df_filtered = df_vendas[
        (df_vendas['data_venda'].dt.date >= start_date) &
        (df_vendas['data_venda'].dt.date <= end_date) &
        (df_vendas['categoria'].isin(categorias_selecionadas)) &
        (df_vendas['estado'].isin(estados_selecionados))
    ]
else:
    df_filtered = df_vendas[
        (df_vendas['categoria'].isin(categorias_selecionadas)) &
        (df_vendas['estado'].isin(estados_selecionados))
    ]

# Cabeçalho Principal
st.title("⚡ Lakehouse Executive Dashboard")
if len(date_range) == 2:
    st.caption(f"Período: **{date_range[0].strftime('%d/%m/%Y')}** a **{date_range[1].strftime('%d/%m/%Y')}** | Dados lidos diretamente do **Trino** sobre tabelas **Apache Iceberg**")
else:
    st.caption("Dados em tempo real lidos diretamente do **Trino** sobre tabelas **Apache Iceberg** (Catálogo Nessie + Storage MinIO)")

# KPI Metrics
total_receita = df_filtered['valor_total'].sum()
total_pedidos = df_filtered['id_venda'].nunique()
ticket_medio = total_receita / total_pedidos if total_pedidos > 0 else 0.0
total_itens = df_filtered['quantidade'].sum()
clientes_unicos = df_filtered['cliente_email'].nunique()

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Receita Total</div>
        <div class="metric-value">{fmt_moeda(total_receita)}</div>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total Pedidos</div>
        <div class="metric-value">{fmt_inteiro(total_pedidos)}</div>
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Ticket Médio</div>
        <div class="metric-value">{fmt_moeda(ticket_medio)}</div>
    </div>
    """, unsafe_allow_html=True)
with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Itens Vendidos</div>
        <div class="metric-value">{fmt_inteiro(total_itens)}</div>
    </div>
    """, unsafe_allow_html=True)
with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Clientes Únicos</div>
        <div class="metric-value">{fmt_inteiro(clientes_unicos)}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Abas de Análise
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Desempenho de Vendas",
    "🗺️ Análise Regional & Categorias (Gold)",
    "🔍 Explorador de Dados (Silver)",
    "🏛️ Metadados & SQL Console"
])

with tab1:
    col_chart1, col_chart2 = st.columns([7, 5])
    
    with col_chart1:
        # Evolução temporal das vendas
        try:
            df_temporal = df_filtered.set_index('data_venda').resample('ME')['valor_total'].sum().reset_index()
        except ValueError:
            df_temporal = df_filtered.set_index('data_venda').resample('M')['valor_total'].sum().reset_index()
            
        fig_tempo = px.area(
            df_temporal,
            x='data_venda',
            y='valor_total',
            title="Evolução Mensal do Faturamento",
            labels={'data_venda': 'Mês', 'valor_total': 'Faturamento'},
            color_discrete_sequence=['#0ea5e9']
        )
        fig_tempo.update_layout(template="plotly_dark", separators=",.", margin=dict(l=20, r=20, t=40, b=20))
        fig_tempo.update_yaxes(tickprefix="R$ ", tickformat=",.2f")
        fig_tempo.update_xaxes(tickformat="%d/%m/%Y")
        fig_tempo.update_traces(hovertemplate="<b>Data: %{x|%d/%m/%Y}</b><br>Faturamento: R$ %{y:,.2f}<extra></extra>")
        st.plotly_chart(fig_tempo, width='stretch')
        
    with col_chart2:
        # Participação por Categoria
        df_cat = df_filtered.groupby('categoria')['valor_total'].sum().reset_index()
        fig_cat = px.pie(
            df_cat,
            names='categoria',
            values='valor_total',
            title="Distribuição de Receita por Categoria",
            hole=0.45,
            color_discrete_sequence=px.colors.sequential.Teal
        )
        fig_cat.update_layout(template="plotly_dark", separators=",.", margin=dict(l=20, r=20, t=40, b=20))
        fig_cat.update_traces(
            textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>Receita: R$ %{value:,.2f}<br>Participação: %{percent}<extra></extra>"
        )
        st.plotly_chart(fig_cat, width='stretch')
        
    col_chart3, col_chart4 = st.columns([6, 6])
    with col_chart3:
        # Faturamento por Produto
        df_prod = df_filtered.groupby('produto')['valor_total'].sum().reset_index().sort_values('valor_total', ascending=True)
        fig_prod = px.bar(
            df_prod,
            x='valor_total',
            y='produto',
            orientation='h',
            title="Faturamento por Produto",
            labels={'valor_total': 'Total', 'produto': 'Produto'},
            color='valor_total',
            color_continuous_scale="Blues"
        )
        fig_prod.update_layout(template="plotly_dark", separators=",.", margin=dict(l=20, r=20, t=40, b=20), showlegend=False)
        fig_prod.update_xaxes(tickprefix="R$ ", tickformat=",.2f")
        fig_prod.update_traces(hovertemplate="<b>%{y}</b><br>Faturamento: R$ %{x:,.2f}<extra></extra>")
        st.plotly_chart(fig_prod, width='stretch')
        
    with col_chart4:
        # Top 10 Estados em Faturamento
        df_uf = df_filtered.groupby('estado')['valor_total'].sum().reset_index().sort_values('valor_total', ascending=False).head(10)
        fig_uf = px.bar(
            df_uf,
            x='estado',
            y='valor_total',
            title="Top 10 Estados (UF) por Faturamento",
            labels={'estado': 'UF', 'valor_total': 'Total'},
            color='valor_total',
            color_continuous_scale="Viridis"
        )
        fig_uf.update_layout(template="plotly_dark", separators=",.", margin=dict(l=20, r=20, t=40, b=20), showlegend=False)
        fig_uf.update_yaxes(tickprefix="R$ ", tickformat=",.2f")
        fig_uf.update_traces(hovertemplate="<b>UF: %{x}</b><br>Faturamento: R$ %{y:,.2f}<extra></extra>")
        st.plotly_chart(fig_uf, width='stretch')

with tab2:
    st.subheader("Tabelas Agregadas na Camada Gold")
    st.markdown("Consultando a tabela analítica pré-computada pelo Spark: `iceberg.gold.faturamento_por_categoria`")
    
    try:
        q_gold = """
        SELECT 
            estado,
            categoria,
            total_pedidos,
            CAST(receita_total AS DOUBLE) as receita_total
        FROM iceberg.gold.faturamento_por_categoria
        ORDER BY receita_total DESC
        """
        df_gold = execute_query(q_gold, trino_host, trino_port, trino_user, trino_catalog, "gold", trino_proto, verify_ssl)
        
        # Filtro local da gold com base na seleção
        df_gold_filtered = df_gold[
            (df_gold['estado'].isin(estados_selecionados)) &
            (df_gold['categoria'].isin(categorias_selecionadas))
        ]
        
        col_g1, col_g2 = st.columns([7, 5])
        with col_g1:
            fig_gold_bar = px.bar(
                df_gold_filtered.head(20),
                x='estado',
                y='receita_total',
                color='categoria',
                barmode='stack',
                title="Receita por Estado e Categoria (Top 20 cruzamentos)",
                labels={'estado': 'UF', 'receita_total': 'Receita', 'categoria': 'Categoria'},
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            fig_gold_bar.update_layout(template="plotly_dark", separators=",.", margin=dict(l=20, r=20, t=40, b=20))
            fig_gold_bar.update_yaxes(tickprefix="R$ ", tickformat=",.2f")
            fig_gold_bar.update_traces(hovertemplate="<b>UF: %{x}</b><br>Categoria: %{data.name}<br>Receita: R$ %{y:,.2f}<extra></extra>")
            st.plotly_chart(fig_gold_bar, width='stretch')
            
        with col_g2:
            st.markdown("**Dados Consolidados (Gold)**")
            st.dataframe(
                df_gold_filtered.style.format({
                    'receita_total': fmt_moeda,
                    'total_pedidos': fmt_inteiro
                }),
                width='stretch',
                height=380
            )
    except Exception as e_gold:
        st.warning(f"Não foi possível carregar a tabela gold: {e_gold}")

with tab3:
    st.subheader("Camada Silver: Vendas Detalhadas")
    st.write(f"Exibindo **{len(df_filtered)}** registros filtrados da tabela `iceberg.silver.vendas`.")
    
    # Formatação amigável para exibição
    df_display = df_filtered.copy()
    df_display['data_venda'] = df_display['data_venda'].dt.strftime('%d/%m/%Y')
    
    st.dataframe(
        df_display[['id_venda', 'data_venda', 'cliente_nome', 'estado', 'produto', 'categoria', 'quantidade', 'preco_unitario', 'valor_total']]
        .style.format({
            'id_venda': fmt_inteiro,
            'quantidade': fmt_inteiro,
            'preco_unitario': fmt_moeda,
            'valor_total': fmt_moeda
        }),
        width='stretch',
        height=450
    )
    
    # Download do CSV filtrado
    csv_bytes = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Baixar Dados Filtrados (CSV)",
        data=csv_bytes,
        file_name="vendas_silver_filtradas.csv",
        mime="text/csv"
    )

with tab4:
    st.subheader("Console Interativo SQL & Metadados do Catálogo")
    
    col_cat, col_sql = st.columns([4, 8])
    
    with col_cat:
        st.markdown("#### 📁 Esquemas & Tabelas")
        try:
            schemas_df = execute_query("SHOW SCHEMAS FROM iceberg", trino_host, trino_port, trino_user, trino_catalog, "information_schema", trino_proto, verify_ssl)
            selected_schema = st.selectbox("Selecione um Esquema", schemas_df['Schema'].tolist(), index=schemas_df['Schema'].tolist().index('silver') if 'silver' in schemas_df['Schema'].tolist() else 0)
            
            tables_df = execute_query(f"SHOW TABLES FROM iceberg.{selected_schema}", trino_host, trino_port, trino_user, trino_catalog, selected_schema, trino_proto, verify_ssl)
            st.dataframe(tables_df, width='stretch', height=200)
        except Exception as e_cat:
            st.error(f"Erro ao listar tabelas: {e_cat}")
            
    with col_sql:
        st.markdown("#### 💻 Executar Consulta SQL no Trino")
        default_query = "SELECT estado, count(*) as total_vendas, sum(valor_total) as faturamento\nFROM iceberg.silver.vendas\nGROUP BY estado\nORDER BY faturamento DESC\nLIMIT 10"
        user_query = st.text_area("SQL Query", value=default_query, height=120)
        
        if st.button("Executar Consulta", type="primary"):
            try:
                with st.spinner("Executando query no Trino..."):
                    custom_df = execute_query(user_query, trino_host, trino_port, trino_user, trino_catalog, "silver", trino_proto, verify_ssl)
                st.success(f"Consulta executada com sucesso! ({len(custom_df)} linhas retornadas)")
                st.dataframe(custom_df, width='stretch')
            except Exception as e_sql:
                st.error(f"Erro na execução da consulta: {e_sql}")

st.markdown("---")
st.caption("Desenvolvido para Data Lakehouse Lab | Trino, Apache Iceberg, Project Nessie, Apache Spark e MinIO")
