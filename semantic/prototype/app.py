"""
FAME Data Space - Prototype Dashboard
======================================
Interactive visualization dashboard built with Streamlit.

Features:
- Real-time data monitoring
- Multi-domain analytics
- SPARQL query interface
- Data lineage visualization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random
import json
from typing import Dict, List

# Page configuration
st.set_page_config(
    page_title="FAME Data Space",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
    }
    .domain-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .market-data { background-color: #3B82F6; color: white; }
    .forex { background-color: #10B981; color: white; }
    .corporate { background-color: #8B5CF6; color: white; }
    .payments { background-color: #F59E0B; color: white; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# DATA GENERATION (Simulated - Replace with actual data sources)
# =============================================================================

@st.cache_data(ttl=60)
def generate_stock_data() -> pd.DataFrame:
    """Generate sample stock data."""
    stocks = [
        ("AAPL", "Apple Inc.", "NASDAQ", "USD"),
        ("MSFT", "Microsoft Corp.", "NASDAQ", "USD"),
        ("BNP.PA", "BNP Paribas", "Euronext", "EUR"),
        ("DBK.DE", "Deutsche Bank", "XETRA", "EUR"),
        ("HSBA.L", "HSBC Holdings", "LSE", "GBP"),
        ("JPM", "JPMorgan Chase", "NYSE", "USD"),
        ("V", "Visa Inc.", "NYSE", "USD"),
        ("MA", "Mastercard Inc.", "NYSE", "USD"),
        ("SQ", "Block Inc.", "NYSE", "USD"),
        ("PYPL", "PayPal Holdings", "NASDAQ", "USD"),
    ]
    
    data = []
    for ticker, name, exchange, currency in stocks:
        base_price = random.uniform(50, 300)
        data.append({
            "ticker": ticker,
            "company": name,
            "exchange": exchange,
            "currency": currency,
            "price": round(base_price + random.uniform(-5, 5), 2),
            "change_pct": round(random.uniform(-3, 3), 2),
            "volume": random.randint(1000000, 50000000),
            "market_cap_b": round(random.uniform(50, 500), 1)
        })
    
    return pd.DataFrame(data)


@st.cache_data(ttl=60)
def generate_forex_data() -> pd.DataFrame:
    """Generate sample forex data."""
    currencies = [
        ("USD", "US Dollar", 1.08),
        ("GBP", "British Pound", 0.86),
        ("JPY", "Japanese Yen", 163.5),
        ("CHF", "Swiss Franc", 0.94),
        ("AUD", "Australian Dollar", 1.65),
        ("CAD", "Canadian Dollar", 1.47),
        ("CNY", "Chinese Yuan", 7.82),
        ("INR", "Indian Rupee", 90.2),
    ]
    
    data = []
    for code, name, base_rate in currencies:
        rate = base_rate * random.uniform(0.98, 1.02)
        data.append({
            "currency_code": code,
            "currency_name": name,
            "rate_vs_eur": round(rate, 4),
            "change_24h": round(random.uniform(-1.5, 1.5), 2),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M")
        })
    
    return pd.DataFrame(data)


@st.cache_data(ttl=60)
def generate_transaction_data() -> pd.DataFrame:
    """Generate sample transaction data."""
    countries = ["FR", "DE", "NL", "ES", "IT", "BE", "AT", "PT", "GB", "US"]
    tx_types = ["SEPA", "INSTANT", "SWIFT", "DOMESTIC"]
    channels = ["MOBILE", "WEB", "API", "BRANCH"]
    
    data = []
    for i in range(100):
        sender_country = random.choice(countries)
        receiver_country = random.choice(countries)
        data.append({
            "tx_id": f"TX{random.randint(100000, 999999)}",
            "amount_eur": round(random.uniform(10, 50000), 2),
            "sender_country": sender_country,
            "receiver_country": receiver_country,
            "tx_type": random.choice(tx_types),
            "channel": random.choice(channels),
            "timestamp": datetime.now() - timedelta(hours=random.randint(0, 72)),
            "is_cross_border": sender_country != receiver_country,
            "status": random.choices(["COMPLETED", "PENDING", "FAILED"], weights=[85, 10, 5])[0]
        })
    
    return pd.DataFrame(data)


@st.cache_data(ttl=60)
def generate_company_financials() -> pd.DataFrame:
    """Generate sample company financial data."""
    companies = [
        ("BNP Paribas", "Banking", "FR"),
        ("Deutsche Bank", "Banking", "DE"),
        ("ING Group", "Banking", "NL"),
        ("Allianz SE", "Insurance", "DE"),
        ("AXA", "Insurance", "FR"),
        ("Adyen", "Payments", "NL"),
        ("Worldline", "Payments", "FR"),
        ("Wise", "Fintech", "GB"),
        ("N26", "Fintech", "DE"),
        ("Revolut", "Fintech", "GB"),
    ]
    
    data = []
    for name, sector, country in companies:
        data.append({
            "company": name,
            "sector": sector,
            "country": country,
            "revenue_m": round(random.uniform(500, 50000), 1),
            "net_income_m": round(random.uniform(50, 5000), 1),
            "profit_margin": round(random.uniform(5, 25), 1),
            "roe": round(random.uniform(5, 20), 1),
            "total_assets_b": round(random.uniform(10, 1000), 1)
        })
    
    return pd.DataFrame(data)


# =============================================================================
# SIDEBAR
# =============================================================================

st.sidebar.image("https://via.placeholder.com/200x80?text=FAME+DataSpace", width=200)
st.sidebar.markdown("## 🏦 FAME Data Space")
st.sidebar.markdown("*Finance & Embedded Finance*")
st.sidebar.markdown("---")

# Domain selector
domain = st.sidebar.selectbox(
    "📊 Select Domain",
    ["Overview", "Market Data", "Foreign Exchange", "Corporate Finance", "Payments", "SPARQL Interface"]
)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 📈 Data Sources")
st.sidebar.markdown("✅ Stock API (Real-time)")
st.sidebar.markdown("✅ ECB XML (Daily)")
st.sidebar.markdown("✅ Financials CSV (Quarterly)")
st.sidebar.markdown("✅ Transactions DB (Real-time)")

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔧 Infrastructure")
st.sidebar.markdown("🟢 Kafka: Connected")
st.sidebar.markdown("🟢 MinIO: Online")
st.sidebar.markdown("🟢 Fuseki: Ready")
st.sidebar.markdown("🟢 PostgreSQL: Active")


# =============================================================================
# MAIN CONTENT
# =============================================================================

st.markdown('<h1 class="main-header">🏦 FAME Financial Data Space</h1>', unsafe_allow_html=True)

# Load data
stocks_df = generate_stock_data()
forex_df = generate_forex_data()
transactions_df = generate_transaction_data()
financials_df = generate_company_financials()


# -----------------------------------------------------------------------------
# OVERVIEW PAGE
# -----------------------------------------------------------------------------

if domain == "Overview":
    st.markdown("### 📊 Data Space Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📈 Stock Quotes",
            value=len(stocks_df),
            delta="+2 today"
        )
    
    with col2:
        st.metric(
            label="💱 Exchange Rates",
            value=len(forex_df),
            delta="ECB Updated"
        )
    
    with col3:
        st.metric(
            label="💳 Transactions (24h)",
            value=f"{len(transactions_df)}",
            delta=f"+{random.randint(10, 50)}"
        )
    
    with col4:
        total_volume = transactions_df['amount_eur'].sum()
        st.metric(
            label="💰 Total Volume",
            value=f"€{total_volume/1000:.0f}K",
            delta=f"+{random.randint(5, 15)}%"
        )
    
    st.markdown("---")
    
    # Data Flow Diagram
    st.markdown("### 🔄 Data Flow Architecture")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Transaction flow sankey
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=["Stock API", "ECB XML", "CSV Files", "PostgreSQL", 
                       "Kafka", "Data Lake", "RDF Store", "Dashboard"],
                color=["#3B82F6", "#10B981", "#8B5CF6", "#F59E0B",
                       "#EF4444", "#06B6D4", "#6366F1", "#EC4899"]
            ),
            link=dict(
                source=[0, 1, 2, 3, 4, 4, 5, 6],
                target=[4, 4, 5, 4, 5, 6, 6, 7],
                value=[30, 20, 25, 35, 50, 40, 60, 70]
            )
        )])
        
        fig.update_layout(
            title_text="Data Pipeline Flow",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Domain distribution pie chart
        domain_data = pd.DataFrame({
            "Domain": ["Market Data", "Foreign Exchange", "Corporate Finance", "Payments"],
            "Records": [len(stocks_df) * 100, len(forex_df) * 50, len(financials_df) * 20, len(transactions_df)]
        })
        
        fig = px.pie(
            domain_data, 
            values='Records', 
            names='Domain',
            title='Data Distribution by Domain',
            color_discrete_sequence=['#3B82F6', '#10B981', '#8B5CF6', '#F59E0B']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Recent Activity
    st.markdown("### 📝 Recent Activity")
    
    activities = [
        {"time": "2 min ago", "event": "📈 Stock data refreshed (10 quotes)", "domain": "Market Data"},
        {"time": "5 min ago", "event": "💱 ECB rates updated", "domain": "Forex"},
        {"time": "8 min ago", "event": "💳 Batch: 50 transactions processed", "domain": "Payments"},
        {"time": "15 min ago", "event": "📊 RDF triples generated (1,250)", "domain": "Semantic"},
        {"time": "1 hour ago", "event": "🔄 ETL pipeline completed", "domain": "System"},
    ]
    
    for activity in activities:
        st.markdown(f"**{activity['time']}** - {activity['event']} `{activity['domain']}`")


# -----------------------------------------------------------------------------
# MARKET DATA PAGE
# -----------------------------------------------------------------------------

elif domain == "Market Data":
    st.markdown("### 📈 Market Data Domain")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Stock table
        st.markdown("#### Real-Time Stock Quotes")
        
        styled_df = stocks_df.style.applymap(
            lambda x: 'color: green' if isinstance(x, float) and x > 0 else ('color: red' if isinstance(x, float) and x < 0 else ''),
            subset=['change_pct']
        )
        st.dataframe(stocks_df, use_container_width=True)
    
    with col2:
        # Exchange distribution
        fig = px.pie(
            stocks_df, 
            names='exchange', 
            title='By Exchange',
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Price comparison bar chart
    st.markdown("#### Stock Price Comparison")
    fig = px.bar(
        stocks_df, 
        x='ticker', 
        y='price',
        color='currency',
        title='Current Stock Prices',
        labels={'price': 'Price', 'ticker': 'Ticker'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Volume chart
    fig = px.bar(
        stocks_df,
        x='ticker',
        y='volume',
        title='Trading Volume',
        color='exchange'
    )
    st.plotly_chart(fig, use_container_width=True)


# -----------------------------------------------------------------------------
# FOREIGN EXCHANGE PAGE
# -----------------------------------------------------------------------------

elif domain == "Foreign Exchange":
    st.markdown("### 💱 Foreign Exchange Domain")
    
    st.markdown("#### ECB Exchange Rates (vs EUR)")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.dataframe(forex_df, use_container_width=True)
    
    with col2:
        st.markdown("**Base Currency:** EUR 🇪🇺")
        st.markdown(f"**Last Update:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        st.markdown("**Source:** European Central Bank")
    
    # Exchange rate visualization
    fig = px.bar(
        forex_df,
        x='currency_code',
        y='rate_vs_eur',
        color='change_24h',
        color_continuous_scale='RdYlGn',
        title='Exchange Rates vs EUR',
        labels={'rate_vs_eur': 'Rate', 'currency_code': 'Currency'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Change heatmap
    fig = px.bar(
        forex_df,
        x='currency_code',
        y='change_24h',
        color='change_24h',
        color_continuous_scale='RdYlGn',
        title='24h Change (%)'
    )
    st.plotly_chart(fig, use_container_width=True)


# -----------------------------------------------------------------------------
# CORPORATE FINANCE PAGE
# -----------------------------------------------------------------------------

elif domain == "Corporate Finance":
    st.markdown("### 📊 Corporate Finance Domain")
    
    # Sector filter
    sectors = ["All"] + list(financials_df['sector'].unique())
    selected_sector = st.selectbox("Filter by Sector", sectors)
    
    if selected_sector != "All":
        filtered_df = financials_df[financials_df['sector'] == selected_sector]
    else:
        filtered_df = financials_df
    
    st.dataframe(filtered_df, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by company
        fig = px.bar(
            filtered_df.sort_values('revenue_m', ascending=True),
            x='revenue_m',
            y='company',
            orientation='h',
            title='Revenue (Millions €)',
            color='sector'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # ROE vs Profit Margin scatter
        fig = px.scatter(
            filtered_df,
            x='profit_margin',
            y='roe',
            size='total_assets_b',
            color='sector',
            hover_name='company',
            title='ROE vs Profit Margin',
            labels={'profit_margin': 'Profit Margin (%)', 'roe': 'ROE (%)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Sector summary
    sector_summary = financials_df.groupby('sector').agg({
        'revenue_m': 'sum',
        'profit_margin': 'mean',
        'roe': 'mean'
    }).round(2)
    
    st.markdown("#### Sector Summary")
    st.dataframe(sector_summary, use_container_width=True)


# -----------------------------------------------------------------------------
# PAYMENTS PAGE
# -----------------------------------------------------------------------------

elif domain == "Payments":
    st.markdown("### 💳 Payments Domain")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Transactions", len(transactions_df))
    
    with col2:
        cross_border = transactions_df['is_cross_border'].sum()
        st.metric("Cross-Border", cross_border, f"{cross_border/len(transactions_df)*100:.0f}%")
    
    with col3:
        completed = (transactions_df['status'] == 'COMPLETED').sum()
        st.metric("Completed", completed, f"{completed/len(transactions_df)*100:.0f}%")
    
    with col4:
        avg_amount = transactions_df['amount_eur'].mean()
        st.metric("Avg. Amount", f"€{avg_amount:,.0f}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Transaction type distribution
        tx_type_counts = transactions_df['tx_type'].value_counts()
        fig = px.pie(
            values=tx_type_counts.values,
            names=tx_type_counts.index,
            title='Transaction Types'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Channel distribution
        channel_counts = transactions_df['channel'].value_counts()
        fig = px.pie(
            values=channel_counts.values,
            names=channel_counts.index,
            title='Channels'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Volume by country
    country_volume = transactions_df.groupby('sender_country')['amount_eur'].sum().sort_values(ascending=False)
    fig = px.bar(
        x=country_volume.index,
        y=country_volume.values,
        title='Transaction Volume by Sender Country',
        labels={'x': 'Country', 'y': 'Volume (EUR)'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Cross-border flows
    st.markdown("#### Cross-Border Payment Flows")
    cross_border_df = transactions_df[transactions_df['is_cross_border']]
    flows = cross_border_df.groupby(['sender_country', 'receiver_country']).agg({
        'amount_eur': ['sum', 'count']
    }).reset_index()
    flows.columns = ['From', 'To', 'Volume', 'Count']
    st.dataframe(flows.head(20), use_container_width=True)


# -----------------------------------------------------------------------------
# SPARQL INTERFACE PAGE
# -----------------------------------------------------------------------------

elif domain == "SPARQL Interface":
    st.markdown("### 🔍 SPARQL Query Interface")
    
    st.markdown("""
    Execute SPARQL queries against the FAME RDF Triple Store.
    
    **Endpoint:** `http://localhost:3030/fame/sparql`
    """)
    
    # Sample queries
    sample_queries = {
        "All Stocks": """PREFIX fame: <http://fame.eu/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?stock ?ticker ?price
WHERE {
    ?stock a fame:Stock .
    ?stock fame:hasTicker ?ticker .
    ?stock fame:price ?price .
}
ORDER BY ?ticker""",
        
        "High-Value Transactions": """PREFIX fame: <http://fame.eu/ontology#>

SELECT ?tx ?amount ?sender ?receiver
WHERE {
    ?tx a fame:Transaction .
    ?tx fame:amountEUR ?amount .
    ?tx fame:hasSender ?sender .
    ?tx fame:hasReceiver ?receiver .
    FILTER (?amount > 10000)
}
LIMIT 20""",
        
        "Exchange Rates": """PREFIX fame: <http://fame.eu/ontology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?from_cur ?to_cur ?rate
WHERE {
    ?fx a fame:ExchangeRate .
    ?fx fame:fromCurrency ?from_uri .
    ?fx fame:toCurrency ?to_uri .
    ?fx fame:rate ?rate .
    ?from_uri skos:notation ?from_cur .
    ?to_uri skos:notation ?to_cur .
}""",
        
        "Data Catalog": """PREFIX fame: <http://fame.eu/ontology#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?type (COUNT(?entity) AS ?count)
WHERE {
    ?entity rdf:type ?type .
    FILTER (STRSTARTS(STR(?type), "http://fame.eu"))
}
GROUP BY ?type
ORDER BY DESC(?count)"""
    }
    
    selected_query = st.selectbox("📝 Sample Queries", list(sample_queries.keys()))
    
    query = st.text_area(
        "SPARQL Query",
        value=sample_queries[selected_query],
        height=250
    )
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        execute_btn = st.button("▶️ Execute Query", type="primary")
    
    with col2:
        endpoint = st.text_input("Endpoint", "http://localhost:3030/fame/sparql")
    
    if execute_btn:
        st.markdown("#### Results")
        
        # Simulated results (replace with actual SPARQL execution)
        st.info("💡 Connect to Fuseki endpoint to execute real queries. Showing sample results.")
        
        if "Stock" in query:
            results = stocks_df[['ticker', 'price']].head(5)
            st.dataframe(results)
        elif "Transaction" in query:
            results = transactions_df[['tx_id', 'amount_eur', 'sender_country', 'receiver_country']].head(5)
            st.dataframe(results)
        elif "Exchange" in query:
            results = forex_df[['currency_code', 'rate_vs_eur']].head(5)
            st.dataframe(results)
        else:
            st.json({
                "fame:Stock": 10,
                "fame:Transaction": 100,
                "fame:ExchangeRate": 8,
                "fame:FinancialInstitution": 10
            })


# =============================================================================
# FOOTER
# =============================================================================

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666;">
    <p>FAME Data Space Prototype | Master M2 Project | 2024</p>
    <p>Built with ❤️ using Streamlit, RDFLib, Kafka, and MinIO</p>
</div>
""", unsafe_allow_html=True)
