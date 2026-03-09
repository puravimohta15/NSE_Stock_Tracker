
import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

@st.cache_data
def load_signals():
    return pd.read_csv("data/signals.csv")
st.set_page_config(page_title="EMA Crossover Screener", layout="wide")

# Auto refresh
st_autorefresh(interval=60000, key="refresh")

st.title("📈 Buy or exit stocks trigger")

# Load signals
df = load_signals()

# Sidebar filters
st.sidebar.header("Filters")

signal_filter = st.sidebar.multiselect(
    "Signal Type",
    options=df["Signal"].unique(),
    default=df["Signal"].unique()
)

filtered_df = df[df["Signal"].isin(signal_filter)]

# Metrics row (dashboard style)
col1, col2, col3 = st.columns(3)

col1.metric("Total Signals", len(df))
col2.metric("Buy Signals", (df["Signal"] == "BUY").sum())
col3.metric("Exit Signals", (df["Signal"] == "EXIT").sum())

st.divider()

# Layout split
left, right = st.columns([1, 2])

# Left panel: signals table
with left:
    st.subheader("Signals")

    st.dataframe(
        filtered_df,
        width="stretch",
        hide_index=True
    )

    stock = st.selectbox("Select Stock", filtered_df["Stock"])

# Right panel: chart
with right:

    st.subheader(f"{stock} Chart")

    data = yf.download(stock, period="6mo")

    data["EMA10"] = data["Close"].ewm(span=10).mean()
    data["EMA20"] = data["Close"].ewm(span=20).mean()

    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=data.index,
        open=data["Open"],
        high=data["High"],
        low=data["Low"],
        close=data["Close"],
        name="Price"
    ))

    fig.add_trace(go.Scatter(
        x=data.index,
        y=data["EMA10"],
        name="EMA10",
        line=dict(width=2)
    ))

    fig.add_trace(go.Scatter(
        x=data.index,
        y=data["EMA20"],
        name="EMA20",
        line=dict(width=2)
    ))

    fig.update_layout(
        height=600,
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )

    st.plotly_chart(fig, use_container_width=True)

