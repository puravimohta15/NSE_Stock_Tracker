import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh


st.title("Stock EMA Crossover Screener")

df = pd.read_csv("data/signals.csv")

st.dataframe(df)

stock = st.selectbox("Select Stock", df["Stock"])

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
    name="EMA10"
))

fig.add_trace(go.Scatter(
    x=data.index,
    y=data["EMA20"],
    name="EMA20"
))

st_autorefresh(interval=60000, key="refresh")  # refresh every 60 seconds
st.plotly_chart(fig)