import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def generate_example_graph(ticker='AAPL'):
    # 1. Setup paths
    db_path = '/home/daniel/sovson-analytics/data/sovson_analytics.db'
    output_dir = '/home/daniel/Mac-D-Alert/scripts/static'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'example_graph_{ticker}.html')

    # 2. Fetch Data
    conn = sqlite3.connect(db_path)
    # Fetch 6 months of data (roughly 126 trading days)
    query = f"""
    SELECT date, open, high, low, close, volume, ha_open, ha_high, ha_low, ha_close
    FROM daily_prices 
    WHERE ticker = '{ticker}' 
    ORDER BY date DESC 
    LIMIT 126
    """
    df = pd.read_sql_query(query, conn)
    
    # Fetch MACD data
    macd_query = f"""
    SELECT calculation_date as date, macd_line as macd, signal_line as signal, histogram
    FROM macd_5d_data
    WHERE ticker = '{ticker}'
    ORDER BY calculation_date DESC
    LIMIT 126
    """
    macd_df = pd.read_sql_query(macd_query, conn)

    # Fetch Signals for Arrows
    signals_query = f"""
    SELECT signal_date, signal_type, price_at_signal
    FROM signals
    WHERE ticker = '{ticker}'
    ORDER BY signal_date DESC
    LIMIT 50
    """
    signals_df = pd.read_sql_query(signals_query, conn)
    conn.close()

    # Reorder to chronological for plotting
    df = df.sort_values('date')
    macd_df = macd_df.sort_values('date')

    # 3. Create Figure with subplots
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, subplot_titles=(f'{ticker} - 6 Month Trend (Heikin Ashi)', '5-Day Rolling MACD'),
                        row_width=[0.3, 0.7])

    # 4. Add Heikin Ashi Candlestick Trace
    fig.add_trace(go.Candlestick(
        x=df['date'],
        open=df['ha_open'],
        high=df['ha_high'],
        low=df['ha_low'],
        close=df['ha_close'],
        name='Price',
        increasing_line_color='#26a69a', decreasing_line_color='#ef5350'
    ), row=1, col=1)

    # 5. Add Signal Arrows
    for _, row in signals_df.iterrows():
        # Only plot if within the date range of our price DF
        if row['signal_date'] in df['date'].values:
            color = '#2ecc71' if 'BUY' in row['signal_type'] else '#e74c3c'
            symbol = 'triangle-up' if 'BUY' in row['signal_type'] else 'triangle-down'
            # Offset the arrow slightly from the price
            y_pos = row['price_at_signal'] * (0.98 if 'BUY' in row['signal_type'] else 1.02)
            
            fig.add_trace(go.Scatter(
                x=[row['signal_date']],
                y=[y_pos],
                mode='markers',
                marker=dict(symbol=symbol, size=12, color=color),
                name=row['signal_type'],
                showlegend=False
            ), row=1, col=1)

    # 6. Add MACD Traces
    fig.add_trace(go.Scatter(x=macd_df['date'], y=macd_df['macd'], 
                             line=dict(color='#2196f3', width=2), name='MACD'), row=2, col=1)
    fig.add_trace(go.Scatter(x=macd_df['date'], y=macd_df['signal'], 
                             line=dict(color='#ff9800', width=2), name='Signal'), row=2, col=1)
    fig.add_trace(go.Bar(x=macd_df['date'], y=macd_df['histogram'], 
                         marker_color=macd_df['histogram'].apply(lambda x: '#26a69a' if x >= 0 else '#ef5350'),
                         name='Histogram'), row=2, col=1)

    # 7. Formatting
    fig.update_layout(
        template='plotly_dark',
        xaxis_rangeslider_visible=False,
        showlegend=True,
        height=800,
        margin=dict(l=50, r=50, t=80, b=50)
    )

    # 8. Save to HTML
    fig.write_html(output_path)
    return output_path

if __name__ == "__main__":
    generate_example_graph('AAPL')
