import sqlite3
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import os
from datetime import datetime

def generate_static_graph(ticker='AAPL'):
    db_path = '/home/daniel/sovson-analytics/data/sovson_analytics.db'
    output_dir = '/home/daniel/Mac-D-Alert/scripts/static'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'graph_{ticker}.png')

    conn = sqlite3.connect(db_path)
    
    # Fetch 6 months of price data
    query = f"""
    SELECT date, ha_open as Open, ha_high as High, ha_low as Low, ha_close as Close
    FROM daily_prices 
    WHERE ticker = '{ticker}' 
    ORDER BY date DESC LIMIT 126
    """
    df = pd.read_sql_query(query, conn)
    
    # Fetch MACD data
    macd_query = f"""
    SELECT period_end_date as date, macd_line, signal_line, histogram
    FROM macd_5d_data
    WHERE ticker = '{ticker}'
    ORDER BY period_end_date DESC LIMIT 126
    """
    mdf = pd.read_sql_query(macd_query, conn)

    # Fetch Signals
    sig_query = f"""
    SELECT signal_date, signal_type, price_at_signal
    FROM signals
    WHERE ticker = '{ticker}'
    ORDER BY signal_date DESC LIMIT 50
    """
    sdf = pd.read_sql_query(sig_query, conn)
    conn.close()

    # Prepare index
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)

    mdf['date'] = pd.to_datetime(mdf['date'])
    mdf.set_index('date', inplace=True)
    mdf.sort_index(inplace=True)

    # Prepare Signal Markers
    # We create a series of NaNs and fill only the signal points
    buy_signals = pd.Series(float('nan'), index=df.index)
    sell_signals = pd.Series(float('nan'), index=df.index)

    has_buy = False
    has_sell = False
    for _, row in sdf.iterrows():
        dt = pd.to_datetime(row['signal_date'])
        if dt in df.index:
            if 'BUY' in row['signal_type']:
                buy_signals.loc[dt] = df.loc[dt, 'Low'] * 0.98
                has_buy = True
            else:
                sell_signals.loc[dt] = df.loc[dt, 'High'] * 1.02
                has_sell = True

    # Add-on plots
    # Create histogram colors: green for positive, red for negative
    hist_colors = ['#26a69a' if val >= 0 else '#ef5350' for val in mdf['histogram']]

    apds = [
        mpf.make_addplot(mdf['macd_line'], panel=1, color='dodgerblue', width=1, ylabel='MACD'),
        mpf.make_addplot(mdf['signal_line'], panel=1, color='orange', width=1),
        mpf.make_addplot(mdf['histogram'], panel=1, type='bar', color=hist_colors, alpha=0.8)
    ]
    
    if has_buy:
        apds.append(mpf.make_addplot(buy_signals, type='scatter', markersize=150, marker='^', color='#2ecc71'))
    if has_sell:
        apds.append(mpf.make_addplot(sell_signals, type='scatter', markersize=150, marker='v', color='#e74c3c'))

    # Plot
    s = mpf.make_mpf_style(base_mpf_style='charles', gridcolor='#2a2d3a', facecolor='#0f1117', edgecolor='#2a2d3a')
    
    fig, axlist = mpf.plot(df, type='candle', addplot=apds, figscale=1.5,
                           style=s, volume=False, datetime_format='%b %Y', 
                           tight_layout=True, returnfig=True)
    
    # Add large watermark to the background of the top panel (axlist[0])
    axlist[0].text(0.5, 0.5, ticker, transform=axlist[0].transAxes,
                   fontsize=80, color='white', alpha=0.07,
                   ha='center', va='center', weight='bold', zorder=0)

    fig.savefig(output_path)
    plt.close(fig)
    
    print(f"Static graph generated with watermark at: {output_path}")

if __name__ == "__main__":
    import sys
    db_path = '/home/daniel/sovson-analytics/data/sovson_analytics.db'
    conn = sqlite3.connect(db_path)
    if len(sys.argv) > 1:
        tickers = sys.argv[1:]
    else:
        tickers = [row[0] for row in conn.execute("SELECT ticker FROM tickers WHERE active = 1")]
    conn.close()
    
    for ticker in tickers:
        try:
            generate_static_graph(ticker)
        except Exception as e:
            print(f"Error generating graph for {ticker}: {e}")
