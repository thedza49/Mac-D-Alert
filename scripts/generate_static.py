import sqlite3
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

def generate_static_graph(ticker='AAPL'):
    base_dir = Path(__file__).resolve().parent.parent
    db_path = base_dir / 'data' / 'sovson_analytics.db'
    output_dir = base_dir / 'scripts' / 'static'
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
    
    # SELF-HEALING: If we have plenty of price data but 0 signals, 
    # trigger the signal detector to backfill history.
    if sdf.empty and len(df) > 50:
        print(f"⚠️ {ticker}: No signals found in database but price history exists. Triggering auto-repair...")
        conn.close() # Close to avoid lock during subprocess
        try:
            # Trigger history scan in background
            subprocess.run([sys.executable, str(base_dir / 'scripts' / 'signal_detector.py'), ticker, '--history'], check=True)
            # Re-fetch signals after repair
            conn = sqlite3.connect(db_path)
            sdf = pd.read_sql_query(sig_query, conn)
        except Exception as e:
            print(f"❌ Failed to repair signals for {ticker}: {e}")
            conn = sqlite3.connect(db_path) # Ensure conn is open for the rest of the script
    
    conn.close()

    # Prepare index
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)

    mdf['date'] = pd.to_datetime(mdf['date'])
    mdf.set_index('date', inplace=True)
    mdf.sort_index(inplace=True)

    # Prepare Signal Markers
    # We create separate series for each phase
    signals = {
        'BUY':              {'series': pd.Series(float('nan'), index=df.index), 'color': '#2ecc71', 'marker': '^', 'label': 'BUY'},
        'APPROACHING_BUY':  {'series': pd.Series(float('nan'), index=df.index), 'color': '#2ecc71', 'marker': 'o', 'label': 'APRCH BUY'},
        'SELL':             {'series': pd.Series(float('nan'), index=df.index), 'color': '#e74c3c', 'marker': 'v', 'label': 'SELL'},
        'APPROACHING_SELL': {'series': pd.Series(float('nan'), index=df.index), 'color': '#e74c3c', 'marker': 'o', 'label': 'APRCH SELL'}
    }

    for _, row in sdf.iterrows():
        dt = pd.to_datetime(row['signal_date'])
        stype = row['signal_type']
        if dt in df.index and stype in signals:
            if 'BUY' in stype:
                signals[stype]['series'].loc[dt] = df.loc[dt, 'Low'] * 0.98
            else:
                signals[stype]['series'].loc[dt] = df.loc[dt, 'High'] * 1.02

    # Add-on plots
    # Create histogram colors: green for positive, red for negative
    hist_colors = ['#26a69a' if val >= 0 else '#ef5350' for val in mdf['histogram']]

    apds = [
        mpf.make_addplot(mdf['macd_line'], panel=1, color='dodgerblue', width=1, ylabel='MACD', label='MACD'),
        mpf.make_addplot(mdf['signal_line'], panel=1, color='orange', width=1, label='Signal'),
        mpf.make_addplot(mdf['histogram'], panel=1, type='bar', color=hist_colors, alpha=0.8)
    ]
    
    # Add scatter plots for each signal type that actually exists in this timeframe
    for stype, cfg in signals.items():
        if not cfg['series'].dropna().empty:
            apds.append(mpf.make_addplot(cfg['series'], type='scatter', markersize=150 if cfg['marker'] != 'o' else 80, 
                                        marker=cfg['marker'], color=cfg['color'], label=cfg['label']))

    # Plot
    s = mpf.make_mpf_style(base_mpf_style='charles', gridcolor='#2a2d3a', facecolor='#0f1117', edgecolor='#2a2d3a')
    
    fig, axlist = mpf.plot(df, type='candle', addplot=apds, figscale=1.5,
                           style=s, volume=False, datetime_format='%b %Y', 
                           tight_layout=True, returnfig=True)
    
    # Set tick colors to white
    for ax in axlist:
        ax.tick_params(axis='x', colors='white')
        ax.tick_params(axis='y', colors='white')

    # Add large watermark to the background of the top panel (axlist[0])
    axlist[0].text(0.5, 0.5, ticker, transform=axlist[0].transAxes,
                   fontsize=80, color='white', alpha=0.07,
                   ha='center', va='center', weight='bold', zorder=0)

    # Add Legend to the top panel
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='^', color='w', label='BUY', markerfacecolor='#2ecc71', markersize=10),
        Line2D([0], [0], marker='o', color='w', label='APRCH BUY', markerfacecolor='#2ecc71', markersize=8),
        Line2D([0], [0], marker='v', color='w', label='SELL', markerfacecolor='#e74c3c', markersize=10),
        Line2D([0], [0], marker='o', color='w', label='APRCH SELL', markerfacecolor='#e74c3c', markersize=8),
    ]
    # Use white text for the legend on dark background
    leg = axlist[0].legend(handles=legend_elements, loc='upper left', fontsize=9, frameon=True, facecolor='#1a1d27', edgecolor='#2a2d3a', labelcolor='white')

    # Add Label and Legend to the MACD panel (usually axlist[2] when no volume)
    # axlist mapping: [Main, [Secondary-Main?], Panel1, [Secondary-Panel1?]]
    # Since we have only one panel, it's at index 2.
    if len(axlist) >= 3:
        macd_ax = axlist[2]
        macd_ax.set_ylabel('MACD / Signal', color='white', fontsize=10)
        macd_ax.tick_params(axis='y', colors='white')
        # Adding a small legend for the MACD panel
        macd_leg = macd_ax.legend(loc='upper left', fontsize=8, frameon=False, labelcolor='white')

    fig.savefig(output_path)
    plt.close(fig)
    
    print(f"Static graph generated with watermark at: {output_path}")

if __name__ == "__main__":
    db_path = Path(__file__).resolve().parent.parent / 'data' / 'sovson_analytics.db'
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
