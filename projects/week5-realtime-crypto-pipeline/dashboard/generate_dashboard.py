# ============================================================
# Week 5 - Crypto Price Dashboard Generator
# 
# What this does:
# 1. Queries BigQuery for all collected crypto prices
# 2. Generates a professional 4-panel dashboard image
# 3. Saves as dashboard.png — ready for LinkedIn/GitHub
#
# Skills shown: Python + BigQuery + Data Viz + Cloud
# ============================================================

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
from google.cloud import bigquery
from datetime import datetime

# ── Configuration ──────────────────────────────────────────
PROJECT_ID = "intricate-ward-459513-e1"
DATASET    = "crypto_streaming"
TABLE      = "raw_crypto_prices"

# Coin colors — makes chart look professional
COIN_COLORS = {
    "bitcoin":  "#F7931A",  # Bitcoin orange
    "ethereum": "#627EEA",  # Ethereum blue
    "solana":   "#9945FF"   # Solana purple
}

COIN_SYMBOLS = {
    "bitcoin":  "BTC",
    "ethereum": "ETH",
    "solana":   "SOL"
}


def fetch_data_from_bigquery() -> pd.DataFrame:
    """Pull all crypto price data from BigQuery into a pandas DataFrame."""
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
        SELECT 
            coin,
            price_usd,
            change_24h_pct,
            volume_24h,
            ingested_at
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        ORDER BY ingested_at ASC
    """
    
    print("Fetching data from BigQuery...")
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df)} rows — {df['coin'].nunique()} coins")
    return df


def get_latest_prices(df: pd.DataFrame) -> dict:
    """Get the most recent price for each coin."""
    latest = {}
    for coin in df['coin'].unique():
        coin_df = df[df['coin'] == coin]
        latest[coin] = coin_df.iloc[-1]
    return latest


def generate_dashboard(df: pd.DataFrame):
    """Generate a 4-panel professional dashboard."""
    
    latest = get_latest_prices(df)
    
    # ── Figure Setup ───────────────────────────────────────
    fig = plt.figure(figsize=(16, 10), facecolor='#0D1117')
    fig.patch.set_facecolor('#0D1117')
    
    # Grid: 3 rows, 3 columns
    gs = gridspec.GridSpec(
        3, 3,
        figure=fig,
        hspace=0.45,
        wspace=0.35,
        top=0.88,
        bottom=0.08
    )
    
    # ── Title ──────────────────────────────────────────────
    fig.text(
        0.5, 0.95,
        'Real-Time Crypto Price Monitor',
        ha='center', va='top',
        fontsize=22, fontweight='bold',
        color='white'
    )
    fig.text(
        0.5, 0.91,
        'Live Pipeline: CoinGecko API → Cloud Function → Pub/Sub → BigQuery Streaming',
        ha='center', va='top',
        fontsize=11, color='#8B949E'
    )
    fig.text(
        0.5, 0.875,
        f'Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  |  '
        f'Total records: {len(df)}  |  '
        f'Project: GCP Week 5',
        ha='center', va='top',
        fontsize=9, color='#6E7681'
    )

    # ── Panel 1, 2, 3 — Price Scorecards ──────────────────
    coins = ["bitcoin", "ethereum", "solana"]
    
    for i, coin in enumerate(coins):
        ax = fig.add_subplot(gs[0, i])
        ax.set_facecolor('#161B22')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        if coin in latest:
            row         = latest[coin]
            price       = row['price_usd']
            change      = row['change_24h_pct']
            volume      = row['volume_24h']
            color       = COIN_COLORS[coin]
            symbol      = COIN_SYMBOLS[coin]
            change_color = '#3FB950' if change >= 0 else '#F85149'
            change_arrow = '▲' if change >= 0 else '▼'

            # Coin name
            ax.text(0.5, 0.85, symbol, ha='center', va='center',
                   fontsize=18, fontweight='bold', color=color)
            ax.text(0.5, 0.68, coin.capitalize(), ha='center',
                   fontsize=9, color='#8B949E')
            
            # Price
            if price >= 1000:
                price_str = f"${price:,.0f}"
            else:
                price_str = f"${price:,.2f}"
            ax.text(0.5, 0.48, price_str, ha='center', va='center',
                   fontsize=20, fontweight='bold', color='white')
            
            # 24h change
            ax.text(0.5, 0.28, f"{change_arrow} {abs(change):.2f}% (24h)",
                   ha='center', fontsize=11,
                   color=change_color, fontweight='bold')
            
            # Volume
            vol_str = f"Vol: ${volume/1e9:.1f}B"
            ax.text(0.5, 0.12, vol_str, ha='center',
                   fontsize=8, color='#6E7681')

            # Colored border
            for spine in ax.spines.values():
                spine.set_edgecolor(color)
                spine.set_linewidth(2)
                spine.set_visible(True)

    # ── Panel 4 — Price History Time Series ───────────────
    ax4 = fig.add_subplot(gs[1, :])
    ax4.set_facecolor('#161B22')
    
    for coin in coins:
        coin_df = df[df['coin'] == coin].copy()
        if len(coin_df) > 0:
            ax4.plot(
                coin_df['ingested_at'],
                coin_df['price_usd'],
                color=COIN_COLORS[coin],
                linewidth=2,
                label=f"{COIN_SYMBOLS[coin]}",
                marker='o',
                markersize=3
            )
    
    ax4.set_facecolor('#161B22')
    ax4.tick_params(colors='#8B949E', labelsize=8)
    ax4.set_ylabel('Price (USD)', color='#8B949E', fontsize=9)
    ax4_right = ax4.twinx()
    for coin in ["ethereum", "solana"]:
        coin_df = df[df['coin'] == coin].copy()
        if len(coin_df) > 0:
            ax4_right.plot(
                coin_df['ingested_at'],
                coin_df['price_usd'],
                color=COIN_COLORS[coin],
                linewidth=2,
                label=COIN_SYMBOLS[coin],
                marker='o',
                markersize=3
            )
    ax4_right.tick_params(colors='#8B949E', labelsize=8)
    ax4_right.set_ylabel('ETH / SOL Price (USD)', color='#8B949E', fontsize=9)
    for spine in ax4_right.spines.values():
        spine.set_edgecolor('#30363D')
    ax4.set_title('Price History (All Collections)',
                 color='white', fontsize=11, pad=8)
    ax4.legend(facecolor='#0D1117', edgecolor='#30363D',
              labelcolor='white', fontsize=9)
    ax4.grid(True, alpha=0.15, color='#30363D')
    for spine in ax4.spines.values():
        spine.set_edgecolor('#30363D')
    plt.setp(ax4.xaxis.get_majorticklabels(), rotation=20, ha='right')

    # ── Panel 5 — 24h Change Bar Chart ────────────────────
    ax5 = fig.add_subplot(gs[2, :2])
    ax5.set_facecolor('#161B22')
    
    bar_coins   = []
    bar_changes = []
    bar_colors  = []
    
    for coin in coins:
        if coin in latest:
            bar_coins.append(COIN_SYMBOLS[coin])
            change = latest[coin]['change_24h_pct']
            bar_changes.append(change)
            bar_colors.append('#3FB950' if change >= 0 else '#F85149')
    
    bars = ax5.bar(bar_coins, bar_changes, color=bar_colors,
                  width=0.5, edgecolor='#30363D')
    
    # Add value labels on bars
    for bar, val in zip(bars, bar_changes):
        ax5.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + (0.05 if val >= 0 else -0.15),
            f'{val:+.2f}%',
            ha='center', va='bottom',
            color='white', fontsize=10, fontweight='bold'
        )
    
    ax5.set_facecolor('#161B22')
    ax5.tick_params(colors='#8B949E')
    ax5.set_ylabel('24h Change (%)', color='#8B949E', fontsize=9)
    ax5.set_title('24h Price Change by Coin',
                 color='white', fontsize=11, pad=8)
    ax5.axhline(y=0, color='#8B949E', linewidth=0.8, linestyle='--')
    ax5.grid(True, alpha=0.15, axis='y', color='#30363D')
    for spine in ax5.spines.values():
        spine.set_edgecolor('#30363D')

    # ── Panel 6 — Volume Bar Chart ────────────────────────
    ax6 = fig.add_subplot(gs[2, 2])
    ax6.set_facecolor('#161B22')
    
    vol_coins  = []
    vol_values = []
    vol_colors = []
    
    for coin in coins:
        if coin in latest:
            vol_coins.append(COIN_SYMBOLS[coin])
            vol_values.append(latest[coin]['volume_24h'] / 1e9)
            vol_colors.append(COIN_COLORS[coin])
    
    bars2 = ax6.bar(vol_coins, vol_values, color=vol_colors,
                   width=0.5, edgecolor='#30363D')
    
    for bar, val in zip(bars2, vol_values):
        ax6.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.3,
            f'${val:.1f}B',
            ha='center', color='white',
            fontsize=9, fontweight='bold'
        )
    
    ax6.set_facecolor('#161B22')
    ax6.tick_params(colors='#8B949E')
    ax6.set_ylabel('Volume (Billions USD)', color='#8B949E', fontsize=9)
    ax6.set_title('24h Trading Volume',
                 color='white', fontsize=11, pad=8)
    ax6.grid(True, alpha=0.15, axis='y', color='#30363D')
    for spine in ax6.spines.values():
        spine.set_edgecolor('#30363D')

    # ── Footer ─────────────────────────────────────────────
    fig.text(
        0.5, 0.02,
        'Data Source: CoinGecko API  |  '
        'Pipeline: GCP Cloud Function + Pub/Sub + BigQuery  |  '
        'Portfolio: github.com/zeeshanyalakpalli',
        ha='center', fontsize=8, color='#6E7681'
    )

    # ── Save ───────────────────────────────────────────────
    output_path = 'dashboard/crypto_dashboard.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
               facecolor='#0D1117')
    print(f"\n✓ Dashboard saved to: {output_path}")
    print("✓ Ready for LinkedIn and GitHub!")
    plt.show()


if __name__ == "__main__":
    df = fetch_data_from_bigquery()
    
    if len(df) == 0:
        print("No data found! Run the Cloud Function first.")
    else:
        generate_dashboard(df)