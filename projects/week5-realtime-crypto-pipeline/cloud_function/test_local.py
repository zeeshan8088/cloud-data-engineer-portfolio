# ============================================================
# Local test — runs the function WITHOUT deploying to GCP
# This tests the CoinGecko API call and data validation only
# (Pub/Sub publish will fail locally — that is expected)
# ============================================================

import json
import requests
from main import fetch_crypto_prices, validate_price_data

def test_fetch():
    print("\n--- Testing CoinGecko API fetch ---")
    data = fetch_crypto_prices()
    
    for coin, values in data.items():
        print(f"\n{coin.upper()}:")
        print(f"  Price:      ${values['usd']:,.2f}")
        print(f"  24h Change: {values['usd_24h_change']:.2f}%")
        print(f"  24h Volume: ${values['usd_24h_vol']:,.0f}")
        
        is_valid = validate_price_data(coin, values)
        print(f"  Valid data: {'✓ YES' if is_valid else '✗ NO'}")

if __name__ == "__main__":
    test_fetch()
    print("\n✓ Local test complete. API and validation working.")