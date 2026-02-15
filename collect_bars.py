import asyncio
import json
import websockets
import pandas as pd
import os
from datetime import datetime, date
import requests
import time

# Konfigūracija
WS_URL = "wss://api.hyperliquid.xyz/ws"
API_URL = "https://api.hyperliquid.xyz/info"
BASE_DATA_DIR = "data/bars"
EXTRA_COINS = ["BTC", "ETH", "SOL"]
RUN_LIMIT_SECONDS = 5.5 * 3600  # 5.5 valandos (GitHub Actions limitas)

os.makedirs(os.path.join(BASE_DATA_DIR, "all"), exist_ok=True)
for coin in EXTRA_COINS:
    os.makedirs(os.path.join(BASE_DATA_DIR, coin), exist_ok=True)

def get_active_coins():
    try:
        payload = {"type": "meta"}
        response = requests.post(API_URL, json=payload)
        return [asset["name"] for asset in response.json()["universe"]]
    except:
        return ["BTC", "ETH", "SOL"]

async def collect_bars():
    start_time = time.time()
    coins = get_active_coins()
    current_date = date.today()
    
    def get_file_paths(d):
        paths = {"all": os.path.join(BASE_DATA_DIR, "all", f"all_coins_{d}.csv")}
        for c in EXTRA_COINS:
            paths[c] = os.path.join(BASE_DATA_DIR, c, f"{c}_1m_{d}.csv")
        return paths

    current_files = get_file_paths(current_date)
    print(f"Cloud Bot paleistas. Trukmė: 5.5h. Monetų: {len(coins)}")

    while time.time() - start_time < RUN_LIMIT_SECONDS:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as websocket:
                for coin in coins:
                    await websocket.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "candle", "coin": coin, "interval": "1m"}
                    }))
                
                async for message in websocket:
                    # Ar laikas baigėsi?
                    if time.time() - start_time > RUN_LIMIT_SECONDS:
                        print("Laikas baigėsi. Ruošiamės commitui.")
                        return

                    data = json.loads(message)
                    if "channel" in data and data["channel"] == "candle":
                        candle = data["data"]
                        symbol = candle['s']
                        entry = {
                            "instrument_id": f"{symbol}-USD-PERP.HYPERLIQUID",
                            "timestamp": candle["t"],
                            "open": candle["o"], "high": candle["h"], "low": candle["l"], "close": candle["c"], "volume": candle["v"]
                        }
                        
                        df = pd.DataFrame([entry])
                        f_all = current_files["all"]
                        df.to_csv(f_all, mode='a', header=not os.path.exists(f_all), index=False)
                        
                        if symbol in EXTRA_COINS:
                            f_extra = current_files[symbol]
                            df.to_csv(f_extra, mode='a', header=not os.path.exists(f_extra), index=False)
                            print(f"⭐ {symbol} | {candle['c']}")

        except Exception as e:
            print(f"Reconnect... {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(collect_bars())
