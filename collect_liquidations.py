import asyncio
import json
import websockets
import pandas as pd
import os
from datetime import datetime, date

# Konfigūracija
WS_URL = "wss://api.hyperliquid.xyz/ws"
DATA_DIR = "data/liquidations"
os.makedirs(DATA_DIR, exist_ok=True)

def archive_to_parquet(csv_path):
    if not os.path.exists(csv_path):
        return
    parquet_path = csv_path.replace(".csv", ".parquet")
    try:
        df = pd.read_csv(csv_path)
        if not df.empty:
            df.to_parquet(parquet_path, index=False, compression='snappy')
            os.remove(csv_path)
            print(f"📦 Likvidavimai suarchyvuoti: {parquet_path}")
    except Exception as e:
        print(f"❌ Likvidavimų archyvavimo klaida: {e}")

async def subscribe_liquidations():
    current_date = date.today()
    current_file = os.path.join(DATA_DIR, f"liquidations_{current_date}.csv")

    while True:
        try:
            async with websockets.connect(WS_URL) as websocket:
                await websocket.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "liquidations"}
                }))
                print(f"🔥 Likvidavimų botas paleistas ({current_date})")

                async for message in websocket:
                    today = date.today()
                    if today != current_date:
                        old_file = current_file
                        current_date = today
                        current_file = os.path.join(DATA_DIR, f"liquidations_{current_date}.csv")
                        asyncio.create_task(asyncio.to_thread(archive_to_parquet, old_file))

                    data = json.loads(message)
                    if "channel" in data and data["channel"] == "liquidations":
                        liq_data = data["data"]
                        entry = {
                            "local_time": datetime.now().isoformat(),
                            "coin": liq_data.get("liquidatedAsset"),
                            "px": liq_data.get("liquidatedPx"),
                            "sz": liq_data.get("liquidatedSize"),
                            "side": "S" if liq_data.get("method") == "sell" else "B",
                            "user": liq_data.get("liquidatedUser"),
                            "hash": liq_data.get("hash")
                        }
                        
                        df = pd.DataFrame([entry])
                        if not os.path.isfile(current_file):
                            df.to_csv(current_file, index=False)
                        else:
                            df.to_csv(current_file, mode='a', header=False, index=False)
                        
                        print(f"💥 LIKVIDAVIMAS: {entry['coin']} | {entry['side']} | {entry['sz']} @ {entry['px']}")

        except Exception as e:
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(subscribe_liquidations())
