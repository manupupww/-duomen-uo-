import os
import pandas as pd
from datetime import datetime, timedelta

DIRS = ["data/bars/all", "data/bars/BTC", "data/bars/ETH", "data/bars/SOL", "data/liquidations"]

def archive():
    # Suarchyvuojame vakar dienos duomenis
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"Ieškome failų archyvavimui už: {yesterday_str}")

    for d in DIRS:
        if not os.path.exists(d): continue
        
        for f in os.listdir(d):
            if yesterday_str in f and f.endswith(".csv"):
                csv_path = os.path.join(d, f)
                parquet_path = csv_path.replace(".csv", ".parquet")
                
                try:
                    print(f"📦 Konvertuojame {f} į Parquet...")
                    df = pd.read_csv(csv_path)
                    if not df.empty:
                        df.to_parquet(parquet_path, index=False, compression='snappy')
                        os.remove(csv_path)
                        print(f"✅ Sėkmingai suarchyvuota.")
                except Exception as e:
                    print(f"❌ Klaida apdorojant {f}: {e}")

if __name__ == "__main__":
    archive()
