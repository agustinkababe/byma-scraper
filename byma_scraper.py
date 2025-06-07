import requests
import pandas as pd
from datetime import datetime

def obtener_bonos_y_guardar_csv():
    url = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/public-bonds"
    
    # Body según lo que me pasaste
    body = {
        "T1": True,
        "T0": False,
        "Content-Type": "application/json, text/plain"
    }
    
    headers = {
        "Content-Type": "application/json",
        "Referer": "https://open.bymadata.com.ar/",
        "Origin": "https://open.bymadata.com.ar",
        "User-Agent": "Mozilla/5.0"
    }
    
    response = requests.post(url, json=body, headers=headers)
    
    if response.status_code != 200:
        print(f"Error al obtener datos: {response.status_code}")
        print(response.text)
        return
    
    data = response.json().get("data", [])
    
    if not data:
        print("No se encontraron datos.")
        return

    # Procesar data
    rows = []
    for item in data:
        rows.append({
            "symbol": item.get("symbol"),
            "trade": item.get("trade"),
            "date": datetime.now().strftime('%Y-%m-%d')
        })

    df = pd.DataFrame(rows)
    archivo_salida = "bonos_byma.csv"
    df.to_csv(archivo_salida, index=False)
    print(f"Archivo CSV generado: {archivo_salida}")

if __name__ == "__main__":
    obtener_bonos_y_guardar_csv()
