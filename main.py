from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import httpx
import asyncio
from datetime import datetime
import logging

# Configuración de logging
logging.basicConfig(
    filename="byma_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()

GENERAL_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general"
COTIZACION_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/cotizacion"

async def fetch_data(symbol: str, client: httpx.AsyncClient):
    headers = {"Content-Type": "application/json"}
    general_payload = {"symbol": symbol}
    cotizacion_payload = {"symbol": symbol, "settlementType": "2"}

    forma_amortizacion = ""
    interes = ""
    fecha_emision = ""
    trade = "N/A"  # Default

    # Obtener datos generales
    try:
        general_response = await client.post(GENERAL_URL, json=general_payload, headers=headers)
        general_response.raise_for_status()
        general_data = general_response.json()["data"][0]
        forma_amortizacion = general_data.get("formaAmortizacion", "")
        interes = general_data.get("interes", "")
        fecha_emision = general_data.get("fechaEmision", "")
    except Exception as e:
        logging.warning(f"[{symbol}] Error al obtener datos generales: {e}")

    # Intentar obtener cotización hasta 1000 veces si hay 503
    for attempt in range(1, 1001):
        try:
            cotizacion_response = await client.post(COTIZACION_URL, json=cotizacion_payload, headers=headers)
            cotizacion_response.raise_for_status()
            cotizacion_data = cotizacion_response.json()["data"][0]
            trade = cotizacion_data.get("trade", "N/A")
            break
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 503:
                logging.warning(f"[{symbol}] Intento {attempt}/1000 - API cotización devolvió 503. Reintentando en 1 segundo...")
                await asyncio.sleep(1)
                continue
            else:
                logging.error(f"[{symbol}] Error inesperado de status: {e}")
                break
        except Exception as e:
            logging.error(f"[{symbol}] Error general en cotización: {e}")
            break

    if trade == "N/A":
        logging.info(f"[{symbol}] Se agotaron los 1000 intentos. Se marca trade como 'N/A'.")

    return {
        "fecha": datetime.now().strftime("%Y-%m-%d"),
        "symbol": symbol,
        "formaAmortizacion": forma_amortizacion,
        "interes": interes,
        "fechaEmision": fecha_emision,
        "trade": trade
    }

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    symbols = df["symbol"].dropna().unique()

    logging.info(f"Inicio de proceso para {len(symbols)} símbolos.")

    results = []
    async with httpx.AsyncClient(timeout=10) as client:
        tasks = [fetch_data(symbol, client) for symbol in symbols]
        results = await asyncio.gather(*tasks)

    output_df = pd.DataFrame(results)
    output = io.StringIO()
    output_df.to_csv(output, index=False)
    output.seek(0)

    logging.info("Proceso finalizado correctamente.")
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=bonos_output.csv"}
    )
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import pandas as pd
import io
import httpx
import asyncio
from datetime import datetime, timedelta
from jose import JWTError, jwt
import logging

# Configuración de logging
logging.basicConfig(
    filename="byma_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()

# Seguridad JWT
SECRET_KEY = "mi-clave-supersecreta"  # Cambiala en producción
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

fake_users_db = {
    "agustin": {
        "username": "agustin",
        "password": "1234"  # en producción usá hash
    }
}

def authenticate_user(username: str, password: str):
    user = fake_users_db.get(username)
    if not user or user["password"] != password:
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(status_code=401, detail="Token inválido", headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
        return username
    except JWTError:
        raise credentials_exception

# Endpoint de login → devuelve token
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")
    access_token = create_access_token(data={"sub": user["username"]}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"access_token": access_token, "token_type": "bearer"}

# Resto de la lógica igual que antes
GENERAL_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general"
COTIZACION_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/cotizacion"

async def fetch_data(symbol: str, client: httpx.AsyncClient):
    headers = {"Content-Type": "application/json"}
    general_payload = {"symbol": symbol}
    cotizacion_payload = {"symbol": symbol, "settlementType": "2"}

    forma_amortizacion = ""
    interes = ""
    fecha_emision = ""
    trade = "N/A"

    try:
        general_response = await client.post(GENERAL_URL, json=general_payload, headers=headers)
        general_response.raise_for_status()
        general_data = general_response.json()["data"][0]
        forma_amortizacion = general_data.get("formaAmortizacion", "")
        interes = general_data.get("interes", "")
        fecha_emision = general_data.get("fechaEmision", "")
    except Exception as e:
        logging.warning(f"[{symbol}] Error al obtener datos generales: {e}")

    for attempt in range(1, 1001):
        try:
            cotizacion_response = await client.post(COTIZACION_URL, json=cotizacion_payload, headers=headers)
            cotizacion_response.raise_for_status()
            cotizacion_data = cotizacion_response.json()["data"][0]
            trade = cotizacion_data.get("trade", "N/A")
            break
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 503:
                logging.warning(f"[{symbol}] Intento {attempt}/1000 - API cotización devolvió 503. Reintentando en 1 segundo...")
                await asyncio.sleep(1)
                continue
            else:
                logging.error(f"[{symbol}] Error inesperado de status: {e}")
                break
        except Exception as e:
            logging.error(f"[{symbol}] Error general en cotización: {e}")
            break

    if trade == "N/A":
        logging.info(f"[{symbol}] Se agotaron los 1000 intentos. Se marca trade como 'N/A'.")

    return {
        "fecha": datetime.now().strftime("%Y-%m-%d"),
        "symbol": symbol,
        "formaAmortizacion": forma_amortizacion,
        "interes": interes,
        "fechaEmision": fecha_emision,
        "trade": trade
    }

# Endpoint protegido
@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...), username: str = Depends(get_current_user)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    symbols = df["symbol"].dropna().unique()

    logging.info(f"{username} inició proceso para {len(symbols)} símbolos.")

    results = []
    async with httpx.AsyncClient(timeout=10) as client:
        tasks = [fetch_data(symbol, client) for symbol in symbols]
        results = await asyncio.gather(*tasks)

    output_df = pd.DataFrame(results)
    output = io.StringIO()
    output_df.to_csv(output, index=False)
    output.seek(0)

    logging.info("Proceso finalizado correctamente.")
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=bonos_output.csv"}
    )
