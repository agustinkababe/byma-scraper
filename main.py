from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from auth import authenticate_user, create_access_token
from byma_source import router as byma_router
from alpha_source import router as alpha_router
from datetime import timedelta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cambiar por dominio específico en prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Token de autenticación
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")
    access_token = create_access_token(
        data={"sub": user["username"]},
        expires_delta=timedelta(minutes=60)
    )
    return {"access_token": access_token, "token_type": "bearer"}

# Registrar routers
app.include_router(byma_router)
app.include_router(alpha_router)
