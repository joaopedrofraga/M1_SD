import asyncio
import json

def criar_mensagem(tipo: str, origem: int, **campos) -> dict:
    return {"tipo": tipo, "origem": origem, **campos}

