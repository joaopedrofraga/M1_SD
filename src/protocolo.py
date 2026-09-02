import asyncio
import json

def criar_mensagem(tipo: str, origem: int, **campos) -> dict:
    return {"tipo": tipo, "origem": origem, **campos}

def receber_json(leitor: asyncio.StreamReader) -> dict:
    linha = await leitor.readline()
    if not linha:
        raise ValueError("Mensagem vazia")
    return json.loads(linha)