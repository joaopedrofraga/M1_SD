import asyncio
import json

def criar_mensagem(tipo: str, origem: int, **campos) -> dict:
    return {"tipo": tipo, "origem": origem, **campos}

async def receber_json(leitor: asyncio.StreamReader) -> dict:
    linha = await leitor.readline()
    if not linha:
        raise ValueError("Mensagem vazia")
    return json.loads(linha)

async def enviar_json(endereco: str, porta: int, mensagem: dict) -> bool:
    try:
        _, escritor = await asyncio.wait_for(
            asyncio.open_connection(endereco, porta), timeout=1
        )
        escritor.write((json.dumps(mensagem) + "\n").encode())
        await escritor.drain()
        escritor.close()
        await escritor.wait_closed()
        return True
    except (OSError, asyncio.TimeoutError):
        return False