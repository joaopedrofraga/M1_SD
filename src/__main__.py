import argparse
import asyncio

from .configuracao import carregar_configuracao
from .interface import executar_interface
from .no import No

def criar_argumentos() -> argparse.ArgumentParser:
    analisador = argparse.ArgumentParser(description="Chat distribuído didático")
    analisador.add_argument("--id", type=int, required=True, help="identificador deste nó")
    analisador.add_argument("--config", required=True, help="arquivo JSON com os nós")
    analisador.add_argument(
        "--sem-interface", action="store_true", help="mantém o nó apenas recebendo mensagens"
    )
    return analisador


async def executar(argumentos) -> None:
    configuracao = carregar_configuracao(argumentos.config)
    no = No(configuracao, argumentos.id)
    await no.iniciar()
    try:
        if argumentos.sem_interface:
            await no.aguardar()
        else:
            await executar_interface(no)
    finally:
        await no.encerrar()


def principal() -> int:
    try:
        asyncio.run(executar(criar_argumentos().parse_args()))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(principal())