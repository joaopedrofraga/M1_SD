import asyncio
import json

Menu = """
    ==============================
    1 - Enviar mensagem privada
    2 - Enviar mensagem para grupo
    3 - Mostrar estado local
    4 - Capturar estado global
    5 - Mostrar ordem global
    6 - Retornar ao menu
    0 - Sair
"""

async def ler_terminal(pergunta: str) -> str:
    return (await asyncio.to_thread(input, pergunta)).strip()

async def solicitar_mensagem(texto: str) -> str:
    if not texto.strip():
        raise ValueError("Mensagem não pode ser vazia")
    return texto.strip()

async def executar_opcao(no: No, opcao: str, perguntar=ler_terminal) -> bool:
    if opcao == "1":
        # Mensagem privada
        pass
    elif opcao == "2":
        # Mensagem em grupo
        pass
    elif opcao == "3":
        # Mostrar estado local
        pass
    elif opcao == "4":
        # Capturar estado global
        pass
    elif opcao == "5":
        # Mostrar ordem global
        pass
    elif opcao == "6":
        # Retornar ao menu
        pass
    elif opcao == "0":
        print("Saindo...")
        return False
    else:
        print("Opção inválida. Tente novamente.")
    return True
    
async def executar_interface(no: No) -> None:
    print("=======CHAT DISTRIBUÍDO=======")
    print(Menu)
    
    try:
        opcao = await ler_terminal(f"Nó {no.identificador} - Escolha uma opção: ")
        if not await executar_opcao(no, opcao):
            return
    except (ValueError, ConnectionError, RuntimeError) as erro:
        print(f"Erro: {erro}")
    except EOFError:
        print("Entrada encerrada. Saindo...")
        return