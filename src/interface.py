import asyncio
import json

from .no import No

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
        destino = int(await perguntar("ID do destino: "))
        texto = await solicitar_mensagem(await perguntar("Mensagem: "))
        await no.enviar_privada(destino, texto)
    elif opcao == "2":
        # Mensagem em grupo
        texto = await solicitar_mensagem(await perguntar("Mensagem: "))
        await no.enviar_grupo(texto)
    elif opcao == "3":
        # Mostrar estado local
        print(json.dumps(no.estado(), ensure_ascii=False, indent=2))
    elif opcao == "4":
        # Capturar estado global
        await no.iniciar_captura()
        print("Captura solicitada; aguarde o resultado.")
    elif opcao == "5":
        # Mostrar ordem global
        print(json.dumps(no.ordem_global, ensure_ascii=False, indent=2))
    elif opcao == "6":
        # Retornar ao menu
        print(Menu)
        pass
    elif opcao == "0":
        await no.encerrar()
        return False
    else:
        print("Opção inválida. Tente novamente.")
    return True
    
async def executar_interface(no: No) -> None:
    print("=======CHAT DISTRIBUÍDO=======")
    print(Menu)
    
    while no.ativo:
        try:
            opcao = await ler_terminal(f"Nó {no.identificador} - Escolha uma opção: ")
            if not await executar_opcao(no, opcao):
                break
        except (ValueError, ConnectionError, RuntimeError) as erro:
            print(f"Erro: {erro}")
        except EOFError:
            await no.encerrar()
            break