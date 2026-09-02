import asyncio
import json
from time import monotonic

from .configuracao import Configuracao
from .protocolo import criar_mensagem, enviar_json, receber_json
from .relogio import RelogioVetorial, pode_entregar

class No:
    def __init__(self, configuracao: Configuracao, identificador: int,
                 intervalo_batimento=1, limite_lider=3,
                 espera_eleicao=1, limite_captura=5, mostrar_saida=True):
        self.configuracao = configuracao
        self.identificador = identificador
        self.dados = configuracao.por_identificador(identificador)
        self.indices = {numero: posicao for posicao, numero in enumerate(configuracao.identificadores)}
        self.intervalo_batimento = intervalo_batimento
        self.limite_lider = limite_lider
        self.espera_eleicao = espera_eleicao
        self.limite_captura = limite_captura
        self.mostrar_saida = mostrar_saida