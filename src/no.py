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

        quantidade = len(configuracao.nos)
        self.relogio = RelogioVetorial(quantidade)
        self.vetor_sequenciado = [0] * quantidade
        self.lider = configuracao.maior_identificador
        self.sequencia = 0
        self.proxima_sequencia = 1
        self.ordem_local, self.ordem_global, self.mensagens_privadas = [], [], []
        self.pedidos_pendentes, self.mensagens_pendentes = [], {}
        self.captura_atual = None
        self.ultima_captura = None

        self.ultimo_batimento = monotonic()
        self.em_eleicao = self.recebeu_ok = self.ativo = False
        self.servico = None
        self.tarefas = set()
        self.trava_ordem = asyncio.Lock()
        self.evento_fim = asyncio.Event()

        ######

        async def iniciar(self):
            self.servidor = await asyncio.start_server(
                self._aceitar, self.dados.enderco, self.dados.porta)
            self.ativo = True
            self._criar_tarefa(self._ciclo_do_lider())
            self._mostrar(f"Nó {self.identificador} ativo; lider {self.lider}")

        async def encerrar(self):
            if not self.ativos:
                return
            self.ativo = False
            for tarefa in list(self.tarefas):
                tarefa.cancel()
            await asyncio.gather(*self.tarefas, return_exceptions=True)
            await asyncio.sleep(0.05)
            self.servidor.close()
            await self.servidor.wait_closed()
            self.evento_fim.set()

        async def aguardar(self):
            await self.evento_fim.wait()