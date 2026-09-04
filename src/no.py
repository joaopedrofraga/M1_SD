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

        def _criar_tarefa(self, rotina):
            tarefa = asyncio.create_task(rotina)
            self.tarefas.add(tarefa)
            tarefa.add_done_callback(self.tarefas.discard)

        async def _aceitar(self, leitor, escritor):
            try:
                await self.processar(await receber_json(leitor))
            except (ValueError, json.JSONDecodeError) as erro:
                self.mostrar(f"Erro ao processar mensagem: {erro}")
            finally:
                escritor.close()
                await escritor.wait_closed()
                
        async def enviar(self, destino, mensagem):
            if destino == self.identificador:
                await self.processar(mensagem)
                return True
            dados = self.configuracao.por_identificador(destino)
            return await enviar_json(dados.endereco, dados.porta, mensagem)
        
        async def difundir (self, mensagem):
            return await asyncio.gather(*(
                self.enviar(destino, mensagem)
                for destino in self.configuracao.identificadores
            ))
            
        async def enviar_privada(self, destino, texto):
            self.configuracao.por_identificador(destino)
            self._registrar("envio_privado", texto, destino)
            if not await self.enviar(destino, criar_mensagem(
                    "PRIVADA", self.identificador, destino=destino, texto=texto)):
                raise ConnectionError(f"Nó {destino} indisponível")
            
        async def enviar_grupo(self, texto):
            vetor = self.relogio.incrementar(self.indices[self.identificador])
            self._registrar("emissão de grupo", texto)
            pedido = criar_mensagem("PEDIDO_GRUPO", self.identificador, 
                                    texto=texto, vetor=vetor)
            if not await self.enviar(self.lider, pedido):
                raise ConnectionError(f"Líder indisponível; Aguarde a eleição e tente novamente")
            
        async def iniciar_captura(self):
            if self.identificador == self.lider and self.captura_atual:
                raise RuntimeError("Já existe uma captura em andamento")
            pedido = criar_mensagem("PEDIDO_CAPTURA", self.identificador,
                                    solicitante=self.identificador)
            if not await self.enviar(self.lider, pedido):
                raise ConnectionError(f"Líder indisponível; Aguarde a eleição e tente novamente")
            
        async def processar(self, mensagem):
            match mensagem.get("tipo"):
                case "PRIVADA":
                    self.mensagens_privadas.append(mensagem)
                    self._registrar("recepção privada", mensagem["texto"], mensagem["origem"])
                    self._mostrar(f"Mensagem privada de {mensagem['origem']}: {mensagem['texto']}")
                case "PEDIDO_GRUPO":
                    if self.identificador != self.lider:
                        await self.enviar(self.lider, mensagem)
                    else:
                        self.pedidos_pendentes.append(mensagem)
                        async with self.trava_ordem:
                            await self._ordenar()            
                case "GRUPO":
                    self.mensagens_pendentes[mensagem["sequencia"]] = mensagem
                    self._entregar()
                case "BATIMENTO":
                    self.lider = mensagem["origem"]
                    self.ultimo_batimento = monotonic()
                    self.em_eleicao = False
                case "ELEICAO":
                    await self.enviar(mensagem["origem"], criar_mensagem("OK", self.identificador))
                    if self.identificador == self.lider:
                        await self.enviar(mensagem["origem"], criar_mensagem("LIDER", self.identificador))
                    elif not self.em_eleicao:
                        self._criar_tarefa(self._eleger())
                case "OK":
                    self.recebeu_ok = True
                case "LIDER":
                    self.lider = mensagem["origem"]
                    self.ultimo_batimento = monotonic()
                    self.em_eleicao = False
                case "PEDIDO_CAPTURA":
                    if self.identificador != self.lider:
                        await self.enviar(self.lider, mensagem)
                    else:
                        await self._capturar(mensagem["solicitante"])
                case "PEDIR_ESTADO":
                    while self.ativo and len(self.ordem_local) < mensagem["corte"]:
                        await asyncio.sleep(0.2)
                    resposta = criar_mensagem("ESTADO", self.identificador,
                                              acao="resposta", atingiu=len(self.ordem_global) >= mensagem["corte"],
                                              estado=self.estado())
                    await self.enviar(mensagem["lider"], resposta)
                case "ESTADO" if mensagem["acao"] == "resposta":
                    if self.captura_atual:
                        self.captura_atual["respostas"][str(mensagem["origem"])] = mensagem
                        if len (self.captura_atual["respostas"]) == len(self.configuracao.nos):
                            await self._finalizar_captura(all(
                                resposta["atingiu"] for resposta in self.captura_atual["respostas"].values()
                            ))
                case "ESTADO":
                    self.ultima_captura = mensagem["resultado"]
                    self._mostrar(json.dumps(self.ultima_captura, ensure_ascii=False, indent=2))
                case tipo:
                    self._mostrar(f"Tipo desconhecido: {tipo}")

        async def _ordenar(self):
            mudou = True
            while mudou:
                mudou = False
                for pedido in self.pedidos_pendentes.copy():
                    if not pode_entregar(pedido["vetor"], self.vetor_sequenciado,
                                        self.indices[pedido["origem"]]):
                        continue
                    self.pedidos_pendentes.remove(pedido)
                    self.vetor_sequenciado = [max(a, b) for a, b in zip(
                        self.vetor_sequenciado, pedido["vetor"])]
                    self.sequencia += 1
                    await self.difundir(criar_mensagem("GRUPO", pedido["origem"],
                        texto=pedido["texto"], vetor=pedido["vetor"], sequencia=self.sequencia))
                    mudou = True
                    break

        def _entregar(self):
            while self.proxima_sequencia in self.mensagens_pendentes:
                mensagem = self.mensagens_pendentes.pop(self.proxima_sequencia)
                self.relogio.mesclar(mensagem["vetor"])
                self.vetor_sequenciado = [max(a, b) for a, b in zip(
                    self.vetor_sequenciado, mensagem["vetor"])]
                registro = {chave: mensagem[chave] for chave in
                            ("sequencia", "origem", "texto", "vetor")}
                self.ordem_global.append(registro)
                self._registrar("entrega global", mensagem["texto"], mensagem["origem"])
                self._mostrar(f"[global {self.proxima_sequencia}] {mensagem['texto']}")
                self.proxima_sequencia += 1

        async def _ciclo_do_lider(self):
            while self.ativo:
                if self.identificador == self.lider:
                    await self.difundir(criar_mensagem("BATIMENTO", self.identificador))
                elif monotonic() - self.ultimo_batimento > self.limite_lider and not self.em_eleicao:
                    self._criar_tarefa(self._eleger())
                await asyncio.sleep(self.intervalo_batimento)

        async def _eleger(self):
            if self.em_eleicao or not self.ativo:
                return
            self.em_eleicao, self.recebeu_ok = True, False
            maiores = [numero for numero in self.configuracao.identificadores
                    if numero > self.identificador]
            await asyncio.gather(*(self.enviar(numero, criar_mensagem(
                "ELEICAO", self.identificador)) for numero in maiores))
            await asyncio.sleep(self.espera_eleicao)
            if not self.recebeu_ok and self.ativo:
                self.lider = self.identificador
                self.sequencia = len(self.ordem_global)
                self.em_eleicao = False
                await self.difundir(criar_mensagem("LIDER", self.identificador))
                self._mostrar(f"Nó {self.identificador} é o novo líder")
            else:
                self.em_eleicao = False
                self.ultimo_batimento = monotonic()

        async def _capturar(self, solicitante):
            if self.captura_atual:
                resultado = {"situacao": "ocupada", "corte": self.sequencia, "estados": {}}
                await self.enviar(solicitante, criar_mensagem(
                    "ESTADO", self.identificador, acao="resultado", resultado=resultado))
                return
            self.captura_atual = {"solicitante": solicitante, "corte": self.sequencia,
                                "respostas": {}}
            await self.difundir(criar_mensagem("PEDIR_ESTADO", self.identificador,
                corte=self.sequencia, lider=self.identificador))
            self._criar_tarefa(self._expirar_captura())

        async def _expirar_captura(self):
            await asyncio.sleep(self.limite_captura)
            if self.captura_atual:
                await self._finalizar_captura(False)

        async def _finalizar_captura(self, completa):
            captura, self.captura_atual = self.captura_atual, None
            resultado = {"situacao": "completa" if completa else "incompleta",
                        "corte": captura["corte"],
                        "estados": {numero: item["estado"]
                                    for numero, item in captura["respostas"].items()}}
            await self.enviar(captura["solicitante"], criar_mensagem(
                "ESTADO", self.identificador, acao="resultado", resultado=resultado))

        def _registrar(self, evento, texto, outro=None):
            self.ordem_local.append({"numero": len(self.ordem_local) + 1,
                "evento": evento, "texto": texto, "outro": outro})

        def _mostrar(self, texto):
            if self.mostrar_saida:
                print(texto)

        def estado(self):
            return {"no": self.identificador, "lider": self.lider,
                    "relogio": self.relogio.copia(),
                    "ordem_local": [item.copy() for item in self.ordem_local],
                    "ordem_global": [item.copy() for item in self.ordem_global],
                    "buffer": [item.copy() for item in self.mensagens_pendentes.values()]}