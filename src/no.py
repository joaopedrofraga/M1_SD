import asyncio
import json
from time import monotonic

from .configuracao import Configuracao
from .protocolo import criar_mensagem, enviar_json, receber_json
from .relogio import RelogioVetorial, pode_entregar

INTERVALO_ENTRE_BATIMENTOS = 1
TEMPO_SEM_LIDER = 3
TEMPO_ESPERA_ELEICAO = 1
TEMPO_LIMITE_CAPTURA = 5


class No:
    def __init__(self, configuracao: Configuracao, identificador: int):
        configuracao.por_identificador(identificador)
        self.configuracao = configuracao
        self.identificador = identificador

        # Relógio causal e ordenação das mensagens de grupo.
        quantidade = len(configuracao.nos)
        self.relogio = RelogioVetorial(quantidade)
        self.vetor_ordenado = [0] * quantidade
        self.ordem_local = []
        self.ordem_global = []
        self.pedidos_aguardando_ordem = []
        self.mensagens_aguardando_entrega = {}
        self.trava_ordenacao = asyncio.Lock()

        # Eleição do líder e captura do estado global.
        self.lider = configuracao.maior_identificador
        self.instante_ultimo_batimento = monotonic()
        self.eleicao_em_andamento = False
        self.no_maior_respondeu = False
        self.captura_em_andamento = None
        self.ultima_captura = None

        # Controle da execução do nó.
        self.servidor = None
        self.tarefas = set()
        self.evento_encerramento = asyncio.Event()

    @property
    def ativo(self):
        return self.servidor is not None and self.servidor.is_serving()

    async def iniciar(self):
        dados = self.configuracao.por_identificador(self.identificador)
        self.servidor = await asyncio.start_server(
            self._aceitar, dados.endereco, dados.porta)
        self.evento_encerramento.clear()
        self._criar_tarefa(self._ciclo_do_lider())
        print(f"Nó {self.identificador} ativo; lider {self.lider}")

    async def encerrar(self):
        if not self.ativo:
            return
        self.servidor.close()
        for tarefa in list(self.tarefas):
            tarefa.cancel()
        await asyncio.gather(*self.tarefas, return_exceptions=True)
        await asyncio.sleep(0.05)
        await self.servidor.wait_closed()
        self.evento_encerramento.set()

    async def aguardar(self):
        await self.evento_encerramento.wait()

    def _criar_tarefa(self, rotina):
        tarefa = asyncio.create_task(rotina)
        self.tarefas.add(tarefa)
        tarefa.add_done_callback(self.tarefas.discard)

    async def _aceitar(self, leitor, escritor):
        try:
            await self.processar(await receber_json(leitor))
        except (ValueError, json.JSONDecodeError) as erro:
            print(f"Erro ao processar mensagem: {erro}")
        finally:
            escritor.close()
            await escritor.wait_closed()
            
    async def enviar(self, destino, mensagem):
        if destino == self.identificador:
            await self.processar(mensagem)
            return True
        dados = self.configuracao.por_identificador(destino)
        return await enviar_json(dados.endereco, dados.porta, mensagem)
    
    async def difundir(self, mensagem):
        # Registra no próprio nó antes de enviar aos demais.
        await self.processar(mensagem)
        return await asyncio.gather(*(
            self.enviar(destino, mensagem)
            for destino in self.configuracao.identificadores
            if destino != self.identificador
        ))
        
    async def enviar_privada(self, destino, texto):
        self.configuracao.por_identificador(destino)
        self._registrar("envio_privado", texto, destino)
        if not await self.enviar(destino, criar_mensagem(
                "PRIVADA", self.identificador, destino=destino, texto=texto)):
            raise ConnectionError(f"Nó {destino} indisponível")
        
    async def enviar_grupo(self, texto):
        vetor = self.relogio.incrementar(self.configuracao.identificadores.index(self.identificador))
        self._registrar("emissão de grupo", texto)
        pedido = criar_mensagem("PEDIDO_GRUPO", self.identificador, 
                                texto=texto, vetor=vetor)
        if not await self.enviar(self.lider, pedido):
            raise ConnectionError(f"Líder indisponível; Aguarde a eleição e tente novamente")
        
    async def iniciar_captura(self):
        if self.identificador == self.lider and self.captura_em_andamento:
            raise RuntimeError("Já existe uma captura em andamento")
        pedido = criar_mensagem("PEDIDO_CAPTURA", self.identificador,
                                solicitante=self.identificador)
        if not await self.enviar(self.lider, pedido):
            raise ConnectionError(f"Líder indisponível; Aguarde a eleição e tente novamente")
        
    async def processar(self, mensagem):
        match mensagem.get("tipo"):
            case "PRIVADA":
                self._registrar("recepção privada", mensagem["texto"], mensagem["origem"])
                print(f"Mensagem privada de {mensagem['origem']}: {mensagem['texto']}")
            case "PEDIDO_GRUPO":
                if self.identificador != self.lider:
                    await self.enviar(self.lider, mensagem)
                else:
                    self.pedidos_aguardando_ordem.append(mensagem)
                    async with self.trava_ordenacao:
                        await self._ordenar()            
            case "GRUPO":
                self.mensagens_aguardando_entrega[mensagem["sequencia"]] = mensagem
                self._entregar()
            case "BATIMENTO" | "LIDER":
                self.lider = mensagem["origem"]
                self.instante_ultimo_batimento = monotonic()
                self.eleicao_em_andamento = False
            case "ELEICAO":
                await self.enviar(mensagem["origem"], criar_mensagem("OK", self.identificador))
                if self.identificador == self.lider:
                    await self.enviar(mensagem["origem"], criar_mensagem("LIDER", self.identificador))
                elif not self.eleicao_em_andamento:
                    self._criar_tarefa(self._eleger())
            case "OK":
                self.no_maior_respondeu = True
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
                if self.captura_em_andamento:
                    self.captura_em_andamento["respostas"][str(mensagem["origem"])] = mensagem
                    if len (self.captura_em_andamento["respostas"]) == len(self.configuracao.nos):
                        await self._finalizar_captura(all(
                            resposta["atingiu"] for resposta in self.captura_em_andamento["respostas"].values()
                        ))
            case "ESTADO":
                self.ultima_captura = mensagem["resultado"]
                print(json.dumps(self.ultima_captura, ensure_ascii=False, indent=2))
            case tipo:
                print(f"Tipo desconhecido: {tipo}")

    async def _ordenar(self):
        while self.pedidos_aguardando_ordem:
            for pedido in self.pedidos_aguardando_ordem:
                if not pode_entregar(pedido["vetor"], self.vetor_ordenado,
                                    self.configuracao.identificadores.index(pedido["origem"])):
                    continue
                self.pedidos_aguardando_ordem.remove(pedido)
                await self.difundir(criar_mensagem("GRUPO", pedido["origem"],
                    texto=pedido["texto"], vetor=pedido["vetor"],
                    sequencia=len(self.ordem_global) + 1))
                break
            else:
                # Nenhum pedido tem suas dependências causais atendidas ainda.
                return

    def _entregar(self):
        proxima_sequencia = len(self.ordem_global) + 1
        while proxima_sequencia in self.mensagens_aguardando_entrega:
            mensagem = self.mensagens_aguardando_entrega.pop(proxima_sequencia)
            self.relogio.mesclar(mensagem["vetor"])
            self.vetor_ordenado = [max(a, b) for a, b in zip(
                self.vetor_ordenado, mensagem["vetor"])]
            registro = {chave: mensagem[chave] for chave in
                        ("sequencia", "origem", "texto", "vetor")}
            self.ordem_global.append(registro)
            self._registrar("entrega global", mensagem["texto"], mensagem["origem"])
            print(f"[global {proxima_sequencia}] {mensagem['texto']}")
            proxima_sequencia += 1

    async def _ciclo_do_lider(self):
        while self.ativo:
            if self.identificador == self.lider:
                await self.difundir(criar_mensagem("BATIMENTO", self.identificador))
            elif monotonic() - self.instante_ultimo_batimento > TEMPO_SEM_LIDER and not self.eleicao_em_andamento:
                self._criar_tarefa(self._eleger())
            await asyncio.sleep(INTERVALO_ENTRE_BATIMENTOS)

    async def _eleger(self):
        if self.eleicao_em_andamento or not self.ativo:
            return
        self.eleicao_em_andamento, self.no_maior_respondeu = True, False
        maiores = [numero for numero in self.configuracao.identificadores
                if numero > self.identificador]
        await asyncio.gather(*(self.enviar(numero, criar_mensagem(
            "ELEICAO", self.identificador)) for numero in maiores))
        await asyncio.sleep(TEMPO_ESPERA_ELEICAO)
        if not self.no_maior_respondeu and self.ativo:
            self.lider = self.identificador
            self.eleicao_em_andamento = False
            await self.difundir(criar_mensagem("LIDER", self.identificador))
            print(f"Nó {self.identificador} é o novo líder")
        else:
            self.eleicao_em_andamento = False
            self.instante_ultimo_batimento = monotonic()

    async def _capturar(self, solicitante):
        if self.captura_em_andamento:
            resultado = {"situacao": "ocupada", "corte": len(self.ordem_global), "estados": {}}
            await self.enviar(solicitante, criar_mensagem(
                "ESTADO", self.identificador, acao="resultado", resultado=resultado))
            return
        self.captura_em_andamento = {"solicitante": solicitante, "corte": len(self.ordem_global),
                            "respostas": {}}
        await self.difundir(criar_mensagem("PEDIR_ESTADO", self.identificador,
            corte=self.captura_em_andamento["corte"], lider=self.identificador))
        self._criar_tarefa(self._expirar_captura())

    async def _expirar_captura(self):
        await asyncio.sleep(TEMPO_LIMITE_CAPTURA)
        if self.captura_em_andamento:
            await self._finalizar_captura(False)

    async def _finalizar_captura(self, completa):
        captura, self.captura_em_andamento = self.captura_em_andamento, None
        resultado = {"situacao": "completa" if completa else "incompleta",
                    "corte": captura["corte"],
                    "estados": {numero: item["estado"]
                                for numero, item in captura["respostas"].items()}}
        await self.enviar(captura["solicitante"], criar_mensagem(
            "ESTADO", self.identificador, acao="resultado", resultado=resultado))

    def _registrar(self, evento, texto, outro=None):
        self.ordem_local.append({"numero": len(self.ordem_local) + 1,
            "evento": evento, "texto": texto, "outro": outro})

    def estado(self):
        return {"no": self.identificador, "lider": self.lider,
                "relogio": self.relogio.copia(),
                "ordem_local": [item.copy() for item in self.ordem_local],
                "ordem_global": [item.copy() for item in self.ordem_global],
                "buffer": [item.copy() for item in self.mensagens_aguardando_entrega.values()]}
