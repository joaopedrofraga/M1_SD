from dataclasses import dataclass
from json import loads
from pathlib import Path

@dataclass(frozen=True)
class DadosNo:
    numero: int
    endereco: str
    porta: int

@dataclass(frozen=True)
class Configuracao:
    execucao: str
    nos: tuple[DadosNo, ...]

    @property
    def identificadores(self) -> tuple[int, ...]:
        return tuple(no.numero for no in self.nos)

    @property
    def maior_identificador(self) -> int:
        return max(self.identificadores)

    def por_identificador(self, numero: int) -> DadosNo:
        for no in self.nos:
            if no.numero == numero:
                return no
        raise ValueError(f"Nó com identificador {numero} não encontrado")

def carregar_configuracao(caminho: str | Path) -> Configuracao:
    bruto = loads(Path(caminho).read_text())
    nos = tuple(DadosNo(numero=no["numero"], endereco=no["endereco"], porta=no["porta"]) for no in bruto["nos"])
    identificadores = [no.numero for no in nos]
    portas = [no.porta for no in nos]
    if not bruto.get("execucao") or not nos:
        raise ValueError("Configuração inválida")
    if any(numero <= 0 for numero in identificadores) or len(identificadores) != len(set(identificadores)):
        raise ValueError("IDs devem ser positivos e únicos")
    if any(porta < 1 or porta > 65535 for porta in portas) or len(portas) != len(set(portas)):
        raise ValueError("Portas devem ser válidas e únicas")
    return Configuracao(str(bruto["execucao"]), nos)