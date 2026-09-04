class RelogioVetorial:
    def __init__(self, tamanho: int):
        self.valores = [0] * tamanho

    def incrementar(self, indice: int) -> list[int]:
        self.valores[indice] += 1
        return self.copia()

    def mesclar(self, outro: list[int]) -> None:
        if len(outro) != len(self.valores):
            raise ValueError("Tamanhos incompatíveis")
        self.valores = [max(atual, recebido) for atual, recebido in zip(self.valores, outro)]

    def copia(self) -> list[int]:
        return self.valores.copy()

def pode_entregar(marca: list[int], entregues: list[int], indice_origem: int) -> bool:
    if len(marca) != len(entregues):
        return False
    if marca[indice_origem] != entregues[indice_origem] + 1:
        return False
    
    for i in range(len(marca)):
        if i == indice_origem:
            continue
        if marca[i] > entregues[i]:
            return False
    return True