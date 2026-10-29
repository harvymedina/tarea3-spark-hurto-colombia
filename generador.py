"""Generador de datos: simula hurtos que llegan en tiempo real.
Imprime un evento JSON por línea; se conecta a Kafka con kafka-console-producer."""
import json
import random
import sys
import time
from datetime import datetime

# Proporciones tomadas del análisis batch (resultados reales)
DEPARTAMENTOS = {
    ("BOGOTA", "BOGOTÁ D.C."): 290,
    ("ANTIOQUIA", "MEDELLÍN"): 230,
    ("VALLE", "CALI"): 163,
    ("ATLÁNTICO", "BARRANQUILLA"): 60,
    ("CUNDINAMARCA", "SOACHA"): 51,
    ("CAUCA", "POPAYÁN"): 48,
    ("SANTANDER", "BUCARAMANGA"): 48,
    ("META", "VILLAVICENCIO"): 38,
}
TIPOS = {
    "HURTO ENTIDADES COMERCIALES": 583,
    "HURTO MOTOCICLETAS": 498,
    "HURTO AUTOMOTORES": 159,
    "HURTO RESIDENCIAS": 70,
}
ARMAS = {
    "SIN EMPLEO DE ARMAS": 630,
    "ARMA DE FUEGO": 268,
    "LLAVE MAESTRA": 231,
    "CONTUNDENTES": 84,
    "ARMA BLANCA / CORTOPUNZANTE": 29,
}

pausa = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5  # segundos entre eventos


def elegir(pesos):
    return random.choices(list(pesos), weights=list(pesos.values()))[0]


while True:
    depto, muni = elegir(DEPARTAMENTOS)
    evento = {
        "fecha_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "departamento": depto,
        "municipio": muni,
        "tipo_hurto": elegir(TIPOS),
        "arma_medio": elegir(ARMAS),
        "cantidad": 1,
    }
    print(json.dumps(evento, ensure_ascii=False), flush=True)
    time.sleep(pausa)
