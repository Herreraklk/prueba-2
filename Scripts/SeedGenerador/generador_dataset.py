"""
GENERADOR DE DATASET - RetailTech Chile

Este script genera un dataset de 100,000 registros para el proyecto Big Data Solemne 1.

El dataset simula operaciones de una empresa retail multicanal que opera en 5 ciudades de Chile.

CAMBIOS RESPECTO AL SEED ORIGINAL:

1. Se agrega campo 'dias_entrega' (0-30 días)
   - En_Bodega: 0-3 días (envíos que aún no salen del almacén)
   - En_Ruta: 2-7 días (envíos en tránsito)
   - Entregado: 3-15 días (envíos completados exitosamente)
   - Retrasado: 15-30 días (envíos con problemas de tiempo)

2. Se agrega campo 'tramo_regional' (derivado de ciudad)
   - Santiago y Valparaíso a Centro
   - Concepción a Sur
   - Antofagasta y La Serena a Norte

3. Se agrega campo 'costo_soporte' (variable por tipo de ticket)
   - Consulta: 0-50 (atención básica)
   - Reclamo_Garantía: 50-200 (gestión de garantía)
   - Devolución: 100-500 (costo de devolución y reposición)
   - Facturación: 0-100 (gestión documental)

4. Fechas futuras para tickets de soporte
   - Los tickets se generan 1-30 días después de la venta original
   - Simula que el cliente contacta soporte después de la transacción

5. Registros de cliente únicos por usuario
   - Cada usuario solo aparece una vez en REGISTRO_CLIENTE
   - Se mantiene un set de usuarios registrados para evitar duplicados
"""


import csv
import random
from datetime import datetime, timedelta

# Número total de registros a generar
NUM_REGISTROS = 100000

# Fecha de inicio para la generación de datos
# El 2025-01-01 es el inicio del rango de fechas para las ventas
FECHA_INICIO = datetime(2025, 1, 1)

# Tipos de evento disponibles, cada tipo representa una operación diferente de la empresa
TIPOS_EVENTO = ["TRANSACCION_VENTA", "LOGISTICA_ENVIO",
                "SOPORTE_TICKET", "REGISTRO_CLIENTE"]

# Distribución porcentual de eventos dado que 40% ventas, 30% logística, 20% soporte, 10% registros
DISTRIBUCION_EVENTOS = [40, 30, 20, 10]

# Categorías de producto disponibles para ventas y envíos
CATEGORIAS_PRODUCTO = ["Electronica", "Hogar",
                       "Moda", "Deportes", "Alimentacion"]

# Ciudades donde opera la empresa
CIUDADES = ["Santiago", "Valparaiso", "Concepcion", "Antofagasta", "La Serena"]

# Mapeo de ciudad a tramo regional, usado para el campo 'tramo_regional'
# Este mapeo permite hacer análisis por región en las consultas Hive/Impala
MAPEO_TRAMO_REGIONAL = {
    "Santiago": "Centro",
    "Valparaiso": "Centro",
    "Concepcion": "Sur",
    "Antofagasta": "Norte",
    "La Serena": "Norte"
}

# Canales de venta disponibles
CANALES = ["Mobile App", "Portal Web B2C", "Sucursal Fisica"]

# Pesos para cada estado de logística, determina la probabilidad de cada estado.
# Estos pesos crean una distribución realista: mayoría de envíos entregados.
PESOS_ESTADO_LOGISTICA = {
    "En_Bodega": 10,    # 10% de envíos en bodega (recién ingresados)
    "En_Ruta": 15,      # 15% de envíos en tránsito
    "Entregado": 55,    # 55% de envíos entregados (mayoría)
    "Retrasado": 20     # 20% de envíos retrasados (problemas logísticos)
}

# Rango de días por estado de logística, cada estado tiene un rango diferente de días
# Esto permite correlacionar el estado con el tiempo de entrega
RANGO_DIAS_POR_ESTADO = {
    "En_Bodega": (0, 3),     # 0 a 3 días (aún no sale del almacén)
    "En_Ruta": (2, 7),       # 2 a 7 días (en tránsito normal)
    "Entregado": (3, 15),    # 3 a 15 días (entrega completada)
    "Retrasado": (15, 30)    # 15 a 30 días (entrega retrasada)
}

# Rango de costos por tipo de ticket de soporte
# El costo representa el impacto económico para la empresa
COSTOS_POR_TIPO_SOPORTE = {
    "Consulta": (0, 50),  # 0 a 50 (atención básica, bajo costo)
    "Reclamo_Garantia": (50, 200),  # 50 a 200 (gestión de garantía)
    "Devolucion": (100, 500),  # 100 a 500 (devolución y reposición)
    "Facturacion": (0, 100)  # 0 a 100 (gestión documental)
}

# Rango de días para fechas futuras de soporte
# Los tickets se generan después de la venta original
RANGO_DIAS_FUTURO_SOPORTE = (1, 30)


def calcular_dias_entrega(estado):
    """
    Calcula los días de entrega basado en el estado del envío.

    Cada estado tiene un rango diferente de días:
    - En_Bodega: 0-3 días (aún no sale)
    - En_Ruta: 2-7 días (en tránsito)
    - Entregado: 3-15 días (completado)
    - Retrasado: 15-30 días (retrasado)

    Args, un args es un diccionario que contiene el estado del envío y devuelve un número aleatorio de días dentro del rango correspondiente:
        estado (str): Estado del envío

    Returns:
        int: Número de días de entrega
    """
    min_dias, max_dias = RANGO_DIAS_POR_ESTADO[estado]
    return random.randint(min_dias, max_dias)


def calcular_costo_soporte(tipo_ticket):
    """
    Calcula el costo del ticket de soporte basado en su tipo.

    Cada tipo de ticket tiene un rango diferente de costos:
    - Consulta: 0-50 (atención básica)
    - Reclamo_Garantía: 50-200 (gestión de garantía)
    - Devolución: 100-500 (devolución y reposición)
    - Facturación: 0-100 (gestión documental)

    Args:
        tipo_ticket (str): Tipo de ticket de soporte

    Returns:
        float: Costo del ticket
    """
    min_costo, max_costo = COSTOS_POR_TIPO_SOPORTE[tipo_ticket]
    return round(random.uniform(min_costo, max_costo), 2)


def generar_fecha_venta(start_date):
    """
    Genera una fecha aleatoria para una venta.

    Args:
        start_date (datetime): Fecha de inicio del rango

    Returns:
        str: Fecha en formato "YYYY-MM-DD HH:MM:SS"
    """
    dias_offset = random.randint(0, 400)
    segundos_offset = random.randint(0, 86400)
    fecha = start_date + timedelta(days=dias_offset, seconds=segundos_offset)
    return fecha.strftime("%Y-%m-%d %H:%M:%S")


def generar_fecha_soporte(fecha_venta_str):
    """
    Genera una fecha futura para ticket de soporte.

    La fecha del ticket es 1-30 días después de la venta original,
    simulando que el cliente contacta soporte después de la transacción.

    Args:
        fecha_venta_str (str): Fecha de la venta en formato string

    Returns:
        str: Fecha futura en formato "YYYY-MM-DD HH:MM:SS"
    """
    # Convertir string a datetime
    fecha_venta = datetime.strptime(fecha_venta_str, "%Y-%m-%d %H:%M:%S")

    # Agregar días futuros (1-30 días después de la venta)
    dias_futuros = random.randint(
        RANGO_DIAS_FUTURO_SOPORTE[0], RANGO_DIAS_FUTURO_SOPORTE[1])
    fecha_futura = fecha_venta + timedelta(days=dias_futuros)

    return fecha_futura.strftime("%Y-%m-%d %H:%M:%S")


def main():
    """
    Función principal que genera el dataset completo.

    El proceso sigue estos pasos:
    1. Generar transacciones de venta (40,000 registros)
    2. Generar envíos logísticos vinculados a ventas (30,000 registros)
    3. Generar tickets de soporte con fechas futuras (20,000 registros)
    4. Generar registros de cliente únicos (10,000 registros)
    """

    # Set para rastrear usuarios ya registrados (evitar duplicados en REGISTRO_CLIENTE)
    usuarios_registrados = set()

    # Lista para almacenar todas las filas antes de escribir
    filas = []

    # Las ventas son la base de datos, los envíos y tickets se vinculan a ellas
    ventas = []  # Almacenar ventas para vincular con logística y soporte

    for i in range(40000):
        id_evento = f"EVT-{len(filas) + 1:07d}"
        fecha_hora = generar_fecha_venta(FECHA_INICIO)
        tipo_evento = "TRANSACCION_VENTA"
        id_usuario = f"USR-{random.randint(1000, 99999)}"
        id_producto = f"PRD-{random.randint(100, 999)}"
        categoria_producto = random.choice(CATEGORIAS_PRODUCTO)
        monto_transaccion = round(random.uniform(10.0, 1500.0), 2)
        ubicacion = random.choice(CIUDADES)
        estado_operativo = "N/A"  # Las ventas no tienen estado operativo
        canal_origen = random.choice(CANALES)

        # Guardar venta para vincular con logística y soporte
        ventas.append({
            "id_evento": id_evento,
            "fecha_hora": fecha_hora,
            "id_usuario": id_usuario,
            "ubicacion": ubicacion
        })

        # Los campos dias_entrega, tramo_regional y costo_soporte se agregan
        # pero con valores por defecto para las ventas (no aplican)
        tramo_regional = MAPEO_TRAMO_REGIONAL[ubicacion]
        
        filas.append([
            id_evento, fecha_hora, tipo_evento, id_usuario,
            id_producto, categoria_producto, monto_transaccion,
            ubicacion, estado_operativo, canal_origen,
            0, tramo_regional, 0.0  # dias_entrega=0, costo_soporte=0 para ventas
        ])

    # Los envíos se vinculan a ventas existentes
    # Cada envío tiene un estado que determina sus días de entrega

    for i in range(30000):
        id_evento = f"EVT-{len(filas) + 1:07d}"

        # Seleccionar una venta aleatoria para vincular el envío
        venta_asociada = random.choice(ventas)

        # La fecha del envío es la misma que la venta (simula mismo día de despacho)
        fecha_hora = venta_asociada["fecha_hora"]

        tipo_evento = "LOGISTICA_ENVIO"
        id_usuario = venta_asociada["id_usuario"]  # Mismo usuario que la venta
        id_producto = f"PRD-{random.randint(100, 999)}"
        categoria_producto = random.choice(CATEGORIAS_PRODUCTO)
        # Los envíos no tienen monto (ya está en la venta)
        monto_transaccion = 0.0
        ubicacion = venta_asociada["ubicacion"]  # Misma ubicación que la venta

        # Seleccionar estado con distribución ponderada
        estado_operativo = random.choices(
            list(PESOS_ESTADO_LOGISTICA.keys()),
            weights=list(PESOS_ESTADO_LOGISTICA.values()),
            k=1
        )[0]

        # Calcular días de entrega basado en el estado
        dias_entrega = calcular_dias_entrega(estado_operativo)

        canal_origen = random.choice(CANALES)
        tramo_regional = MAPEO_TRAMO_REGIONAL[ubicacion]

        filas.append([
            id_evento, fecha_hora, tipo_evento, id_usuario,
            id_producto, categoria_producto, monto_transaccion,
            ubicacion, estado_operativo, canal_origen,
            dias_entrega, tramo_regional, 0.0  # costo_soporte = 0 para logística
        ])

    # Los tickets se generan después de la venta (fechas futuras)
    # Cada tipo de ticket tiene un costo diferente

    for i in range(20000):
        id_evento = f"EVT-{len(filas) + 1:07d}"

        # Seleccionar una venta aleatoria para vincular el ticket
        venta_asociada = random.choice(ventas)

        # La fecha del ticket es futura respecto a la venta
        fecha_hora = generar_fecha_soporte(venta_asociada["fecha_hora"])

        tipo_evento = "SOPORTE_TICKET"
        id_usuario = venta_asociada["id_usuario"]  # Mismo usuario que la venta
        id_producto = ""  # Los tickets de soporte no tienen producto específico
        categoria_producto = ""  # Los tickets de soporte no tienen categoría
        monto_transaccion = 0.0  # Los tickets no tienen monto de venta

        ubicacion = venta_asociada["ubicacion"]

        # Seleccionar tipo de ticket de soporte
        tipo_ticket = random.choice(
            ["Consulta", "Reclamo_Garantia", "Devolucion", "Facturacion"])
        estado_operativo = tipo_ticket

        # Calcular costo del ticket
        costo_soporte = calcular_costo_soporte(tipo_ticket)

        canal_origen = random.choice(CANALES)
        tramo_regional = MAPEO_TRAMO_REGIONAL[ubicacion]

        filas.append([
            id_evento, fecha_hora, tipo_evento, id_usuario,
            id_producto, categoria_producto, monto_transaccion,
            ubicacion, estado_operativo, canal_origen,
            0, tramo_regional, costo_soporte  # dias_entrega = 0 para soporte
        ])

    # Cada usuario solo aparece una vez en REGISTRO_CLIENTE
    # Se usa un set para rastrear usuarios ya registrados

    registros_generados = 0
    intentos = 0
    max_intentos = 100000  # Evitar bucle infinito

    while registros_generados < 10000 and intentos < max_intentos:
        intentos += 1

        # Generar usuario único
        id_usuario = f"USR-{random.randint(1000, 99999)}"

        # Verificar que el usuario no esté ya registrado
        if id_usuario not in usuarios_registrados:
            usuarios_registrados.add(id_usuario)

            id_evento = f"EVT-{len(filas) + 1:07d}"
            fecha_hora = generar_fecha_venta(FECHA_INICIO)
            tipo_evento = "REGISTRO_CLIENTE"
            id_producto = ""  # Los registros de cliente no tienen producto
            categoria_producto = ""  # Los registros de cliente no tienen categoría
            monto_transaccion = 0.0  # Los registros no tienen monto

            ubicacion = random.choice(CIUDADES)
            estado_operativo = "N/A"  # Los registros no tienen estado
            canal_origen = random.choice(CANALES)
            tramo_regional = MAPEO_TRAMO_REGIONAL[ubicacion]

            filas.append([
                id_evento, fecha_hora, tipo_evento, id_usuario,
                id_producto, categoria_producto, monto_transaccion,
                ubicacion, estado_operativo, canal_origen,
                0, tramo_regional, 0.0  # sin días de entrega, sin costo soporte
            ])

            registros_generados += 1

    # Nombre del archivo de salida
    archivo_salida = "dataset_final.csv"

    with open(archivo_salida, mode="w", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)

        # Escribir header con todas las columnas incluyendo las nuevas
        writer.writerow([
            "id_evento", "fecha_hora", "tipo_evento", "id_usuario",
            "id_producto", "categoria_producto", "monto_transaccion",
            "ubicacion", "estado_operativo", "canal_origen",
            "dias_entrega", "tramo_regional", "costo_soporte"
        ])

        # Escribir todas las filas
        writer.writerows(filas)

    print(f"Dataset generado con éxito: {archivo_salida}")
    print(f"Total de registros: {len(filas)}")
    print(f"")
    print(f"Distribución por tipo de evento:")
    print(f"  - TRANSACCION_VENTA: 40,000 registros")
    print(f"  - LOGISTICA_ENVIO: 30,000 registros")
    print(f"  - SOPORTE_TICKET: 20,000 registros")
    print(f"  - REGISTRO_CLIENTE: 10,000 registros")
    print(f"")
    print(f"Nuevos campos agregados:")
    print(f"  - dias_entrega: 0-30 días (correlacionado con estado)")
    print(f"  - tramo_regional: Centro/Sur/Norte (derivado de ciudad)")
    print(f"  - costo_soporte: 0-500 (variable por tipo de ticket)")
    print(f"")
    print(f"Características:")
    print(f"  - Fechas futuras para tickets de soporte")
    print(f"  - Registros de cliente únicos por usuario")
    print(f"  - Distribución realista de estados logísticos")


if __name__ == "__main__":
    main()
