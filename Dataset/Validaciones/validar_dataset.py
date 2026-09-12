"""
VALIDADOR DE DATASET - RetailTech Chile

Este script valida el dataset actual (dataset_operaciones_empresa.csv) antes de modificar el seed y generar el dataset_final.csv

PROPÓSITO:

Conocer el estado actual del dataset para identificar problemas que puedan afectar la compatibilidad con Cloudera CDH 5.13.0

QUÉ VERIFICAMOS:

1. Encoding del archivo, un encoding incorrecto puede causar problemas al cargar en Hive, Pig o MapReduce. Encoding es UTF-8 sin BOM (Byte Order Mark), BOM es un caracter invisible al inicio del archivo que puede causar errores en Cloudera.

2. Delimitador correcto (comas), todas las filas deben tener el mismo número de columnas

3. Campos vacíos (valores nulos), pueden ser esperados en algunos tipos de eventos, pero no en todos. Se reporta la cantidad de campos vacíos por columna y por tipo de evento.

4. Tipos de datos (fechas, montos, IDs), los tipos de datos deben ser consistentes con lo esperado para cada columna. Se reportan errores de formato y valores inválidos. Con consistentes nos referimos a que los tipos de datos sean correctos y que los valores tengan sentido dentro del contexto del negocio.

5. Consistencia lógica (reglas de negocio), nuestras reglas de negocio definen cómo deben relacionarse los datos entre sí. Por ejemplo, un evento de tipo TRANSACCION_VENTA debe tener un monto mayor a 0, mientras que un evento de tipo LOGISTICA_ENVIO no debería tener un monto mayor a 0. Se reportan inconsistencias encontradas.

6. Tamaño total de registros, el dataset debe tener al menos 50,000 registros para ser considerado válido. Se reporta el número total de registros y si cumple con el mínimo requerido.

7. IDs duplicados, cada evento debe tener un ID único. Se reporta la cantidad de IDs duplicados y ejemplos de los mismos. Imagina que cada evento es como un ticket de soporte, si dos tickets tienen el mismo número de ticket, se genera confusión y problemas al procesar los datos.

8. Estructura de columnas, el dataset debe tener las columnas correctas en el orden esperado. Se reportan diferencias entre las columnas encontradas y las esperadas.

PARA QUÉ SIRVE ESTA INFORMACIÓN:

- Conocer qué problemas tiene el dataset actual
- Decidir qué cambios hacer en el seed
- Comparar con el dataset_final después de las modificaciones
- Asegurar compatibilidad con Hive, Pig y MapReduce

"""
import re
import csv
from datetime import datetime
from collections import Counter

# Ruta al dataset actual, para validar antes de modificar el seed. Si lo van a hacer en otra máquina, cambiar la ruta al dataset.
DATASET_PATH = "C:\\Users\\ignac\\OneDrive\\Escritorio\\Big Data\\PruebaPractica\\Dataset\\dataset_operaciones_empresa.csv"

# Número esperado de columnas en el dataset
NUM_COLUMNAS_ESPERADAS = 10

# Columnas esperadas
COLUMNAS_ESPERADAS = [
    "id_evento", "fecha_hora", "tipo_evento", "id_usuario",
    "id_producto", "categoria_producto", "monto_transaccion",
    "ubicacion", "estado_operativo", "canal_origen"
]

# Tipos de evento válidos de acuerdo a las reglas de negocio, si se agregan nuevos tipos de evento, deben agregarse aquí para que el validador los reconozca como válidos.
TIPOS_EVENTO_VALIDOS = ["TRANSACCION_VENTA",
                        "LOGISTICA_ENVIO", "SOPORTE_TICKET", "REGISTRO_CLIENTE"]

# Estados operativos válidos por tipo de evento, un estado operativo es un valor que indica el estado actual de un evento, por ejemplo, un evento de tipo LOGISTICA_ENVIO puede estar en estado "En_Bodega", "En_Ruta", "Entregado" o "Retrasado". Si se agregan nuevos tipos de evento o estados, deben agregarse aquí para que el validador los reconozca como válidos.
ESTADOS_POR_TIPO = {
    "LOGISTICA_ENVIO": ["En_Bodega", "En_Ruta", "Entregado", "Retrasado"],
    "SOPORTE_TICKET": ["Consulta", "Reclamo_Garantia", "Devolucion", "Facturacion"],
    "TRANSACCION_VENTA": ["N/A"],
    "REGISTRO_CLIENTE": ["N/A"]
}

# Categorías de producto válidas, si se agregan nuevas categorías, deben agregarse aquí para que el validador las reconozca como válidas. Una categoría de producto es un valor que indica la categoría a la que pertenece un producto, por ejemplo, un producto puede ser de la categoría "Electronica", "Hogar", "Moda", "Deportes" o "Alimentacion". Si se agregan nuevas categorías, deben agregarse aquí para que el validador las reconozca como válidas.
CATEGORIAS_VALIDAS = ["Electronica", "Hogar",
                      "Moda", "Deportes", "Alimentacion"]

# Ciudades válidas, si se agregan nuevas ciudades, deben agregarse aquí para que el validador las reconozca como válidas. Una ciudad es un valor que indica la ciudad en la que se realizó un evento, por ejemplo, un evento puede haberse realizado en "Santiago", "Valparaiso", "Concepcion", "Antofagasta" o "La Serena". Si hay ciudades fuera de este listado, el validador las marcará como inválidas.
CIUDADES_VALIDAS = ["Santiago", "Valparaiso",
                    "Concepcion", "Antofagasta", "La Serena"]

# Canales válidos, un canal es un valor que indica el canal a través del cual se realizó un evento, por ejemplo, un evento puede haberse realizado a través de "Mobile App", "Portal Web B2C" o "Sucursal Fisica".
CANALES_VALIDOS = ["Mobile App", "Portal Web B2C", "Sucursal Fisica"]


def validar_encoding(ruta_archivo):
    """
    Verificar que el archivo está en UTF-8 sin BOM

    Solo nos retorna si es (aprobado, detalles)
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8-sig') as f:
            contenido = f.read()

        # Verificar si tiene BOM (Byte Order Mark)
        tiene_bom = contenido.startswith('\ufeff')

        if tiene_bom:
            return False, "Archivo tiene BOM (Byte Order Mark). Cloudera puede tener problemas."

        return True, "Encoding UTF-8 válido sin BOM"

    except UnicodeDecodeError:
        return False, "Archivo no es UTF-8 válido. Encoding detectado incorrecto."
    except Exception as e:
        return False, f"Error al leer archivo: {str(e)}"


def validar_delimitador(ruta_archivo):
    """
    Este test lo que hace es verificar que el delimitador es coma y todas las filas tienen el mismo número de columnas

    Nos reporta lo mismo, (aprobado, detalles)
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            lineas = f.readlines()

        if len(lineas) == 0:
            return False, "Archivo vacío"

        # Verificar primera línea (header), esperamos que tenga 10 columnas como dice más arriba
        header = lineas[0].strip().split(',')
        num_columnas_header = len(header)

        # Verificar filas con número incorrecto de columnas, si hay filas con número incorrecto de columnas, las reportamos como problemáticas, pero no las contamos como errores, solo las reportamos para que el usuario pueda revisarlas.
        filas_problematicas = []
        for i, linea in enumerate(lineas[1:], start=2):
            columnas = linea.strip().split(',')
            if len(columnas) != num_columnas_header:
                filas_problematicas.append((i, len(columnas)))

        if filas_problematicas:
            ejemplos = filas_problematicas[:5]
            return False, f"Filas con número incorrecto de columnas: {ejemplos}"

        return True, f"Delimitador correcto. Todas las filas tienen {num_columnas_header} columnas"

    except Exception as e:
        return False, f"Error al validar delimitador: {str(e)}"


def validar_campos_vacios(ruta_archivo):
    """
    Este testeo se encarga de contar campos vacíos (valores entre comas consecutivas) 
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            lineas = f.readlines()

        total_campos_vacios = 0
        campos_vacios_por_columna = Counter()
        eventos_con_vacios = Counter()

        for linea in lineas[1:]:  # Saltar header con el fin de no contar la primera línea
            # Contar comas consecutivas (campos vacíos), cada campo vacío es un valor entre comas consecutivas, por ejemplo: "valor1,,valor3" tiene un campo vacío entre "valor1" y "valor3".
            partes = linea.strip().split(',')
            for i, parte in enumerate(partes):
                if parte == '':
                    total_campos_vacios += 1
                    campos_vacios_por_columna[COLUMNAS_ESPERADAS[i]] += 1

            # Identificar tipo de evento con campos vacíos, si el tipo de evento es SOPORTE_TICKET o REGISTRO_CLIENTE, los campos vacíos son esperados, pero si el tipo de evento es TRANSACCION_VENTA o LOGISTICA_ENVIO, los campos vacíos no son esperados.
            if len(partes) >= 4:
                tipo_evento = partes[2]
                if '' in [partes[4], partes[5]]:  # id_producto y categoria_producto
                    eventos_con_vacios[tipo_evento] += 1

        total_registros = len(lineas) - 1
        porcentaje = (total_campos_vacios /
                      (total_registros * NUM_COLUMNAS_ESPERADAS)) * 100

        detalles = [
            f"Total de campos vacíos: {total_campos_vacios}",
            f"Porcentaje: {porcentaje:.2f}%",
            f"Por columna: {dict(campos_vacios_por_columna)}",
            f"Por tipo de evento: {dict(eventos_con_vacios)}"
        ]

        # Los campos vacíos son esperados en SOPORTE_TICKET y REGISTRO_CLIENTE
        if eventos_con_vacios.get("SOPORTE_TICKET", 0) > 0 or eventos_con_vacios.get("REGISTRO_CLIENTE", 0) > 0:
            detalles.append(
                "NOTA: Campos vacíos son normales para SOPORTE_TICKET y REGISTRO_CLIENTE (no tienen producto)")

        return True, "\n    ".join(detalles)

    except Exception as e:
        return False, f"Error al validar campos vacíos: {str(e)}"


def validar_tipos_datos(ruta_archivo):
    """
    Este test lo que hace es verificar que los tipos de datos son correctos, un dato puede ser de tipo fecha, número, string, etc. y cada columna tiene un tipo de dato esperado. Por ejemplo, la columna "fecha_hora" debe ser de tipo fecha, la columna "monto_transaccion" debe ser de tipo número, la columna "id_evento" debe ser de tipo string con un formato específico, etc.
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            lineas = f.readlines()

        errores_fecha = 0
        errores_monto = 0
        errores_id = 0
        errores_tipo_evento = 0
        errores_ciudad = 0
        errores_canal = 0
        errores_categoria = 0

        for linea in lineas[1:]:  # Saltar header
            partes = linea.strip().split(',')

            if len(partes) < NUM_COLUMNAS_ESPERADAS:
                continue

            id_evento, fecha_hora, tipo_evento, id_usuario, id_producto, \
                categoria_producto, monto_transaccion, ubicacion, \
                estado_operativo, canal_origen = partes

            # Validar formato de fecha y hora, funciona para el formato "YYYY-MM-DD HH:MM:SS", si el formato es incorrecto, se cuenta como error.
            try:
                datetime.strptime(fecha_hora, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                errores_fecha += 1

            # Validar monto como número, si el monto no es un número válido, se cuenta como error.
            try:
                float(monto_transaccion)
            except ValueError:
                errores_monto += 1

            # Validar formato de ID de evento, el ID de evento debe tener el formato "EVT-XXXXXXX" donde XXXXXXX es un número de 7 dígitos. Si el formato es incorrecto, se cuenta como error.
            if not re.match(r'^EVT-\d{7}$', id_evento):
                errores_id += 1

            # Validar tipo de evento, usamos la lista de TIPOS_EVENTO_VALIDOS para verificar si el tipo de evento es válido.
            if tipo_evento not in TIPOS_EVENTO_VALIDOS:
                errores_tipo_evento += 1

            # Validar ciudad
            if ubicacion not in CIUDADES_VALIDAS:
                errores_ciudad += 1

            # Validar canal
            if canal_origen not in CANALES_VALIDOS:
                errores_canal += 1

            # Validar categoría (puede estar vacía para eventos sin producto), dado que la categoría de producto puede estar vacía para eventos que no tienen producto, como SOPORTE_TICKET o REGISTRO_CLIENTE, solo validamos la categoría si el tipo de evento es TRANSACCION_VENTA o LOGISTICA_ENVIO.
            if categoria_producto and categoria_producto not in CATEGORIAS_VALIDAS:
                errores_categoria += 1

        total_registros = len(lineas) - 1
        detalles = [
            f"Registros analizados: {total_registros}",
            f"Errores de fecha: {errores_fecha}",
            f"Errores de monto: {errores_monto}",
            f"Errores de ID evento: {errores_id}",
            f"Errores de tipo evento: {errores_tipo_evento}",
            f"Errores de ciudad: {errores_ciudad}",
            f"Errores de canal: {errores_canal}",
            f"Errores de categoría: {errores_categoria}"
        ]

        total_errores = errores_fecha + errores_monto + errores_id + \
            errores_tipo_evento + errores_ciudad + errores_canal + errores_categoria

        if total_errores == 0:
            return True, "Todos los tipos de datos son correctos"
        else:
            return False, f"Total de errores: {total_errores}\n    " + "\n    ".join(detalles)

    except Exception as e:
        return False, f"Error al validar tipos de datos: {str(e)}"


def validar_consistencia_logica(ruta_archivo):
    """
    Verificar reglas de negocio 
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            lineas = f.readlines()

        errores_monto_venta = 0
        errores_monto_no_venta = 0
        errores_estado_tipo = 0
        errores_producto_venta = 0

        for linea in lineas[1:]:
            partes = linea.strip().split(',')

            if len(partes) < NUM_COLUMNAS_ESPERADAS:
                continue

            tipo_evento = partes[2]
            id_producto = partes[4]
            categoria = partes[5]
            monto = float(partes[6]) if partes[6] else 0.0
            estado = partes[8]

            # TRANSACCION_VENTA debe tener monto > 0, esta regla nace de la lógica de negocio, ya que una venta no puede tener un monto menor o igual a cero. Si se encuentra un evento de tipo TRANSACCION_VENTA con monto <= 0, se cuenta como error.
            if tipo_evento == "TRANSACCION_VENTA" and monto <= 0:
                errores_monto_venta += 1

            # Eventos que no son venta deben tener monto = 0, es así ya que un evento que no es una venta no debería tener un monto asociado. Si se encuentra un evento que no es de tipo TRANSACCION_VENTA con monto > 0, se cuenta como error.
            if tipo_evento != "TRANSACCION_VENTA" and monto > 0:
                errores_monto_no_venta += 1

            # TRANSACCION_VENTA y LOGISTICA_ENVIO deben tener producto, porque si no tienen producto, no tiene sentido que sean eventos de venta o envío.
            # Por ejemplo, un evento de tipo LOGISTICA_ENVIO sin producto no tiene sentido, ya que no hay nada que enviar. Si se encuentra un evento de tipo TRANSACCION_VENTA o LOGISTICA_ENVIO sin id_producto o sin categoria, se cuenta como error.
            if tipo_evento in ["TRANSACCION_VENTA", "LOGISTICA_ENVIO"]:
                if not id_producto or not categoria:
                    errores_producto_venta += 1

            # Estado debe ser válido para el tipo de evento
            if tipo_evento in ESTADOS_POR_TIPO:
                if estado not in ESTADOS_POR_TIPO[tipo_evento]:
                    errores_estado_tipo += 1

        total_errores = errores_monto_venta + errores_monto_no_venta + \
            errores_estado_tipo + errores_producto_venta

        detalles = [
            f"Ventas con monto <= 0: {errores_monto_venta}",
            f"Eventos no-venta con monto > 0: {errores_monto_no_venta}",
            f"Estados inválidos por tipo: {errores_estado_tipo}",
            f"Ventas/envíos sin producto: {errores_producto_venta}"
        ]

        if total_errores == 0:
            return True, "Consistencia lógica correcta"
        else:
            return False, f"Inconsistencias encontradas: {total_errores}\n    " + "\n    ".join(detalles)

    except Exception as e:
        return False, f"Error al validar consistencia: {str(e)}"


def validar_tamanio(ruta_archivo):
    """
    Verificar el número total de registros, para validar que el dataset tenga al menos 50,000 registros o más.
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            lineas = f.readlines()

        total_lineas = len(lineas)
        total_registros = total_lineas - 1  # Sin contar header

        tiene_header = lineas[0].strip().startswith('id_evento')

        detalles = [
            f"Total de líneas: {total_lineas}",
            f"Total de registros (sin header): {total_registros}",
            f"Tiene header: {'Sí' if tiene_header else 'No'}",
            f"Mínimo requerido: 50,000"
        ]

        if total_registros >= 50000:
            return True, f"50,000 registros mínimo cumplido\n    " + "\n    ".join(detalles)
        else:
            return False, f"Solo {total_registros} registros. Mínimo: 50,000\n    " + "\n    ".join(detalles)

    except Exception as e:
        return False, f"Error al contar registros: {str(e)}"


def validar_duplicados(ruta_archivo):
    """
    Verificar que no hay IDs de evento duplicados, evitar problemas de integridad de datos y confusión al procesar los eventos.  
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            lineas = f.readlines()

        ids_evento = []
        for linea in lineas[1:]:
            partes = linea.strip().split(',')
            if partes:
                ids_evento.append(partes[0])

        total_registros = len(ids_evento)
        ids_unicos = len(set(ids_evento))
        duplicados = total_registros - ids_unicos

        # Encontrar ejemplos de duplicados, mediante un contador de IDs de evento, si un ID aparece más de una vez, se considera duplicado. Se reportan hasta 5 ejemplos de IDs duplicados. Este tipo de validación se puede resolver de varias formas, pero la más simple es usar un contador de Python para contar cuántas veces aparece cada ID de evento con la función Counter de la librería collections. Luego, se filtran los IDs que tienen un conteo mayor a 1 y se toman los primeros 5 como ejemplos de duplicados.
        contador = Counter(ids_evento)
        ejemplos = [id for id, count in contador.items() if count > 1][:5]

        detalles = [
            f"Registros totales: {total_registros}",
            f"IDs únicos: {ids_unicos}",
            f"Duplicados: {duplicados}",
            f"Ejemplos de duplicados: {ejemplos}"
        ]

        if duplicados == 0:
            return True, "No hay IDs de evento duplicados"
        else:
            return False, f"Se encontraron {duplicados} IDs duplicados\n    " + "\n    ".join(detalles)

    except Exception as e:
        return False, f"Error al validar duplicados: {str(e)}"


def validar_estructura_columnas(ruta_archivo):
    """
    Verificar que la estructura de columnas es correcta, usamos la lista COLUMNAS_ESPERADAS para comparar con las columnas encontradas en el archivo. Este procedimiento se puede acortar mediante la lectura de la primera línea del archivo, que contiene el header con los nombres de las columnas. Luego, se comparan las columnas encontradas con las esperadas y se reportan diferencias.
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            header = f.readline().strip()

        columnas = header.split(',')

        detalles = [
            f"Columnas encontradas: {len(columnas)}",
            f"Columnas esperadas: {NUM_COLUMNAS_ESPERADAS}",
            f"Columnas: {columnas}",
            f"Esperadas: {COLUMNAS_ESPERADAS}"
        ]

        if columnas == COLUMNAS_ESPERADAS:
            return True, "Estructura de columnas correcta"
        else:
            # Encontrar diferencias, lo hacemos para evitar problemas de compatibilidad con Cloudera, ya que si las columnas no están en el orden esperado o faltan columnas, puede causar errores al procesar los datos, y peor, empezar de cero de nuevo.
            faltantes = [c for c in COLUMNAS_ESPERADAS if c not in columnas]
            extra = [c for c in columnas if c not in COLUMNAS_ESPERADAS]
            if faltantes:
                detalles.append(f"Columnas faltantes: {faltantes}")
            if extra:
                detalles.append(f"Columnas extra: {extra}")
            return False, "Estructura de columnas incorrecta\n    " + "\n    ".join(detalles)

    except Exception as e:
        return False, f"Error al validar estructura: {str(e)}"


def ejecutar_validaciones():
    """
    Ejecuta todas las validaciones y genera un reporte de resultados. El reporte se guarda en un archivo de texto en la ruta especificada.
    """
    print("=" * 60)
    print("VALIDACIÓN DE DATASET - RetailTech Chile")
    print("=" * 60)
    print(f"Dataset: {DATASET_PATH}")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Definir tests, cada test es una tupla con el nombre del test y la función que lo ejecuta. Es modificable para agregar nuevos tests o cambiar el orden de ejecución.
    tests = [
        ("1. Encoding UTF-8", validar_encoding),
        ("2. Delimitador y columnas", validar_delimitador),
        ("3. Campos vacíos", validar_campos_vacios),
        ("4. Tipos de datos", validar_tipos_datos),
        ("5. Consistencia lógica", validar_consistencia_logica),
        ("6. Tamaño de registros", validar_tamanio),
        ("7. IDs duplicados", validar_duplicados),
        ("8. Estructura de columnas", validar_estructura_columnas),
    ]

    resultados = []
    total_aprobados = 0

    for nombre, funcion in tests:
        print(f"\n--- {nombre} ---")
        try:
            aprobado, detalles = funcion(DATASET_PATH)
            resultados.append((nombre, aprobado, detalles))

            if aprobado:
                print(f"  [APROBADO] {detalles}")
                total_aprobados += 1
            else:
                print(f"  [FALLIDO] {detalles}")

        except Exception as e:
            print(f"  [ERROR] Excepción durante la validación: {str(e)}")
            resultados.append((nombre, False, f"Excepción: {str(e)}"))

    # Resumen
    print("\n" + "=" * 60)
    print("RESUMEN DE VALIDACIÓN")
    print("=" * 60)
    print(f"Tests aprobados: {total_aprobados}/{len(tests)}")

    if total_aprobados == len(tests):
        print("\n[OK] Dataset validado correctamente. Compatible con Cloudera.")
    else:
        print("\n[!] Dataset tiene problemas que requieren atención.")
        print("    Revisar los tests fallidos antes de modificar el seed.")

    # Guardar reporte
    print("\nGenerando reporte de validación...")
    # la ruta_reporte es la ruta donde se guardará el reporte de validación, si lo van a hacer en otra máquina, cambiar la ruta al reporte, para evitar problemas.
    ruta_reporte = "C:\\Users\\ignac\\OneDrive\\Escritorio\\Big Data\\PruebaPractica\\Dataset\\Validaciones\\reporte_validacion.txt"
    with open(ruta_reporte, 'w', encoding='utf-8') as f:
        f.write("REPORTE DE VALIDACIÓN - DATASET ACTUAL\n")
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Dataset: dataset_operaciones_empresa.csv\n")
        f.write("=" * 60 + "\n\n")

        for nombre, aprobado, detalles in resultados:
            estado = "APROBADO" if aprobado else "FALLIDO"
            f.write(f"{nombre}: [{estado}]\n")
            f.write(f"  {detalles}\n\n")

        f.write("=" * 60 + "\n")
        f.write(f"RESUMEN: {total_aprobados}/{len(tests)} tests aprobados\n\n")

        f.write("ACCIONES RECOMENDADAS:\n")
        if total_aprobados < len(tests):
            f.write("- Revisar tests fallidos\n")
            f.write("- Corregir problemas antes de modificar el seed\n")
            f.write("- Considerar encoding, delimitador y campos vacíos\n")
        else:
            f.write("- Dataset actual es compatible con Cloudera\n")
            f.write("- Proceder a modificar el seed para agregar nuevos campos\n")
            f.write("- Generar dataset_final.csv con los cambios\n")

    print(f"\nReporte guardado en: {ruta_reporte}")

    return resultados


if __name__ == "__main__":
    ejecutar_validaciones()
