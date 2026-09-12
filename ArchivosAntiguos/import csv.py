import csv
import random
from datetime import datetime, timedelta

num_registros = 100000
start_date = datetime(2025, 1, 1)

# Nuevos tipos de eventos coherentes con operaciones y logística
tipos_evento = ["TRANSACCION_VENTA", "LOGISTICA_ENVIO", "SOPORTE_TICKET", "REGISTRO_CLIENTE"]
categorias_producto = ["Electronica", "Hogar", "Moda", "Deportes", "Alimentacion"]
ciudades = ["Santiago", "Valparaiso", "Concepcion", "Antofagasta", "La Serena"]

with open("dataset_operaciones_empresa.csv", mode="w", newline="", encoding="utf-8") as archivo:
    writer = csv.writer(archivo)
    writer.writerow([
        "id_evento", "fecha_hora", "tipo_evento", "id_usuario", 
        "id_producto", "categoria_producto", "monto_transaccion", 
        "ubicacion", "estado_operativo", "canal_origen"
    ])
    
    for i in range(1, num_registros + 1):
        id_evento = f"EVT-{i:07d}"
        dias_offset = random.randint(0, 400)
        segundos_offset = random.randint(0, 86400)
        fecha_hora = (start_date + timedelta(days=dias_offset, seconds=segundos_offset)).strftime("%Y-%m-%d %H:%M:%S")
        
        tipo_evento = random.choices(tipos_evento, weights=[40, 30, 20, 10], k=1)[0]
        id_usuario = f"USR-{random.randint(1000, 99999)}"
        id_producto = f"PRD-{random.randint(100, 999)}" if tipo_evento in ["TRANSACCION_VENTA", "LOGISTICA_ENVIO"] else ""
        cat_prod = random.choice(categorias_producto) if id_producto else ""
        monto = round(random.uniform(10.0, 1500.0), 2) if tipo_evento == "TRANSACCION_VENTA" else 0.0
        ubicacion = random.choice(ciudades)
        
        # Campo dinámico coherente según el tipo de evento
        if tipo_evento == "LOGISTICA_ENVIO":
            detalle_operativo = random.choice(["En_Bodega", "En_Ruta", "Entregado", "Retrasado"])
        elif tipo_evento == "SOPORTE_TICKET":
            detalle_operativo = random.choice(["Consulta", "Reclamo_Garantia", "Devolucion", "Facturacion"])
        else:
            detalle_operativo = "N/A"
            
        canal = random.choice(["Mobile App", "Portal Web B2C", "Sucursal Fisica"])
        
        writer.writerow([
            id_evento, fecha_hora, tipo_evento, id_usuario, 
            id_producto, cat_prod, monto, ubicacion, detalle_operativo, canal
        ])

print(f"Dataset actualizado y generado con éxito ({num_registros} registros).")