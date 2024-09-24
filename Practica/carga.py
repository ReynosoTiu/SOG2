import sys
from mysql.connector import Error

def insertar_datos_bd(conexion, datos, tamano_lote=100):
    try:
        total_filas = datos.shape[0]
        consulta = """
                INSERT INTO venta (id_orden, fecha_compra, id_cliente, genero_cliente, edad_cliente, categoria_producto, nombre_producto, precio_producto, cantidad, total_orden, metodo_pago, region_envio)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        with conexion.cursor() as cursor:
            for inicio in range(0, total_filas, tamano_lote):
                fin = min(inicio + tamano_lote, total_filas)
                lote = [tuple(fila) for _, fila in datos.iloc[inicio:fin].iterrows()]
                cursor.executemany(consulta, lote)
                
                percent_complete = (fin) / total_filas * 100
                sys.stdout.write(f'\rProgreso: {percent_complete:.2f}%')
                sys.stdout.flush()
        
        print()
        conexion.commit()
        print("Los datos han sido añadidos exitosamente a la base de datos.")
    except Error as error:
        print(f"Hubo un error al intentar insertar los datos: {error}")
        conexion.rollback()




def insertar_dimensiones_sql(conexion):
    try:
        cursor = conexion.cursor()

        # Insertar clientes únicos en la tabla dimension_cliente
        cursor.execute("""
            INSERT INTO dimension_cliente (id_cliente, genero_cliente, edad_cliente)
            SELECT DISTINCT id_cliente, genero_cliente, edad_cliente
            FROM venta
            ON DUPLICATE KEY UPDATE genero_cliente = VALUES(genero_cliente), edad_cliente = VALUES(edad_cliente)
        """)
        print("Clientes insertados en 'dimension_cliente'.")

        # Insertar productos únicos en la tabla dimension_producto
        cursor.execute("""
            INSERT INTO dimension_producto (nombre_producto, categoria_producto, precio_producto)
            SELECT DISTINCT nombre_producto, categoria_producto, precio_producto
            FROM venta
            ON DUPLICATE KEY UPDATE categoria_producto = VALUES(categoria_producto), precio_producto = VALUES(precio_producto)
        """)
        print("Productos insertados en 'dimension_producto'.")

        # Insertar métodos de pago únicos en la tabla dimension_metodo_pago
        cursor.execute("""
            INSERT INTO dimension_metodo_pago (metodo_pago)
            SELECT DISTINCT metodo_pago
            FROM venta
            ON DUPLICATE KEY UPDATE metodo_pago = VALUES(metodo_pago)
        """)
        print("Métodos de pago insertados en 'dimension_metodo_pago'.")

        # Insertar regiones de envío únicas en la tabla dimension_region_envio
        cursor.execute("""
            INSERT INTO dimension_region_envio (region_envio)
            SELECT DISTINCT region_envio
            FROM venta
            ON DUPLICATE KEY UPDATE region_envio = VALUES(region_envio)
        """)
        print("Regiones de envío insertadas en 'dimension_region_envio'.")

        conexion.commit()
    except Error as error:
        print(f"Hubo un error al insertar datos en las tablas de dimensiones: {error}")
        conexion.rollback()

def insertar_hechos_sql(conexion):
    try:
        cursor = conexion.cursor()

        # Insertar o actualizar en caso de duplicados
        cursor.execute("""
            INSERT INTO hechos_ventas (id_orden, id_cliente, id_producto, id_metodo_pago, id_region_envio, cantidad, total_orden, fecha_compra)
            SELECT v.id_orden, dc.id_cliente, dp.id_producto, mp.id_metodo_pago, re.id_region_envio, v.cantidad, v.total_orden, v.fecha_compra
            FROM venta v
            JOIN dimension_cliente dc ON v.id_cliente = dc.id_cliente
            JOIN dimension_producto dp ON v.nombre_producto = dp.nombre_producto and v.categoria_producto = dp.categoria_producto and v.precio_producto = dp.precio_producto
            JOIN dimension_metodo_pago mp ON v.metodo_pago = mp.metodo_pago
            JOIN dimension_region_envio re ON v.region_envio = re.region_envio
        """)
        print("Datos insertados o actualizados en 'hechos_ventas'.")
        
        conexion.commit()
    except Error as error:
        print(f"Hubo un error al insertar datos en la tabla de hechos: {error}")
        conexion.rollback()

