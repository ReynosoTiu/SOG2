from mysql.connector import Error
import os

def eliminar_tablas(conexion):
    try:
        cursor = conexion.cursor()
        # Eliminar tabla de hechos
        cursor.execute("DROP TABLE IF EXISTS hechos_ventas")
        print("Tabla 'hechos_ventas' eliminada.")

        # Eliminar tablas de dimensiones
        cursor.execute("DROP TABLE IF EXISTS dimension_cliente")
        cursor.execute("DROP TABLE IF EXISTS dimension_producto")
        cursor.execute("DROP TABLE IF EXISTS dimension_metodo_pago")
        cursor.execute("DROP TABLE IF EXISTS dimension_region_envio")
        print("Tablas de dimensiones eliminadas.")

        # Eliminar tabla de ventas
        cursor.execute("DROP TABLE IF EXISTS venta")
        print("Tabla 'venta' eliminada.")
        
        conexion.commit()
    except Error as error:
        print(f"Error al eliminar las tablas: {error}")
        conexion.rollback()


def inicializar_bd_y_tabla(conexion):
    try:
        cursor = conexion.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('MYSQLDATABASE')}")
        cursor.execute(f"USE {os.getenv('MYSQLDATABASE')}")
        eliminar_tablas(conexion)
        # Creación de la tabla de ventas (esta tabla queda como está en tu requerimiento)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS venta (
                id_orden INT PRIMARY KEY,
                fecha_compra DATE,
                id_cliente INT,
                genero_cliente VARCHAR(10),
                edad_cliente INT,
                categoria_producto VARCHAR(20),
                nombre_producto VARCHAR(50),
                precio_producto DECIMAL(10, 2),
                cantidad INT,
                total_orden DECIMAL(10, 2),
                metodo_pago VARCHAR(200),
                region_envio VARCHAR(20)
            )
        """)

        # Crear tabla de dimensiones para productos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_producto (
                id_producto INT PRIMARY KEY AUTO_INCREMENT,
                nombre_producto VARCHAR(50),
                categoria_producto VARCHAR(20),
                precio_producto DECIMAL(10, 2)
            )
        """)

        # Crear tabla de dimensiones para clientes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_cliente (
                id_cliente INT PRIMARY KEY,
                genero_cliente VARCHAR(10),
                edad_cliente INT
            )
        """)

        # Crear tabla de dimensiones para método de pago
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_metodo_pago (
                id_metodo_pago INT PRIMARY KEY AUTO_INCREMENT,
                metodo_pago VARCHAR(100)
            )
        """)

        # Crear tabla de dimensiones para región de envío
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimension_region_envio (
                id_region_envio INT PRIMARY KEY AUTO_INCREMENT,
                region_envio VARCHAR(50)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hechos_ventas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_orden INT,
                id_cliente INT,
                id_producto INT,
                id_metodo_pago INT,
                id_region_envio INT,
                cantidad INT,
                total_orden DECIMAL(10, 2),
                fecha_compra DATE,
                FOREIGN KEY (id_cliente) REFERENCES dimension_cliente(id_cliente),
                FOREIGN KEY (id_producto) REFERENCES dimension_producto(id_producto),
                FOREIGN KEY (id_metodo_pago) REFERENCES dimension_metodo_pago(id_metodo_pago),
                FOREIGN KEY (id_region_envio) REFERENCES dimension_region_envio(id_region_envio)
            )
        """)
        print("Base de datos y tablas creadas correctamente.")
    except Error as error:
        print(f"Error durante la creación de la base de datos o las tablas: {error}")