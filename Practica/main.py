import os
import pandas as pd
from modelo import *
from carga import *
from graficas import *
from sqlalchemy import create_engine
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

load_dotenv()

tabla = os.getenv('MYSQL_TABLE')

def obtener_motor_sql():
    try:
        motor = create_engine(f"mysql+mysqlconnector://{os.getenv('MYSQLUSER')}:{os.getenv('MYSQLPASSWORD')}@{os.getenv('MYSQLHOST')}:{os.getenv('MYSQLPORT')}/{os.getenv('MYSQLDATABASE')}")
        return motor
    except Exception as error:
        print(f"Hubo un problema en obtener_motor_sql(): {error}")
        return None

def establecer_conexion_mysql():
    try:
        conexion = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            port=os.getenv('MYSQLPORT')
        )
        if conexion.is_connected():
            print("Conexión establecida exitosamente con la base de datos MySQL")
        return conexion
    except Error as error:
        print(f"No se pudo establecer la conexión a la base de datos: {error}")
        return None

def cargar_datos_csv(archivo_csv):
    try:
        datos = pd.read_csv(archivo_csv, parse_dates=['purchase_date'], dayfirst=True)
        datos.drop_duplicates(inplace=True)
        if datos.isnull().values.any():
            datos.dropna(inplace=True)
        datos = validar_datos(datos)
        return datos
    except FileNotFoundError as error:
        print(f"No se encontró el archivo: {error}")
    except Exception as error:
        print(f"Error al cargar el archivo CSV: {error}")

def validar_datos(datos):
    longitudes_maximas = {
        'metodo_pago': 200,
        'categoria_producto': 20,
        'region_envio': 20
    }

    for columna, longitud_maxima in longitudes_maximas.items():
        if columna in datos.columns:
            if datos[columna].astype(str).str.len().max() > longitud_maxima:
                print(f"Nota: Hay valores en '{columna}' que superan el límite de {longitud_maxima} caracteres.")
                datos[columna] = datos[columna].astype(str).apply(lambda valor: valor[:longitud_maxima])

    return datos

def main():
    motor = obtener_motor_sql()
    if motor:
        conexion = establecer_conexion_mysql()
        if conexion:
            archivo_csv = os.getenv('CSVFILE')
            datos = cargar_datos_csv(archivo_csv)
            if datos is not None:
                inicializar_bd_y_tabla(conexion)
                insertar_datos_bd(conexion, datos)
                insertar_dimensiones_sql(conexion)
                insertar_hechos_sql(conexion)
                datos2 = obtener_datos(motor)
                generar_graficas(datos2)
            conexion.close()

if __name__ == "__main__":
    main()



