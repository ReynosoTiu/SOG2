import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from math import pi

def obtener_datos(motor):
    consulta = """
    SELECT hv.id_orden, hv.total_orden, hv.cantidad, hv.fecha_compra, dp.nombre_producto, dp.categoria_producto, dp.precio_producto,
           dc.id_cliente, dc.genero_cliente, dc.edad_cliente, mp.metodo_pago, re.region_envio
    FROM hechos_ventas hv
    JOIN dimension_cliente dc ON hv.id_cliente = dc.id_cliente
    JOIN dimension_producto dp ON hv.id_producto = dp.id_producto
    JOIN dimension_metodo_pago mp ON hv.id_metodo_pago = mp.id_metodo_pago
    JOIN dimension_region_envio re ON hv.id_region_envio = re.id_region_envio
    """
    datos = pd.read_sql(consulta, motor)
    return datos

def analisis_exploratorio(datos):
    if not datos.empty:
        datos_numericos = datos.select_dtypes(include=[np.number])
        print("Promedio de las columnas numéricas:\n", datos_numericos.mean())
        print("Mediana de las columnas numéricas:\n", datos_numericos.median())
        print("Valores más comunes en las columnas numéricas:\n", datos_numericos.mode().iloc[0])
        print("Correlación entre las variables numéricas:\n", datos_numericos.corr())

        if 'total_orden' in datos_numericos.columns:
            plt.figure(figsize=(10, 6))
            sns.barplot(x='categoria_producto', y='total_orden', hue='region_envio', data=datos)
            plt.xlabel('Categoría de Producto')
            plt.ylabel('Total de Ventas')
            plt.title('Distribución de Ventas por Categoría de Producto y Región')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.show()
    else:
        print("La tabla 'hechos_ventas' no contiene datos.")

def tendencias_ventas(datos):
    if not datos.empty:
        datos['mes'] = pd.to_datetime(datos['fecha_compra']).dt.month

        ventas_mensuales = datos.groupby('mes')['total_orden'].sum().reset_index()

        plt.figure(figsize=(10, 6))
        plt.plot(ventas_mensuales['mes'], ventas_mensuales['total_orden'], marker='o', color='blue')
        plt.xlabel('Mes')
        plt.ylabel('Total de ventas')
        plt.title('Evolución de las ventas por mes')
        plt.grid(True)
        plt.tight_layout()
        plt.show()

        ventas_por_producto = datos.groupby('nombre_producto')['total_orden'].sum().reset_index().sort_values(by='total_orden', ascending=False)

        plt.figure(figsize=(10, 6))
        plt.bar(ventas_por_producto['nombre_producto'], ventas_por_producto['total_orden'], color='orange')
        plt.xlabel('Producto')
        plt.ylabel('Total de Ventas')
        plt.title('Ventas Totales por Producto')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.show()
    else:
        print("No se encontraron datos suficientes para realizar el análisis de tendencias.")


def analizar_patrones_por_edad(datos):
    if not datos.empty:
        bins = [0, 20, 30, 40, 50, 60, 70, 80, 90]  # Definir los límites de los grupos de edad, rangos de 10 años
        labels = ['0-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90']
        datos['grupo_edad'] = pd.cut(datos['edad_cliente'], bins=bins, labels=labels, right=False)

        ventas_por_edad = datos.groupby('grupo_edad')['total_orden'].sum().reset_index()

        plt.figure(figsize=(10, 6))
        sns.barplot(x='grupo_edad', y='total_orden', data=ventas_por_edad, hue='grupo_edad', palette='Blues_d', legend=False)
        plt.xlabel('Grupo de Edad')
        plt.ylabel('Total de Ventas')
        plt.title('Total de Ventas por Grupo de Edad')
        plt.tight_layout()
        plt.show()
    else:
        print("No se encontraron datos suficientes para el análisis de edad.")


def comparar_comportamiento_por_genero(datos):
    if not datos.empty:
        ventas_por_genero = datos.groupby('genero_cliente')['total_orden'].sum().reset_index()

        plt.figure(figsize=(8, 6))
        sns.barplot(x='genero_cliente', y='total_orden', data=ventas_por_genero, palette='Set2')
        plt.xlabel('Género')
        plt.ylabel('Total de Ventas')
        plt.title('Comparación de Ventas por Género')
        plt.tight_layout()
        plt.show()
    else:
        print("No se encontraron datos suficientes para el análisis por género.")


def analizar_categoria_metodo_pago(datos):
    if not datos.empty:
        tabla_contingencia = pd.crosstab(datos['categoria_producto'], datos['metodo_pago'])

        print("Tabla de contingencia entre Categoría de Producto y Método de Pago:")
        print(tabla_contingencia)

        plt.figure(figsize=(10, 6))
        sns.heatmap(tabla_contingencia, annot=True, cmap="Blues", fmt="d")
        plt.title('Frecuencia de Métodos de Pago por Categoría de Producto')
        plt.ylabel('Categoría de Producto')
        plt.xlabel('Método de Pago')
        plt.tight_layout()
        plt.show()
    else:
        print("No se encontraron datos suficientes para analizar la relación.")

def grafico_cantidad_productos_por_mes(datos):
    datos['fecha_compra'] = pd.to_datetime(datos['fecha_compra'])
    datos['mes'] = datos['fecha_compra'].dt.to_period('M').astype(str)
    datos_agrupados = datos.groupby('mes').agg(total_productos=('cantidad', 'sum')).reset_index()
    if not datos_agrupados.empty:
        plt.figure(figsize=(10, 6))
        plt.fill_between(datos_agrupados['mes'], datos_agrupados['total_productos'], color="skyblue", alpha=0.4)
        plt.plot(datos_agrupados['mes'], datos_agrupados['total_productos'], color="Slateblue", alpha=0.6, linewidth=2)
        plt.title('Cantidad de Productos Comprados por Mes')
        plt.xlabel('Mes')
        plt.ylabel('Cantidad de Productos')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.show()
    else:
        print("No se encontraron datos suficientes para generar la gráfica.")

def grafico_metodo_pago_edad(datos):
    if not datos.empty:
        bins = [0, 20, 30, 40, 50, 60, 70, 80, 90]
        labels = ['0-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90']
        datos['grupo_edad'] = pd.cut(datos['edad_cliente'], bins=bins, labels=labels, right=False)

        metodo_pago_edad = datos.groupby(['grupo_edad', 'metodo_pago']).size().unstack().fillna(0)

        categorias = labels
        N = len(categorias)

        angles = [n / float(N) * 2 * pi for n in range(N)]
        angles += angles[:1]

        fig, ax = plt.subplots(figsize=(10, 8), subplot_kw=dict(polar=True))

        for metodo in metodo_pago_edad.columns:
            valores = metodo_pago_edad[metodo].tolist()
            valores += valores[:1]

            ax.plot(angles, valores, linewidth=2, linestyle='solid', label=metodo)
            ax.fill(angles, valores, alpha=0.25)

        plt.xticks(angles[:-1], categorias)
        plt.legend(loc='upper right', bbox_to_anchor=(1.1, 1.1))
        plt.title("Frecuencia de Métodos de Pago por Grupo de Edad")
        plt.show()
    else:
        print("No se encontraron datos suficientes para el análisis.")


def generar_graficas(datos):
    print("Generando gráficas...")
    analisis_exploratorio(datos)
    tendencias_ventas(datos)
    analizar_patrones_por_edad(datos)
    comparar_comportamiento_por_genero(datos)
    analizar_categoria_metodo_pago(datos)
    grafico_metodo_pago_edad(datos)
    grafico_cantidad_productos_por_mes(datos)