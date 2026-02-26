import pandas as pd
import sqlite3

def normalizacion(df): 
    df['nombre'] = df['nombre'].str.strip()
    df['nombre'] = df['nombre'].str.upper()
    duplicados = df.duplicated(subset=['nombre']).sum()
    df = df.groupby('nombre').agg({'precio': 'mean', 'stock': 'sum', 'categoria': lambda x: x.mode()[0]}).reset_index()
    print(f'Se eliminaron {duplicados} duplicados del inventario, se promediaron los precios y se sumó el stock')
    return df

def eliminar_precio_negativo(df):
    #Filtramos los productos con precio negativo
    precio_negativo = df[df['precio'] <= 0] 
    if not precio_negativo.empty:
        df = df[df['precio'] > 0]
        for producto in precio_negativo['nombre']:
            print(f"El producto {producto} ha sido eliminado por precio negativo")
    return df

def eliminar_stock_negativo(df): 
    #Filtramos los productos con stock negativo
    stock_negativo = df[df['stock'] < 0]
    if not stock_negativo.empty:
        df = df[df['stock'] >= 0]
        for producto in stock_negativo['nombre']:
            print(f"El producto {producto} ha sido eliminado por stock negativo")
    return df

def exportar_json(df, nombre_archivo='inventario_validado.json'):
    df.to_json(f'./data/{nombre_archivo}', orient='records', indent=4)

def cargar_a_sql(df, nombre_db):
    conexion = sqlite3.connect(nombre_db)
    cursor = conexion.cursor()

    #Creamos la tabla manualmente
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventario(
            nombre TEXT,
            precio REAL,
            stock INTEGER,
            categoria TEXT
        )               
        """)

    #Creamos el comando SQL con marcadores '?'
    comando_sql = "INSERT INTO inventario (nombre, precio, stock, categoria) VALUES (?, ?, ?, ?) "

    #Caminamos fila por fila con iterrows
    for index, fila in df.iterrows():
        #Extraemos los valores de la fila actual en una tupla
        valores = (fila['nombre'], fila['precio'], fila['stock'], fila['categoria'])
        cursor.execute(comando_sql, valores)

    conexion.commit()
    conexion.close()



def procesar_datos(ruta_json):
    df = pd.read_json(ruta_json)
    df = normalizacion(df)
    df = eliminar_precio_negativo(df)
    df = eliminar_stock_negativo(df)
    exportar_json(df)
    return df

df = procesar_datos('./data/inventario.json')

cargar_a_sql(df, 'inventario.db')