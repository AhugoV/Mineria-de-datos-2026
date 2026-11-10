import yfinance as yf
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from delta.tables import DeltaTable
from Spark_session import spark_session
from Database_spark import database_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
#Le dije a la ia que me diera todos los imports, a lo mejor alguno sobra


def descargar_historico(spark, ticker, inicio, fin):

    pdf = yf.Ticker(ticker).history(start=inicio, end=fin, interval="1d", auto_adjust=False)
    pdf = pdf.reset_index()

    # Por algún motivo, la columna ticker no se me descargaba en el dataframe.
    pdf["Ticker"] = ticker
    
    df_pyspark = spark.createDataFrame(pdf)

    # Parece ser que los datos se descargaban con un espacio en adj close y stock splits y me daba un error al transformalos a delta. Solucioné el error con gemini.
    for nombre_columna in df_pyspark.columns:
        nuevo_nombre = nombre_columna.replace(" ", "_")
        df_pyspark = df_pyspark.withColumnRenamed(nombre_columna, nuevo_nombre)

    return df_pyspark

def ejecutar_ej1b(spark, df_tickers, inicio, fin):

    lista_columnas = df_tickers.columns
    df_ibex_completo = None

    for ticker_yf in lista_columnas:
        
        df = descargar_historico(spark, ticker_yf, inicio, fin)
        if df_ibex_completo is None:
            df_ibex_completo = df
        else:    
            df_ibex_completo = df_ibex_completo.union(df)

    df.write.mode("overwrite").parquet("aplicacion2026\data/lake/bronze/ibex")
    return df_ibex_completo

def ejecutar_ej2a(spark, ruta_csv):

    df_sentimiento = spark.read.option("header", True).option("sep", ";").csv(ruta_csv)
    df_sentimiento.printSchema()
    return df_sentimiento

def ejecutar_ej2b(df_sentimiento):

    df_sentimiento.write.mode("overwrite").parquet("aplicacion2026/data/lake/bronze/sentimiento")

    # Número de filas del DataFrame
    filas_escritas = df_sentimiento.count()
    print(f"Filas escritas en la capa bronze: {filas_escritas}")

def ejecutar_ej3(spark):
    # Usar las comillas invertidas dentro del string rodeando la ruta.
    DeltaTable.convertToDelta(spark, "parquet.`aplicacion2026/data/lake/bronze/ibex`")
    DeltaTable.convertToDelta(spark, "parquet.`aplicacion2026/data/lake/bronze/sentimiento`")

    print("Historial de la tabla Delta 'ibex':")
    DeltaTable.forPath(spark, "aplicacion2026/data/lake/bronze/ibex").history().show()

    print("Historial de la tabla Delta 'sentimiento':")
    DeltaTable.forPath(spark, "aplicacion2026/data/lake/bronze/sentimiento").history().show()

    # _delta_log
    print(
        "Comentario: Al convertir los ficheros, ha aparecido una nueva subcarpeta oculta llamada '_delta_log' dentro de cada directorio. "
        "Esta carpeta contiene el registro de transacciones compuesto por ficheros JSON. Esto es lo que convierte a Parquet "
        "en Delta, permitiendo tener transacciones ACID, actualizar, borrar y recuperar versiones anteriores sin mover ni reescribir los datos originales."
    )

def ejecutar_ej4(spark, df_tickers, fecha_inicio, fecha_fin):
    
    tabla_ibex = DeltaTable.forPath(spark, "aplicacion2026/data/lake/bronze/ibex")
    
    version_actual = tabla_ibex.history(1).select("version").collect()[0][0]
    print(f"Versión actual de la tabla IBEX: {version_actual}")
    
    filas_antes = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex").count()
    print(f"Filas ANTES del MERGE: {filas_antes}")
    
    lista_columnas = df_tickers.columns
    df_nuevos_datos = None
    
    for ticker_yf in lista_columnas:
        df = descargar_historico(spark, ticker_yf, fecha_inicio, fecha_fin)
        if df_nuevos_datos is None:
            df_nuevos_datos = df
        else:    
            df_nuevos_datos = df_nuevos_datos.union(df)

            
    (tabla_ibex.alias("d")
        .merge(df_nuevos_datos.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date")
        .whenNotMatchedInsertAll()
        .execute())
        
    filas_despues_1 = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex").count()
    print(f"Filas después de la PRIMERA ejecución del MERGE: {filas_despues_1}")
    
    (tabla_ibex.alias("d")
        .merge(df_nuevos_datos.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date")
        .whenNotMatchedInsertAll()
        .execute())
        
    filas_despues_2 = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex").count()
    print(f"Filas después de la SEGUNDA ejecución del MERGE: {filas_despues_2}")

def ejecutar_ej5(spark):

    print("Ej5a")

    df_ibex = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex")
    
    df_mas5 = df_ibex.withColumn("Close_mas5", col("Close") + 5)
    
    # Intentamos escribir en modo append. Lo metemos en un try-except porque va a fallar debido a la protección schema enforcement.
    try:
        df_mas5.write.format("delta").mode("append").save("aplicacion2026/data/lake/bronze/ibex")
    except AnalysisException as e:
        print("¡Operación rechazada por Delta Lake (Schema Enforcement)!")
        print(f"Error devuelto: {str(e).split(';')[0]}") # Imprimimos solo la primera parte del error para no saturar

    print("Ej5b")
    # Repetimos la escritura pero activando la evolución de esquema mergeSchema
    (df_mas5.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("aplicacion2026/data/lake/bronze/ibex"))
    print("Escritura completada con éxito. La columna 'Close_mas5' ha sido integrada en la tabla.")

    print("Ej5c")
    tabla_ibex = DeltaTable.forPath(spark, "aplicacion2026/data/lake/bronze/ibex")
    
    filas_antes = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex").count()
    print(f"Filas ANTES: {filas_antes}")
    
    tabla_ibex.delete("Ticker = 'BBVA.MC'")
    
    filas_despues = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex").count()
    print(f"Filas DESPUÉS: {filas_despues}")

    print("Ej5d")
    print("Historial tras las operaciones --------------------------------------------")
    tabla_ibex.history().show()
    
    # Extraemos el número de la versión actual para calcular cuál era la versión del Ej4
    # Como todos los apartados del ej5 estan en la misma función, en el main al poner 5, 
    # se ejecutal el b y el c siempre y la versión debería ser (version_actual - 2).
    version_actual = tabla_ibex.history(1).select("version").collect()[0][0]
    version_antigua = version_actual - 2
    
    print(f"--- Leyendo la versión antigua (v{version_antigua}) con versionAsOf ---")
    df_v_antigua = spark.read.format("delta").option("versionAsOf", version_antigua).load("aplicacion2026/data/lake/bronze/ibex")
    
    print(f"Columnas en la versión antigua: {df_v_antigua.columns}")
    filas_bbva_antiguo = df_v_antigua.filter("Ticker = 'BBVA.MC'").count()
    print(f"Número de filas de BBVA en esta versión antigua: {filas_bbva_antiguo}")

def ejecutar_ej6(spark):
    
    print("Ej6")
    tabla_ibex = DeltaTable.forPath(spark, "aplicacion2026/data/lake/bronze/ibex")
    
    # Determinamos la versión a la que queremos restaurar. 
    # Sabiendo que el ej5 generó 2 nuevos commits (el append/mergeSchema y el DELETE),    Para esta parte del ejercicio, usé la ia, ya que me lié un poco
    # la versión del ej4 será (versión actual - 2).                                        con este ejercicio y el último apartado del anterior.
    version_actual = tabla_ibex.history(1).select("version").collect()[0][0]
    version_restaurar = version_actual - 2
    
    print(f"Restaurando la tabla a la versión {version_restaurar} (estado tras el Ejercicio 4)...")
    
    tabla_ibex.restoreToVersion(version_restaurar)
    
    print("--- Historial DESPUÉS del RESTORE ---")
    tabla_ibex.history().show()
    
    df_actual = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex")
    
    filas_bbva = df_actual.filter("Ticker = 'BBVA.MC'").count()
    columnas = df_actual.columns
    
    print(f"Filas de BBVA en la tabla actual (restaurada): {filas_bbva}")
    print(f"¿Existe la columna 'Close_mas5' en la tabla actual?: {'Close_mas5' in columnas}")

def ejecutar_ej7(spark, df_tickers):
    
    lista_columnas = df_tickers.columns
    datos_sectores = []

    for ticker_yf in lista_columnas:
        # objeto info de la API de Yahoo para extraer datos
        info = yf.Ticker(ticker_yf).info
        
        sector = info.get('sector')
        
        #Extraemos el nombre de la empresa como shortName ya que asi venia en la api
        nombre = info.get('shortName')
        
        datos_sectores.append((ticker_yf, nombre, sector))

    # DataFrame de Spark
    esquema = StructType([
        StructField("Ticker", StringType(), False),
        StructField("Nombre_Empresa", StringType(), True),
        StructField("Sector_Yahoo_EN", StringType(), True)]) # Aquí en este momento pensaba que los sectores de mi csv estaban en español, por eso puse aquí el _EN
    
    df_sectores = spark.createDataFrame(datos_sectores, schema=esquema)
    return df_sectores

def ejecutar_ej8(spark):

# Supuestamente los sectores de mi csv estaban en español pero no lo estan, a si que no hay que hacer cambios en ese sentido y los dejo todos en ingles.
# Sentimiento_ibex ya esta en formato largo a si que no hace falta hacerle unpivot, que en el pdf ponia que habia que usar esa función.
    df_bronze = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/sentimiento")
    filas_bronze = df_bronze.count()
    
    df_limpio = df_bronze.withColumn("Sector", initcap(trim(col("Sector"))))
    
    # Castear fecha y números por si acaso vienen en string
    df_limpio = df_limpio.withColumn("Fecha", to_date(col("Fecha")))
    df_limpio = df_limpio.withColumn("Sentimiento", col("Sentimiento").cast("double"))
    
    df_limpio = df_limpio.dropna().dropDuplicates()
    
    df_limpio.write.format("delta").mode("overwrite").save("aplicacion2026/data/lake/silver/sentimiento")
    
    return filas_bronze, df_limpio.count(), df_limpio

def ejecutar_ej9(spark, df_dimension_empresa):

    # Aplicamos la limpieza de texto a la dimensión de yahoo
    df_dim_silver = df_dimension_empresa.withColumn("Sector_Yahoo_EN", initcap(trim(col("Sector_Yahoo_EN"))))
    
    # Renombramos la columna para estandarizar
    df_dim_silver = df_dim_silver.withColumnRenamed("Sector_Yahoo_EN", "Sector")
    df_dim_silver.write.format("delta").mode("overwrite").save("aplicacion2026/data/lake/silver/dim_empresa")
    
    # Los precios de yahoo ya vienen limpios, solo los movemos de bronze a silver
    df_precios = spark.read.format("delta").load("aplicacion2026/data/lake/bronze/ibex")
    df_precios.write.format("delta").mode("overwrite").save("aplicacion2026/data/lake/silver/ibex")
    
    return df_dim_silver, df_precios

def ejecutar_ej10(spark):

    df_precios = spark.read.format("delta").load("aplicacion2026/data/lake/silver/ibex")
    df_dim = spark.read.format("delta").load("aplicacion2026/data/lake/silver/dim_empresa")
    df_sent = spark.read.format("delta").load("aplicacion2026/data/lake/silver/sentimiento")
    
    # Traemos temporalmente el sector a la tabla de precios cruzando por ticker
    df_fact = df_precios.join(df_dim, on="Ticker", how="inner")
    
    # Cruzamos con la tabla de sentimiento usando la fecha y el sector
    # ¡¡¡¡En yahoo la fecha se llama sate, en el csv se llama fecha!!!!. IMPORTANTE ESTO
    df_fact = df_fact.join(df_sent,(df_fact["Date"] == df_sent["Fecha"]) & (df_fact["Sector"] == df_sent["Sector"]),how="left") # Mantenemos los precios aunque ese día no haya sentimiento
    
    # En el diseño de estrella la tabla de hechos solo debe tener métricas como slose, sentimiento y ticker o date, para ir agrupaando los datos. 
    # Sector y fecha los borramos.
    df_fact = df_fact.drop(df_sent["Fecha"]).drop(df_sent["Sector"]).drop(df_dim["Nombre_Empresa"]).drop(df_dim["Sector"])
    
    df_fact.write.format("delta").mode("overwrite").save("aplicacion2026/data/lake/gold/fact_cotizaciones")
    df_dim.write.format("delta").mode("overwrite").save("aplicacion2026/data/lake/gold/dim_empresa")
    
    return df_fact

def ejecutar_ej13(spark):

    df_fact = spark.read.format("delta").load("aplicacion2026/data/lake/gold/fact_cotizaciones")
    df_dim = spark.read.format("delta").load("aplicacion2026/data/lake/gold/dim_empresa")

    # Exportamos la dimensión y los hechos usando coalesce, porque asi lo requiere la práctica 2 de power bi.
    df_dim.coalesce(1).write.mode("overwrite").parquet("aplicacion2026/data/export/gold/dim_empresa")
    df_fact.coalesce(1).write.mode("overwrite").parquet("aplicacion2026/data/export/gold/fact_cotizaciones")
    
    return df_fact.count(), df_dim.count()