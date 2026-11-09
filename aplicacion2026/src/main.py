from Spark_session import spark_session
import Operaciones as op

def main():
    spark = spark_session()
    ejercicio = input("Introduce el ejercicio que quieras hacer: ")

    if ejercicio == "1":
        print("Ej1a")
        df_ej1a = op.descargar_historico(spark, "BBVA.MC", "2024-10-01", "2026-10-01")
        df_ej1a.show(5)
        
        print("""Justificación de los nulos:
        - Fecha: No permite nulos. Es el identificador principal de la sesión.
        - Open, High, Low, Close: No permiten nulos. Representan los precios de cotización obligatorios de la sesión.
        - Volume: Permite nulos. Representa las acciones negociadas y puede haber días sin registro de volumen.
        - Ticker: No permite nulos. Es el símbolo único que representa el activo.
        - Dividends, Stock Splits: No permiten nulos. Por defecto toman el valor 0.0 si no ocurren ese día.""")

        print("Ej1b")
        df_tickers = spark.read.option("header", True).option("sep", ";").csv(r"aplicacion2026\data\ibex35_close-2024.csv")
        if "Fecha" in df_tickers.columns:
            df_tickers = df_tickers.drop("Fecha")
            
        df_ibex_completo = op.ejecutar_ej1b(spark, df_tickers, "2024-10-01", "2026-10-01")
        print(f"Total registros guardados en data/lake/bronze/ibex: {df_ibex_completo.count()}")

    elif ejercicio == "2":
        print("Ej2a")
        df_sentimiento = op.ejecutar_ej2a(spark, r"aplicacion2026\data\sentimiento_ibex.csv")
        df_sentimiento.printSchema()
        print("Un archivo CSV es simplemente un formato de texto plano y no guarda datos internos como el tipo de dato de cada columna.")


        print("Ej2b")
        op.ejecutar_ej2b(df_sentimiento)

    elif ejercicio == "3":
        print("Ej3")
        
        op.ejecutar_ej3(spark)
        
        # _delta_log
        print("Al convertir los ficheros, ha aparecido una nueva subcarpeta oculta llamada '_delta_log' dentro de cada directorio. "
            "Esta carpeta contiene el registro de transacciones compuesto por ficheros JSON. Esto es lo que convierte a Parquet "
            "en Delta, permitiendo tener transacciones ACID, actualizar, borrar y recuperar versiones anteriores sin mover ni reescribir los datos originales."
        )

    elif ejercicio == "4":
        print("Ej4")
        df_tickers = spark.read.option("header", True).option("sep", ";").csv(r"aplicacion2026\data\ibex35_close-2024.csv")
        if "Fecha" in df_tickers.columns:
            df_tickers = df_tickers.drop("Fecha")
        op.ejecutar_ej4(spark, df_tickers, "2026-10-01", "2026-11-08")
        
        print("""
        - En la primera ejecución, comprobamos que el número de filas sube porque se insertan los registros de los días nuevos.
        - Al ejecutar por segunda vez, el total de filas se mantiene intacto. Merge inserta los datos que no existen pero al no detectar filas nuevas en la segunda pasada, no duplica información.""")

    elif ejercicio == "5":

        op.ejecutar_ej5(spark)

    elif ejercicio == "6":

        op.ejecutar_ej6(spark)

    elif ejercicio == "7":

        print("Ej7")
        
        df_tickers = spark.read.option("header", True).option("sep", ";").csv(r"aplicacion2026\data\ibex35_close-2024.csv")
        if "Fecha" in df_tickers.columns:
            df_tickers = df_tickers.drop("Fecha")
        df_dimension_empresa = op.ejecutar_ej7(spark, df_tickers)
        
        df_dimension_empresa.show(5, truncate=False)

    elif ejercicio == "8":

        print("Ej8")
        
        filas_bronze, filas_silver, df_silver_sentimiento = op.ejecutar_ej8(spark)
        
        print(f"Filas en la tabla Bronze: {filas_bronze}")
        print(f"Filas en la tabla Silver: {filas_silver}")
        
        df_silver_sentimiento.show(5)

    elif ejercicio == "9":

        #Ej 7 para volver a obtener la variable df_dimension_empresa
        df_tickers = spark.read.option("header", True).option("sep", ";").csv(r"aplicacion2026\data\ibex35_close-2024.csv")
        if "Fecha" in df_tickers.columns:
            df_tickers = df_tickers.drop("Fecha")
        df_dimension_empresa = op.ejecutar_ej7(spark, df_tickers)

        # Ej 9
        df_dim_silver, df_precios_silver = op.ejecutar_ej9(spark, df_dimension_empresa)

        df_dim_silver.show(5, truncate=False)
        df_precios_silver.show(5, truncate=False)

    elif ejercicio == "10":

        print("\nEj10")
        df_fact = op.ejecutar_ej10(spark)
        print("Tabla de Hechos en la capa Gold:")
        df_fact.select("Date", "Ticker", "Close", "Sentimiento").show(5)

    elif ejercicio == "13":

        print("\nEj13")
        
        filas_fact, filas_dim = op.ejecutar_ej13(spark)
        
        print(f"Tabla de Hechos exportada: {filas_fact} registros que hemos particionado por Ticker.")
        print(f"Tabla de Dimensión exportada: {filas_dim} registros.")
    
if __name__ == "__main__":
    main()