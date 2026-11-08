from Spark_session import spark_session
import Modelo as op

def main():
    spark = spark_session()
    ejercicio = input("Introduce el ejercicio que quieras hacer: ")

    if ejercicio == "1":
        print("Ej1a")
        df_ej1a = op.descargar_historico(spark, "BBVA.MC", "2024-10-01", "2026-10-01")
        df_ej1a.show(5)
        
        print("""Justificación de nulos:
        - Fecha: No permite nulos. Es el identificador principal de la sesión.
        - Open, High, Low, Close: No permiten nulos. Representan los precios de cotización obligatorios de la sesión.
        - Volume: Permite nulos. Representa las acciones negociadas y puede haber días sin registro de volumen.
        - Ticker: No permite nulos. Es el símbolo único que representa el activo.
        - Dividends, Stock Splits: No permiten nulos. Por defecto toman el valor 0.0 si no ocurren ese día.""")

        print("Ej1b")
        # Cargamos el CSV para extraer la lista de tickers
        df_tickers = spark.read.option("header", True).option("sep", ";").csv(r"aplicacion2026\data\ibex35_close-2024.csv")
        if "Fecha" in df_tickers.columns:
            df_tickers = df_tickers.drop("Fecha")
            
        df_ibex_completo = op.ejecutar_ej1b(spark, df_tickers, "2024-10-01", "2026-10-01")
        print(f"Total registros guardados en data/lake/bronze/ibex: {df_ibex_completo.count()}")

    elif ejercicio == "2":
        print("Ej2a")
        df_sentimiento = op.ejecutar_ej2a(spark, r"aplicacion2026\data\ibex35_close-2024.csv")
        df_sentimiento.printSchema()
        print("Un archivo CSV es simplemente un formato de texto plano y no guarda datos internos como el tipo de dato de cada columna.")


        print("Ej2b")
        filas_escritas = op.ejecutar_ej2b(df_sentimiento)
        print(f"Filas escritas en la capa bronze (sentimiento): {filas_escritas}")

    elif ejercicio == "3":
        print("Ej3")
        
        op.ejecutar_ej3(spark)
        
        # _delta_log
        print(
            "Comentario: Al convertir los ficheros, ha aparecido una nueva subcarpeta oculta llamada '_delta_log' dentro de cada directorio. "
            "Esta carpeta contiene el registro de transacciones compuesto por ficheros JSON. Esto es lo que convierte a Parquet "
            "en Delta, permitiendo tener transacciones ACID, actualizar, borrar y recuperar versiones anteriores sin mover ni reescribir los datos originales."
        )

if __name__ == "__main__":
    main()