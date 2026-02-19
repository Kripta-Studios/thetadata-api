"""
Ejemplo corregido de uso para descargar, verificar y limpiar datos del subyacente SPX,
usando los módulos locales.
"""

import asyncio
from client import ThetaClient
from utils import verify_data_integrity, fix_empty_rows

async def main():
    # 1. Configurar el cliente (por defecto ya apunta a http://127.0.0.1:25503/v3)
    client = ThetaClient()
    
    # 2. Definir parámetros
    symbol = "SPX"
    target_date = "2026-02-18"
    interval = "1m"
    
    try:
        print(f"Descargando datos de {symbol} para la fecha {target_date}...")
        
        # 3. Descargar datos del subyacente
        underlying = await client.fetch_underlying_ohlc(
            symbol=symbol, 
            date=target_date, 
            interval=interval
        )
        
        # Extraemos el DataFrame puro (Pandas)
        df = underlying.data
        
        # 4. Verificar integridad
        print("Verificando integridad de los datos...")
        integrity = verify_data_integrity(df)
        print(f"Estado inicial: {integrity['message']}")
        
        # 5. Aplicar limpieza si se detectan valores nulos
        if not integrity["valid"]:
            print("Aplicando limpieza y rellenado de datos (forward/backward fill)...")
            df = fix_empty_rows(df)
            
            # Volvemos a comprobar tras limpiar
            integrity = verify_data_integrity(df)
            print(f"Estado tras la limpieza: {integrity['message']}")
        
        # 6. Mostrar resultados
        print("\n✅ Datos descargados y limpiados exitosamente")
        print(f"Dimensiones del dataset: {df.shape}")
        print("\nPrimeras 5 filas:")
        print(df.head())
        
    except Exception as e:
        print(f"\n❌ Error durante la ejecución: {e}")
    finally:
        # Cerrar el cliente HTTP de forma segura
        await client.close()

if __name__ == "__main__":
    # Ejecutamos el bucle asíncrono
    asyncio.run(main())