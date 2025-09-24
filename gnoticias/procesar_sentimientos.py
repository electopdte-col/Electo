import json
import time
import google.generativeai as genai

# Funciones de DB
from gnoticias.db_gnoticias import (
    get_news_without_sentiment,
    update_news_sentiment
)
from gnoticias.db_log_ejecucion import log_start, log_end, log_error_update

# ================= CONSTANTES =================
API_KEYS_PATH = "api_keys.txt"
MODEL_NAME = "gemini-2.5-flash-lite" # Hardcoded model name

# ================= FUNCIONES =================

def get_api_key(key_name="GEMINI_API_KEY"):
    """Lee la clave de API desde el archivo api_keys.txt."""
    try:
        with open(API_KEYS_PATH, 'r') as f:
            for line in f:
                if line.startswith(key_name):
                    return line.strip().split('=')[1]
    except FileNotFoundError:
        print(f"Error: El archivo {API_KEYS_PATH} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Error al leer la clave de API: {e}")
        return None

def analizar_sentimiento(prompt):
    """Analiza el sentimiento de un texto usando el modelo Gemini 2.5 Flash-Lite."""
    print(f"🤖 Usando modelo: {MODEL_NAME}")
    
    response_schema = {
        "type": "object",
        "properties": {
            "tema_principal": {"type": "string"},
            "sentimiento": {"type": "string", "enum": ["Positivo", "Negativo", "Neutral"]}
        },
        "required": ["tema_principal", "sentimiento"]
    }
    
    modelo = genai.GenerativeModel(
        MODEL_NAME,
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": response_schema
        }
    )
    
    try:
        respuesta = modelo.generate_content(prompt)
        return json.loads(respuesta.text)
    except Exception as e:
        print(f"Ocurrió un error al generar contenido con {MODEL_NAME}: {e}")
        return None

def procesar_todas_las_noticias_sin_sentimiento(log_id=None):
    """Procesa todas las noticias sin sentimiento de una sola vez."""
    print("\nBuscando todas las noticias sin sentimiento...")
    try:
        news_batch = get_news_without_sentiment() # No limit, fetches all
    except Exception as e:
        print(f"❌ Error inesperado al obtener noticias: {e}")
        if log_id:
            log_error_update(log_id, e)
        return 0

    if not news_batch:
        print("✅ No hay más noticias por procesar.")
        return 0

    print(f"Se encontraron {len(news_batch)} noticias. Procesando...")
    total_procesadas = 0

    for news in news_batch:
        id_gnoticia = news["id_gnoticia"]
        titular = news["noticia"]
        candidato_nombre = news["candidato_nombre"]

        prompt = f'''
        Analiza el siguiente titular de una noticia sobre {candidato_nombre}.
        Extrae el tema principal en una frase corta (máx 5 palabras).
        Clasifica el sentimiento hacia {candidato_nombre} en: Positivo, Negativo, o Neutral.
        Retorna solo un JSON con claves "tema_principal" y "sentimiento".
        Titular: {titular}
        '''

        print(f"-> Analizando: {titular}")
        try:
            analisis_json = analizar_sentimiento(prompt)
            
            if analisis_json:
                sentimiento = analisis_json.get('sentimiento')
                tema = analisis_json.get('tema_principal')
                update_news_sentiment(id_gnoticia, sentimiento, tema)
                total_procesadas += 1
                print(f"   -> Análisis exitoso. Sentimiento: {sentimiento}")
            else:
                print("   -> Falló el análisis con IA. Se reintentará en la próxima ejecución.")

            time.sleep(5) # Pausa para no exceder limites de API

        except Exception as e:
            print(f"❌ Error procesando noticia {id_gnoticia}: {e}")
            if log_id:
                log_error_update(log_id, e)
            continue
    
    print(f"Proceso completado. {total_procesadas} noticias actualizadas.")
    return total_procesadas

# ========= EJECUCIÓN PRINCIPAL =========
def main():
    """Función principal para procesar todas las noticias sin sentimiento."""
    api_key = get_api_key()
    if not api_key:
        print("No se pudo obtener la API Key de Gemini. Abortando.")
        return
    genai.configure(api_key=api_key)

    log_id = log_start('procesar_sentimientos', 'Inicio de análisis de sentimientos faltantes')
    
    total_procesadas = procesar_todas_las_noticias_sin_sentimiento(log_id)

    print(f"\n🏁 Proceso completado. Total de noticias actualizadas: {total_procesadas}")
    log_end(log_id, estado='completed', mensaje=f'Total procesadas: {total_procesadas}')

if __name__ == "__main__":
    main()
