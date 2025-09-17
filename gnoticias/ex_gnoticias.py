def similarity(a, b):
    """Similitud usando SequenceMatcher (0-100)."""
    return SequenceMatcher(None, a, b).ratio() * 100
import re
import unicodedata
from difflib import SequenceMatcher

# ================= IMPORTS =================
import re
import unicodedata
from difflib import SequenceMatcher
import sqlite3
import feedparser
import hashlib
import time
import json
import urllib.parse
from datetime import datetime, timedelta
from googlenewsdecoder import gnewsdecoder
import requests # Importa requests para un mejor manejo de errores

# Funciones de DB movidas a `db_gnoticias.py`
from gnoticias.db_gnoticias import (
    get_db_connection,
    news_exists_dia,
    save_news_to_db_dia,
    marcar_candidato_como_procesado,
    reset_candidatos_news,
)
from gnoticias.db_log_ejecucion import log_start, log_end, log_error_update, log_error_new

# ================= CONSTANTES =================
STOPWORDS_APELLIDO = {"de", "del", "la", "las", "los", "y", "san", "santa"}

# ================= FUNCIONES =================
def similarity(a, b):
    """Similitud usando SequenceMatcher (0-100)."""
    return SequenceMatcher(None, a, b).ratio() * 100

def normalize_text(text):
    """Normaliza el texto: minúsculas, sin tildes, puntuación como espacios."""
    text = text.lower().strip()
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def match_nombre_titular(nombre, titular):
    """
    Devuelve True si el titular contiene:
    - El apellido principal, o
    - El nombre completo exacto
    """
    nombre_norm = normalize_text(nombre)
    titular_norm = normalize_text(titular)
    tokens = nombre_norm.split()
    if not tokens:
        return False
    apellido = tokens[-1]
    nombre_completo = " ".join(tokens)
    if re.search(rf"\b{re.escape(apellido)}\b", titular_norm):
        return True
    if nombre_completo in titular_norm:
        return True
    return False

def limpiar_apellidos(tokens):
    """
    Quita partículas de apellidos (de, del, la, etc.)
    """
    return [t for t in tokens if t not in STOPWORDS_APELLIDO]

def buscar_nombre_en_titular(nombre, titular, umbral_nombre=85, umbral_fuzzy=85):
    nombre_norm = normalize_text(nombre)
    titular_norm = normalize_text(titular)
    partes = nombre_norm.split()
    if len(partes) < 2:
        return (None, 0, "nombre_invalido")
    partes_limpias = limpiar_apellidos(partes)
    nombre_simple = partes_limpias[0]
    apellido_paterno = partes_limpias[-2] if len(partes_limpias) >= 3 else partes_limpias[-1]
    apellido_materno = partes_limpias[-1]
    apellido_compuesto = " ".join(partes_limpias[-2:]) if len(partes_limpias) >= 3 else None
    if apellido_compuesto and apellido_compuesto in titular_norm:
        return (apellido_compuesto, 100, "apellido_compuesto")
    if f"{nombre_simple} {apellido_paterno}" in titular_norm:
        return (f"{nombre_simple} {apellido_paterno}", 100, "nombre_apellido_paterno")
    if f"{nombre_simple} {apellido_materno}" in titular_norm:
        return (f"{nombre_simple} {apellido_materno}", 100, "nombre_apellido_materno")
    if apellido_paterno in titular_norm:
        return (apellido_paterno, 100, "apellido_paterno")
    if apellido_materno in titular_norm:
        return (apellido_materno, 100, "apellido_materno")
    score_full = similarity(nombre_norm, titular_norm)
    if score_full >= umbral_nombre:
        return (nombre_norm, score_full, "nombre_completo")
    candidatos = []
    for palabra in titular_norm.split():
        for clave, tipo in [
            (apellido_paterno, "fuzzy_apellido_paterno"),
            (apellido_materno, "fuzzy_apellido_materno"),
            (nombre_simple, "fuzzy_nombre"),
        ]:
            score = similarity(clave, palabra)
            if score >= umbral_fuzzy:
                candidatos.append((palabra, score, tipo))
    if candidatos:
        return max(candidatos, key=lambda x: x[1])
    return (None, score_full, "no_match")
from gnoticias.db_log_ejecucion import log_start, log_end, log_error_update, log_error_new

# ========= PROCESAMIENTO DE ENTRADAS DEL FEED =========
def process_feed_entry(entry, candidato_id):
    """
    Extrae campos relevantes del feed RSS, genera un ID único
    y retorna un diccionario con los datos preliminares de la noticia.

    Args:
        entry: Un objeto de entrada de feedparser.
        candidato_id: El ID del candidato al que se relaciona la noticia.

    Returns:
        Un diccionario que contiene los datos de la noticia extraídos y procesados,
        o None si falta información esencial o falla el parseo.
    """
    titulo = entry.get("title", "")
    published_parsed = entry.get("published_parsed")


    if not published_parsed:
        print(f"⚠️ Entrada sin fecha publicada, omitiendo: {titulo}")
        return None

    try:
        # Extrae los componentes de fecha y hora de la tupla de fecha parseada
        fecha_dt = datetime(*published_parsed[:6])

        published_year, published_month, published_day = published_parsed.tm_year, published_parsed.tm_mon, published_parsed.tm_mday
        published_hour, published_minute = published_parsed.tm_hour, published_parsed.tm_min
        published_wday, published_yday = published_parsed.tm_wday, published_parsed.tm_yday
    except Exception as e:
        print(f"⚠️ Error al parsear fecha de entrada '{titulo}': {e}, omitiendo.")
        return None



    # Separa el título de la noticia y la fuente del medio del título de la entrada
    if " - " in titulo:
        parts = titulo.rsplit(" - ", 1)
        noticia = parts[0].strip() # Elimina espacios en blanco al inicio/final
        medio = parts[1].strip()   # Elimina espacios en blanco al inicio/final
    else:
        noticia = titulo.strip()
        medio = "Desconocido"

    # Decodifica la URL de Google News para obtener el enlace original
    google_news_url = entry.get("link", "")
    link = ""
    if google_news_url:
        try:
            # Usa gnewsdecoder para obtener la URL original
            result = gnewsdecoder(google_news_url)
            link = result.get("decoded_url", google_news_url) # Vuelve al original si falla la decodificación
        except Exception as e:
            print(f"Error al decodificar la URL {google_news_url}: {e}")
            link = google_news_url # Vuelve a la URL original de Google News
    else:
        print(f"⚠️ Entrada sin enlace, omitiendo: {noticia}")
        return None


    # Genera un ID corto usando el hash MD5 del ID largo (o el enlace si falta el ID)
    entry_id = entry.get("id", "")
    if not entry_id:
         entry_id = google_news_url # Usa el enlace como alternativa para el id
         if not entry_id:
             print(f"⚠️ Entrada sin ID ni enlace, omitiendo: {noticia}")
             return None

    # Obtiene el href de la fuente si está disponible
    source_href = entry.source.get("href", "") if hasattr(entry, "source") and entry.source else ""
    id_corto = hashlib.md5(entry_id.encode("utf-8")).hexdigest()

    # Retorna un diccionario con los datos de la noticia extraídos y procesados
    return {
        "candidato_id": candidato_id,
        "id": id_corto,
        "noticia": noticia,
        "medio": medio,
        "fecha": fecha_dt,
        "source_href": source_href,
        "ano": published_year,
        "mes": published_month,
        "dia": published_day,
        "hora": published_hour,
        "minuto": published_minute,
        "dia_sem": published_wday,
        "dia_ano": published_yday,
        "link": link,
        "id_largo": entry_id,
    }


# Las funciones de base de datos (conexión, consultas e inserciones)
# se encuentran ahora en `gnoticias/db_gnoticias.py` e importadas arriba.


# ========= OBTENCIÓN DE NOTICIAS DE GOOGLE =========
def build_gnews_url(query, hl="es-419", gl="CO", ceid="CO:es-419"):
    """
    Construye la URL del feed RSS de Google News para una consulta y parámetros dados.
    
    Args:
        query: La cadena de consulta de búsqueda.
        hl: El idioma de la interfaz (por defecto es "es").
        gl: La ubicación geográfica (por defecto es "CO").
        ceid: El ID de la edición de Google News (por defecto es "CO:es").
    
    Returns:
        La URL construida del feed RSS de Google News.
    """
    # Construye URL de Google News RSS. `query` debe incluir modificadores como "when:1d" si se requieren.
    params = {"q": query, "hl": hl, "gl": gl, "ceid": ceid}
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode(
        params, quote_via=urllib.parse.quote_plus
    )



def fetch_news_for_candidate(candidato_id, candidato_nombre, start_date, end_date, id_tema=None):
    """
    Obtiene noticias del día (when:1d) para un candidato y las guarda en la base de datos.
    Esta versión del script diario consulta solo las noticias de hoy para cada candidato.

    Args:
        candidato_id: El ID del candidato.
        candidato_nombre: El nombre del candidato.
        start_date, end_date: parámetros preservados por compatibilidad (no usados aquí).
    # ...existing code...
    """
    safe_name = " ".join(str(candidato_nombre).split())  # normaliza espacios para la consulta
    # Construye la consulta limitada a 1 día usando el modificador when:1d y búsqueda exacta
    query = f'"{safe_name}" when:1d'
    url = build_gnews_url(query, hl="es-419", gl="CO", ceid="CO:es-419")

    print(f"\n📅 hoy | URL: {url}")
    try:
        feed = feedparser.parse(url)
        if feed.bozo:
            print(f"⚠️ Error al parsear el feed: {feed.bozo_exception}")


        for entry in feed.entries:
            prelim = process_feed_entry(entry, candidato_id)
            if not prelim:
                continue


            coincidencia, score, tipo = buscar_nombre_en_titular(candidato_nombre, prelim["noticia"])
            if not coincidencia:
                print(f"⏩ Omitida por baja similitud ({score}, {tipo}): {prelim['noticia']}")
                continue
            else:
                print(f"✅ Guardada por similitud ({score}, {tipo}): {prelim['noticia']}")

            if news_exists_dia(prelim["id"], candidato_id):
                print(f"⚠️ Duplicada (omitida): {prelim['noticia']}")
                continue

            save_news_to_db_dia(prelim)

    except requests.exceptions.RequestException as e:
        print(f"❌ Error de red al obtener noticias: {e}")
    except Exception as e:
        print(f"❌ Error inesperado al procesar feed: {e}")

    marcar_candidato_como_procesado(candidato_id, campo="ex")


# ========= EJECUCIÓN PRINCIPAL =========
def main(start_date_str=None, end_date_str=None):
    """
    Función principal para procesar todos los candidatos pendientes.
    Itera sobre candidatos (uno por uno) y llama a fetch_news_for_candidate
    usando el mismo rango de fechas para todos.
    """
    # Rango de fechas (calcular una sola vez y reutilizar)
    if end_date_str:
        end_date = datetime.fromisoformat(end_date_str)
    else:
        end_date = datetime.now()

    if start_date_str:
        # acepta YYYY-MM-DD
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except Exception:
            # intentar parseo simple YYYY-MM-DD
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    else:
        # por defecto desde 2025-01-01
        start_date = datetime(2025, 1, 1)

    # Registra inicio de ejecución en log_ejecucion
    log_id = log_start('ex_gnoticias_diario', f'inicio procesamiento desde {start_date.date()}')

    # Resetea campo ex a NULL en candidatos antes de comenzar (uso tabla `candidatos`)
    try:
        reset_candidatos_news()
    except Exception as e:
        print(f"❌ No se pudo resetear 'news' en candidatos: {e}")

    # Itera candidatos hasta que no queden
    while True:
        with get_db_connection() as conn:
            cur = conn.cursor()
            try:
                # Solo selecciona candidatos cuyo id_tema sea NULL
                cur.execute("SELECT id_candidato, nombre, id_tema FROM candidatos WHERE ex IS NOT 1 AND id_tema IS NOT NULL ORDER BY id_candidato ASC LIMIT 1")
                row = cur.fetchone()
            except sqlite3.OperationalError as e:
                print(f"❌ Error al consultar candidatos: {e}")
                row = None
            except Exception as e:
                print(f"❌ Error inesperado al obtener candidato: {e}")
                row = None

        if not row:
            print("✅ No hay más candidatos por procesar. Proceso finalizado.")
            log_end(log_id, estado='finished', mensaje='No quedan candidatos')
            break

        candidato_id, candidato_nombre, id_tema = row
        print(f"✅ Procesando: {candidato_nombre} (ID {candidato_id})")
        # Procesa y guarda noticias para el candidato seleccionado
        try:
            fetch_news_for_candidate(candidato_id, candidato_nombre, start_date, end_date, id_tema=id_tema)
        except Exception as e:
            print(f"❌ Error procesando candidato {candidato_id}: {e}")
            try:
                # intenta actualizar el log existente
                log_error_update(log_id, e)
            except Exception:
                # si falla al actualizar, crea un nuevo registro de error
                log_error_new('ex_gnoticias_diario', e)
            raise

    # ...existing code...

if __name__ == "__main__":
    # Agrega un candidato de ejemplo y ejecuta la función principal
    # add_candidate("Gustavo Petro") # Candidato de ejemplo
    import argparse
    parser = argparse.ArgumentParser(description="Obtener noticias de Google News para candidatos")
    parser.add_argument("--start-date", type=str, help="Fecha de inicio YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", type=str, help="Fecha de fin YYYY-MM-DD (exclusive)")
    args = parser.parse_args()
    main(start_date_str=args.start_date, end_date_str=args.end_date)


