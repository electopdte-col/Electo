import re
import unicodedata
from difflib import SequenceMatcher
import sqlite3
import feedparser
import hashlib
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from googlenewsdecoder import gnewsdecoder
import requests

# Funciones de DB
from gnoticias.db_gnoticias import (
    get_db_connection,
    news_exists_in_gnoticias,
    save_news_to_gnoticias,
    marcar_candidato_como_procesado,
    reset_candidatos_news,
)
from gnoticias.db_log_ejecucion import log_start, log_end, log_error_update, log_error_new

# ================= CONSTANTES =================
STOPWORDS_APELLIDO = {"de", "del", "la", "las", "los", "y", "san", "santa"}

# Zona horaria de Colombia
COL_TZ = timezone(timedelta(hours=-5))

# ================= FUNCIONES =================
def similarity(a, b):
    """Similitud usando SequenceMatcher (0-100)."""
    return SequenceMatcher(None, a, b).ratio() * 100

def normalize_text(text):
    """Normaliza el texto: minúsculas, sin tildes, puntuación como espacios."""
    if not text:
        return ""
    text = unicodedata.normalize('NFD', text.lower()).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9ñ\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def normalize_to_colombia_time(fecha_dt):
    # Si fecha_dt NO tiene tzinfo, asumimos que viene en UTC (caso Google News)
    if fecha_dt.tzinfo is None:
        fecha_dt = fecha_dt.replace(tzinfo=timezone.utc)
    # Convertimos a UTC-5
    return fecha_dt.astimezone(COL_TZ)

def process_feed_entry(entry, candidato_id):
    titulo = entry.get("title", "")
    published_parsed = entry.get("published_parsed")
    if not published_parsed:
        print(f"⚠️ Entrada sin fecha publicada, omitiendo: {titulo}")
        return None
    try:
        fecha_dt = datetime(*published_parsed[:6])
        published_year, published_month, published_day = fecha_dt.year, fecha_dt.month, fecha_dt.day
        published_hour, published_minute = fecha_dt.hour, fecha_dt.minute
        published_wday, published_yday = fecha_dt.weekday(), fecha_dt.timetuple().tm_yday
    except Exception as e:
        print(f"⚠️ Error al parsear fecha de entrada '{titulo}': {e}, omitiendo.")
        return None
    if " - " in titulo:
        parts = titulo.rsplit(" - ", 1)
        noticia = parts[0].strip()
        medio = parts[1].strip()
    else:
        noticia = titulo.strip()
        medio = "Desconocido"

    # Normalizar timezone
    fecha_local = normalize_to_colombia_time(fecha_dt)

    google_news_url = entry.get("link", "")
    link = gnewsdecoder(google_news_url).get("decoded_url", google_news_url) if google_news_url else ""
    if not link:
        return None

    entry_id = entry.get("id", google_news_url)
    id_corto = hashlib.md5(entry_id.encode("utf-8")).hexdigest()
    return {
        "candidato_id": candidato_id, "id": id_corto, "noticia": noticia, "medio": medio,
        "fecha": fecha_local,   # <-- fecha final en Colombia
        "source_href": entry.source.get("href", "") if hasattr(entry, "source") else "",
        "ano": fecha_local.year,
        "mes": fecha_local.month,
        "dia": fecha_local.day,
        "hora": fecha_local.hour,
        "minuto": fecha_local.minute,
        "dia_sem": fecha_local.weekday(),
        "dia_ano": fecha_local.timetuple().tm_yday,
        "link": link, "id_largo": entry_id,
    }



def fetch_news_for_candidate(candidato_id, candidato_nombre, start_date, end_date, id_tema=None, keywords=None, log_id=None):
    """Obtiene noticias, las analiza y las guarda directamente en la tabla gnoticias."""
    safe_name = " ".join(str(candidato_nombre).split())
    query = f'"{safe_name}" when:1d'
    url = "https://news.google.com/rss/search?" + urllib.parse.urlencode({"q": query, "hl": "es-419", "gl": "CO", "ceid": "CO:es-419"})
    print(f"\n📅 hoy | URL: {url}")
    try:
        feed = feedparser.parse(url)
        if feed.bozo:
            print(f"⚠️ Error al parsear el feed: {feed.bozo_exception}")

        for entry in feed.entries:
            prelim = process_feed_entry(entry, candidato_id)
            if not prelim:
                continue

            if not keywords:
                print(f"⏩ Omitida por falta de keywords para el candidato: {candidato_nombre}")
                continue

            keyword_list = [k.strip() for k in keywords.split(',')]
            normalized_noticia = normalize_text(prelim["noticia"])
            
            es_relevante = False
            for keyword in keyword_list:
                normalized_keyword = normalize_text(keyword)
                if normalized_keyword in normalized_noticia:
                    es_relevante = True
                    break # Found a keyword, no need to check others
            
            if not es_relevante:
                print(f"⏩ Omitida por no contener keywords: {prelim['noticia']}")
                continue

            if news_exists_in_gnoticias(prelim["id"], candidato_id):
                print(f"⚠️ Duplicada en gnoticias (omitida): {prelim['noticia']}")
                continue

            prelim['id_log'] = log_id # Asignar el id de log a la noticia
            print(f"-> Relevante. Guardando noticia: {prelim['noticia']}")
            save_news_to_gnoticias(prelim)
            time.sleep(1)

    except requests.exceptions.RequestException as e:
        print(f"❌ Error de red al obtener noticias: {e}")
    except Exception as e:
        print(f"❌ Error inesperado al procesar feed: {e}")

    marcar_candidato_como_procesado(candidato_id, campo="ex")

def main(start_date_str=None, end_date_str=None):
    """Función principal para procesar todos los candidatos pendientes."""
    

    log_id = log_start('ex_gnoticias_diario', 'inicio procesamiento')
    try:
        reset_candidatos_news()
    except Exception as e:
        print(f"❌ No se pudo resetear 'news' en candidatos: {e}")

    while True:
        with get_db_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("SELECT id_candidato, nombre, id_tema, keywords FROM candidatos WHERE ex IS NOT 1 AND id_tema IS NOT NULL ORDER BY id_candidato ASC LIMIT 1")
                row = cur.fetchone()
            except Exception as e:
                print(f"❌ Error inesperado al obtener candidato: {e}")
                row = None

        if not row:
            print("✅ No hay más candidatos por procesar. Proceso finalizado.")
            break

        candidato_id, candidato_nombre, id_tema, keywords = row
        print(f"✅ Procesando: {candidato_nombre} (ID {candidato_id})")
        try:
            fetch_news_for_candidate(candidato_id, candidato_nombre, None, None, id_tema=id_tema, keywords=keywords, log_id=log_id)
        except Exception as e:
            print(f"❌ Error procesando candidato {candidato_id}: {e}")
            log_error_update(log_id, e)

    log_end(log_id, estado='finished', mensaje='Proceso diario completado.')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Obtener y analizar noticias de Google News para candidatos.")
    args = parser.parse_args()
    main()
