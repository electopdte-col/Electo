import sqlite3
from datetime import datetime

DB_PATH = "data/Eto_col26.db"  # Ruta a la base de datos SQLite


class DatabaseConnection:
    """Gestor de contexto para la conexión a la base de datos."""
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            return self.conn
        except sqlite3.Error as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        if exc_val:
            print(f"❌ Ocurrió un error durante la operación de base de datos: {exc_val}")


def get_db_connection():
    """Retorna un gestor de contexto DatabaseConnection."""
    return DatabaseConnection(DB_PATH)


def news_exists(news_id, candidato_id):
    """Verifica si una noticia ya existe en la tabla `gnoticias_ex_his`."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM gnoticias_ex_his WHERE id_gnoticia = ? AND id_candidato = ?", (news_id, candidato_id))
            exists = cur.fetchone() is not None
        return exists
    except Exception as e:
        print(f"❌ Error al verificar existencia de noticia {news_id}: {e}")
        return False


def save_news_to_db(news_data):
    """Inserta una nueva noticia en la tabla `gnoticias_ex_his`."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO gnoticias_ex_his (
                    id_candidato, id_gnoticia, noticia, medio, fecha, source_href,
                    link, ano, mes, dia, hora, minuto, dia_sem, dia_ano, id_original
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                news_data["candidato_id"],        # id_candidato
                news_data["id"],                 # id_gnoticia (id corto)
                news_data["noticia"],
                news_data["medio"],
                news_data["fecha"],
                news_data["source_href"],
                news_data["link"],               # link de la noticia
                news_data["ano"],
                news_data["mes"],
                news_data["dia"],
                news_data["hora"],
                news_data["minuto"],
                news_data["dia_sem"],
                news_data["dia_ano"],
                news_data["id_largo"]             # id_original
            ))
            conn.commit()
            print(f"✅ Guardada: {news_data['noticia']}")
    except sqlite3.IntegrityError:
        print(f"⚠️ Error de integridad al guardar: {news_data['noticia']} (posible duplicado de link o id_largo)")
    except Exception as err:
        print(f"❌ Error al guardar noticia '{news_data['noticia']}': {err}")


def marcar_candidato_como_procesado(candidato_id, campo="ex"):
    """Marca un candidato como procesado en la tabla `candidatos` usando el campo especificado ('ex' o 'his')."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            if campo not in ("ex", "his"):
                raise ValueError(f"Campo inválido para marcar como procesado: {campo}")
            cur.execute(f"UPDATE candidatos SET {campo} = 1 WHERE id_candidato = ?", (candidato_id,))
            conn.commit()
        print(f"🟢 Candidato {candidato_id} marcado como procesado ({campo}=1).")
    except Exception as e:
        print(f"❌ Error al marcar candidato {candidato_id} como procesado: {e}")


def news_exists_dia(news_id, candidato_id):
    """Verifica si una noticia ya existe en la tabla `gnoticias_ex`."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM gnoticias_ex WHERE id_gnoticia = ? AND id_candidato = ?", (news_id, candidato_id))
            exists = cur.fetchone() is not None
        return exists
    except Exception as e:
        print(f"❌ Error al verificar existencia de noticia (dia) {news_id}: {e}")
        return False


def save_news_to_db_dia(news_data):
    """Inserta una nueva noticia en la tabla `gnoticias_ex`."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO gnoticias_ex (
                    id_candidato, id_gnoticia, noticia, medio, fecha, source_href,
                    link, ano, mes, dia, hora, minuto, dia_sem, dia_ano, id_original
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                news_data["candidato_id"],        # id_candidato
                news_data["id"],                 # id_gnoticia (id corto)
                news_data["noticia"],
                news_data["medio"],
                news_data["fecha"],
                news_data["source_href"],
                news_data["link"],               # link de la noticia
                news_data["ano"],
                news_data["mes"],
                news_data["dia"],
                news_data["hora"],
                news_data["minuto"],
                news_data["dia_sem"],
                news_data["dia_ano"],
                news_data["id_largo"]             # id_original
            ))
            conn.commit()
            print(f"✅ (dia) Guardada: {news_data['noticia']}")
    except sqlite3.IntegrityError:
        print(f"⚠️ Error de integridad al guardar (dia): {news_data['noticia']} (posible duplicado de link o id_original)")
    except Exception as err:
        print(f"❌ Error al guardar noticia (dia) '{news_data['noticia']}': {err}")

def reset_candidatos_news():
    """Resetea el campo 'ex' a NULL para todos los registros de candidatos."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE candidatos SET ex = NULL")
            conn.commit()
        print("ℹ️ Campo 'ex' de 'candidatos' reseteado a NULL (función).")
    except Exception as e:
        print(f"❌ Error al resetear 'news' en candidatos: {e}")

def save_news_to_gnoticias_with_sentiment(news_data):
    """Inserta una nueva noticia con su análisis de sentimiento en la tabla `gnoticias`."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO gnoticias (
                    id_candidato, id_gnoticia, noticia, medio, fecha, source_href,
                    link, ano, mes, dia, hora, minuto, dia_sem, dia_ano, id_original,
                    sentimiento, tema
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                news_data["candidato_id"],
                news_data["id"],
                news_data["noticia"],
                news_data["medio"],
                news_data["fecha"],
                news_data["source_href"],
                news_data["link"],
                news_data["ano"],
                news_data["mes"],
                news_data["dia"],
                news_data["hora"],
                news_data["minuto"],
                news_data["dia_sem"],
                news_data["dia_ano"],
                news_data["id_largo"],
                news_data.get("sentimiento"), # Use .get() for safety
                news_data.get("tema")         # Use .get() for safety
            ))
            conn.commit()
            print(f"✅ (gnoticias) Guardada con análisis: {news_data['noticia']}")
    except sqlite3.IntegrityError:
        # This is expected if the news already exists, so we can ignore it or log it quietly.
        pass
    except Exception as err:
        print(f"❌ Error al guardar noticia en gnoticias '{news_data['noticia']}': {err}")

def news_exists_in_gnoticias(news_id, candidato_id):
    """Verifica si una noticia ya existe en la tabla `gnoticias`."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM gnoticias WHERE id_gnoticia = ? AND id_candidato = ?", (news_id, candidato_id))
            exists = cur.fetchone() is not None
        return exists
    except Exception as e:
        print(f"❌ Error al verificar existencia de noticia en gnoticias {news_id}: {e}")
        return False

def get_news_without_sentiment(limit=250):
    """
    Obtiene noticias de 'gnoticias' sin sentimiento, uniendo con 'candidatos' para obtener el nombre.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT g.id_gnoticia, g.noticia, c.nombre AS candidato_nombre
                FROM gnoticias g
                JOIN candidatos c ON g.id_candidato = c.id_candidato
                WHERE g.sentimiento IS NULL
                ORDER BY g.fecha DESC
                LIMIT ?
            """, (limit,))
            return cur.fetchall()
    except Exception as e:
        print(f"❌ Error al obtener noticias sin sentimiento: {e}")
        return []

def update_news_sentiment(id_gnoticia, sentimiento, tema):
    """
    Actualiza el sentimiento y el tema de una noticia específica en la tabla `gnoticias`.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE gnoticias
                SET sentimiento = ?, tema = ?, fecha_analisis = ?
                WHERE id_gnoticia = ?
            """, (sentimiento, tema, datetime.now(), id_gnoticia))
            conn.commit()
            # print(f"✅ Noticia {id_gnoticia} actualizada con sentimiento: {sentimiento}")
    except Exception as e:
        print(f"❌ Error al actualizar noticia {id_gnoticia}: {e}")