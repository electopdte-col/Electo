import sqlite3
from datetime import datetime

DB_PATH = "data/gnoticias.db"  # Ruta a la base de datos SQLite


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

def save_news_to_gnoticias(news_data):
    """Inserta una nueva noticia en la tabla `gnoticias`."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO gnoticias (
                    id_candidato, id_gnoticia, noticia, medio, fecha, source_href,
                    link, ano, mes, dia, hora, minuto, dia_sem, dia_ano, id_original,
                    id_log
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                news_data.get("id_log")
            ))
            conn.commit()
            print(f"✅ (gnoticias) Guardada: {news_data['noticia']}")
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