from datetime import datetime
import uuid

# Importo el gestor de conexión existente
from gnoticias.db_gnoticias import get_db_connection


def log_start(proceso, mensaje=None):
    """Inserta un registro de inicio en la tabla `log_ejecucion` y retorna el id generado."""
    log_id = str(uuid.uuid4())
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO log_ejecucion (id, proceso, estado, mensaje, fecha_inicio) VALUES (?, ?, ?, ?, ?)",
                (log_id, proceso, 'running', mensaje or '', datetime.now().isoformat()),
            )
            conn.commit()
    except Exception as e:
        print(f"❌ Error al insertar log inicio: {e}")
    return log_id


def log_end(log_id, estado='finished', mensaje=None):
    """Actualiza el registro de ejecución con estado y fecha_fin."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE log_ejecucion SET estado = ?, mensaje = ?, fecha_fin = ? WHERE id = ?",
                (estado, mensaje or '', datetime.now().isoformat(), log_id),
            )
            conn.commit()
    except Exception as e:
        print(f"❌ Error al actualizar log fin: {e}")


def log_error_update(log_id, exception):
    """Marca un log existente como error y guarda el mensaje de excepción."""
    try:
        mensaje = str(exception)
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE log_ejecucion SET estado = ?, mensaje = ?, fecha_fin = ? WHERE id = ?",
                ('error', mensaje, datetime.now().isoformat(), log_id),
            )
            conn.commit()
    except Exception as e:
        print(f"❌ Error al actualizar log como error: {e}")


def log_error_new(proceso, exception):
    """Crea un nuevo registro de log con estado 'error' y el mensaje de la excepción."""
    log_id = str(uuid.uuid4())
    try:
        mensaje = str(exception)
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO log_ejecucion (id, proceso, estado, mensaje, fecha_inicio, fecha_fin) VALUES (?, ?, ?, ?, ?, ?)",
                (log_id, proceso, 'error', mensaje, datetime.now().isoformat(), datetime.now().isoformat()),
            )
            conn.commit()
    except Exception as e:
        print(f"❌ Error al insertar log error: {e}")
    return log_id