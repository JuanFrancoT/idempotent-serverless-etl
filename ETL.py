import os
import time
import logging
import mysql.connector
from pathlib import Path
from dotenv import load_dotenv

# ==============================
# CARGAR VARIABLES DE ENTORNO
# ==============================
load_dotenv()

# ==============================
# CONFIGURACIÓN
# ==============================
DB_CFG = dict(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    allow_local_infile=True,
)

CSV_PATH = Path(os.getenv("CSV_PATH")).resolve()
TABLE_NAME = os.getenv("TABLE_NAME", "orders")

# ==============================
# LOGGING
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

# ==============================
# VALIDAR VARIABLES DE ENTORNO
# ==============================
def validate_env():
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "CSV_PATH"]
    
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Falta variable de entorno: {var}")

# ==============================
# VALIDAR CSV
# ==============================
def validate_csv(path: Path):
    logger.info("Validando archivo CSV...")
    
    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {path}")
    
    size_mb = path.stat().st_size / (1024 * 1024)
    logger.info(f"Archivo encontrado ({size_mb:.2f} MB)")

# ==============================
# CONEXIÓN CON RETRY
# ==============================
def connect_with_retry():
    logger.info("Conectando a MySQL...")

    for attempt in range(10):
        try:
            cnx = mysql.connector.connect(
                **DB_CFG,
                autocommit=True
            )
            logger.info("Conexión exitosa a MySQL")
            return cnx
        except Exception as e:
            logger.warning(f"Intento {attempt+1}/10 fallido: {e}")
            time.sleep(5)

    raise Exception("No se pudo conectar a MySQL después de varios intentos")

# ==============================
# CREAR TABLA
# ==============================
def create_tables(cursor):
    logger.info("Creando tablas...")

    sql = f"""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY,
        store_id INT,
        channel_id INT,
        payment_order_id INT,
        delivery_order_id INT,
        order_status VARCHAR(50),
        order_amount FLOAT,
        order_delivery_fee FLOAT,
        order_delivery_cost FLOAT,
        order_created_hour INT,
        order_created_minute INT,
        order_created_day INT,
        order_created_month INT,
        order_created_year INT,
        order_moment_created DATETIME,
        order_moment_accepted DATETIME,
        order_moment_ready DATETIME,
        order_moment_collected DATETIME,
        order_moment_in_expedition DATETIME,
        order_moment_delivering DATETIME,
        order_moment_delivered DATETIME,
        order_moment_finished DATETIME,
        order_metric_collected_time FLOAT,
        order_metric_paused_time FLOAT,
        order_metric_production_time FLOAT,
        order_metric_walking_time FLOAT,
        order_metric_expediton_speed_time FLOAT,
        order_metric_transit_time FLOAT,
        order_metric_cycle_time FLOAT
    );

    DROP TABLE IF EXISTS staging_orders;

    CREATE TABLE staging_orders (
        order_id VARCHAR(50),
        store_id VARCHAR(50),
        channel_id VARCHAR(50),
        payment_order_id VARCHAR(50),
        delivery_order_id VARCHAR(50),
        order_status VARCHAR(50),
        order_amount VARCHAR(50),
        order_delivery_fee VARCHAR(50),
        order_delivery_cost VARCHAR(50),
        order_created_hour VARCHAR(50),
        order_created_minute VARCHAR(50),
        order_created_day VARCHAR(50),
        order_created_month VARCHAR(50),
        order_created_year VARCHAR(50),
        order_moment_created VARCHAR(50),
        order_moment_accepted VARCHAR(50),
        order_moment_ready VARCHAR(50),
        order_moment_collected VARCHAR(50),
        order_moment_in_expedition VARCHAR(50),
        order_moment_delivering VARCHAR(50),
        order_moment_delivered VARCHAR(50),
        order_moment_finished VARCHAR(50),
        order_metric_collected_time VARCHAR(50),
        order_metric_paused_time VARCHAR(50),
        order_metric_production_time VARCHAR(50),
        order_metric_walking_time VARCHAR(50),
        order_metric_expediton_speed_time VARCHAR(50),
        order_metric_transit_time VARCHAR(50),
        order_metric_cycle_time VARCHAR(50)
    );
    """

    for stmt in sql.strip().split(";"):
        if stmt.strip():
            cursor.execute(stmt)

# ==============================
# CARGA MASIVA
# ==============================
def load_data(cursor, path: Path):
    logger.info("Iniciando carga masiva (LOAD DATA)...")

    load_sql = f"""
    LOAD DATA LOCAL INFILE '{path.as_posix()}'
    INTO TABLE {TABLE_NAME}
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS;
    """

    start = time.perf_counter()
    cursor.execute(load_sql)
    end = time.perf_counter()

    rows = cursor.rowcount
    duration = end - start

    logger.info(f"Filas cargadas: {rows:,}")
    logger.info(f"Tiempo: {duration:.2f}s")
    logger.info(f"Velocidad: {rows/duration:,.0f} filas/s")

    return rows

# ==============================
# Carga CSV a Staging
# ==============================
def load_to_staging(cursor, path: Path):
    logger.info("Cargando datos en staging...")

    load_sql = f"""
    LOAD DATA LOCAL INFILE '{path.as_posix()}'
    INTO TABLE staging_orders
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS
    (
        order_id,
        store_id,
        channel_id,
        payment_order_id,
        delivery_order_id,
        order_status,
        order_amount,
        order_delivery_fee,
        order_delivery_cost,
        order_created_hour,
        order_created_minute,
        order_created_day,
        order_created_month,
        order_created_year,
        @order_moment_created,
        @order_moment_accepted,
        @order_moment_ready,
        @order_moment_collected,
        @order_moment_in_expedition,
        @order_moment_delivering,
        @order_moment_delivered,
        @order_moment_finished,
        order_metric_collected_time,
        order_metric_paused_time,
        order_metric_production_time,
        order_metric_walking_time,
        order_metric_expediton_speed_time,
        order_metric_transit_time,
        order_metric_cycle_time
    )
    SET
        order_moment_created = NULLIF(@order_moment_created, '0000-00-00 00:00:00'),
        order_moment_accepted = NULLIF(@order_moment_accepted, '0000-00-00 00:00:00'),
        order_moment_ready = NULLIF(@order_moment_ready, '0000-00-00 00:00:00'),
        order_moment_collected = NULLIF(@order_moment_collected, '0000-00-00 00:00:00'),
        order_moment_in_expedition = NULLIF(@order_moment_in_expedition, '0000-00-00 00:00:00'),
        order_moment_delivering = NULLIF(@order_moment_delivering, '0000-00-00 00:00:00'),
        order_moment_delivered = NULLIF(@order_moment_delivered, '0000-00-00 00:00:00'),
        order_moment_finished = NULLIF(@order_moment_finished, '0000-00-00 00:00:00');
    """

    cursor.execute(load_sql)

# ==============================
# UPSERT
# ==============================

def upsert_data(cursor):
    logger.info("Insertando/actualizando datos (UPSERT)...")

    upsert_sql = """
    INSERT INTO orders (
        order_id,
        store_id,
        channel_id,
        payment_order_id,
        delivery_order_id,
        order_status,
        order_amount,
        order_delivery_fee,
        order_delivery_cost,
        order_created_hour,
        order_created_minute,
        order_created_day,
        order_created_month,
        order_created_year,
        order_moment_created,
        order_moment_accepted,
        order_moment_ready,
        order_moment_collected,
        order_moment_in_expedition,
        order_moment_delivering,
        order_moment_delivered,
        order_moment_finished,
        order_metric_collected_time,
        order_metric_paused_time,
        order_metric_production_time,
        order_metric_walking_time,
        order_metric_expediton_speed_time,
        order_metric_transit_time,
        order_metric_cycle_time
    )
    SELECT
        order_id,
        store_id,
        channel_id,
        payment_order_id,
        delivery_order_id,
        order_status,

        --LIMPIEZA NUMÉRICOS
        CASE WHEN TRIM(order_amount) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_amount ELSE NULL END,
        CASE WHEN TRIM(order_delivery_fee) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_delivery_fee ELSE NULL END,
        CASE WHEN TRIM(order_delivery_cost) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_delivery_cost ELSE NULL END,

        order_created_hour,
        order_created_minute,
        order_created_day,
        order_created_month,
        order_created_year,

        --LIMPIEZA FECHAS
        STR_TO_DATE(NULLIF(order_moment_created, ''), '%m/%d/%Y %h:%i:%s %p'),
        STR_TO_DATE(NULLIF(order_moment_accepted, ''), '%m/%d/%Y %h:%i:%s %p'),
        STR_TO_DATE(NULLIF(order_moment_ready, ''), '%m/%d/%Y %h:%i:%s %p'),
        STR_TO_DATE(NULLIF(order_moment_collected, ''), '%m/%d/%Y %h:%i:%s %p'),
        STR_TO_DATE(NULLIF(order_moment_in_expedition, ''), '%m/%d/%Y %h:%i:%s %p'),
        STR_TO_DATE(NULLIF(order_moment_delivering, ''), '%m/%d/%Y %h:%i:%s %p'),
        STR_TO_DATE(NULLIF(order_moment_delivered, ''), '%m/%d/%Y %h:%i:%s %p'),
        STR_TO_DATE(NULLIF(order_moment_finished, ''), '%m/%d/%Y %h:%i:%s %p'),

        -- LIMPIEZA MÉTRICAS
        CASE WHEN TRIM(order_metric_collected_time) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_metric_collected_time ELSE NULL END,
        CASE WHEN TRIM(order_metric_paused_time) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_metric_paused_time ELSE NULL END,
        CASE WHEN TRIM(order_metric_production_time) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_metric_production_time ELSE NULL END,
        CASE WHEN TRIM(order_metric_walking_time) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_metric_walking_time ELSE NULL END,
        CASE WHEN TRIM(order_metric_expediton_speed_time) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_metric_expediton_speed_time ELSE NULL END,
        CASE WHEN TRIM(order_metric_transit_time) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_metric_transit_time ELSE NULL END,
        CASE WHEN TRIM(order_metric_cycle_time) REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$' THEN order_metric_cycle_time ELSE NULL END

    FROM staging_orders

    ON DUPLICATE KEY UPDATE
        store_id = VALUES(store_id),
        channel_id = VALUES(channel_id),
        order_status = VALUES(order_status),
        order_amount = VALUES(order_amount),
        order_delivery_fee = VALUES(order_delivery_fee),
        order_delivery_cost = VALUES(order_delivery_cost);
    """

    cursor.execute(upsert_sql)

    affected_rows = cursor.rowcount
    logger.info(f"Filas afectadas (insert/update): {affected_rows}")

    return affected_rows
# ==============================
# Limpiar Staging
# ==============================

def clear_staging(cursor):
    logger.info("Limpiando staging...")
    cursor.execute("TRUNCATE TABLE staging_orders")


# ==============================
# VALIDACIÓN POST-CARGA
# ==============================
def validate_load(cursor, expected_rows=None):
    logger.info("Validando datos en BD...")

    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = cursor.fetchone()[0]

    logger.info(f"Filas en tabla: {count:,}")

    if expected_rows is not None:
        if count != expected_rows:
            logger.warning("Diferencia entre filas cargadas y almacenadas")
        else:
            logger.info("Validación exitosa")
    


# ==============================
# QUALITY CHECK
# ==============================

def run_data_quality_checks(cursor):
    logger.info("Ejecutando validaciones de calidad de datos...")

    checks = [
        {
            "name": "order_id nulos",
            "query": f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE order_id IS NULL",
            "expected": 0
        },
        {
            "name": "valores negativos en order_amount",
            "query": f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE order_amount < 0",
            "expected": 0
        },
        {
            "name": "fechas inválidas",
            "query": f"""
                SELECT COUNT(*) FROM {TABLE_NAME}
                WHERE order_moment_delivered < order_moment_created
            """,
            "expected": 0
        }
    ]

    for check in checks:
        cursor.execute(check["query"])
        result = cursor.fetchone()[0]

        if result != check["expected"]:
            logger.warning(f"Falló check: {check['name']} → {result} registros")
        else:
            logger.info(f"OK: {check['name']}")

# ==============================
# MAIN PIPELINE
# ==============================
def run_pipeline():
    logger.info("=== INICIANDO ETL ===")

    try:
        # 1. VALIDAR ENTORNO
        validate_env()

        # 2. VALIDAR CSV
        validate_csv(CSV_PATH)

        # 3. CONEXIÓN
        cnx = connect_with_retry()
        cursor = cnx.cursor()

        # 4. CREAR TABLAS
        create_tables(cursor)

        # 5. CARGAR A STAGING
        load_to_staging(cursor, CSV_PATH)

        # 6. UPSERT
        upsert_data(cursor)

        # 7. VALIDAR
        validate_load(cursor)

        # 8. DATA QUALITY
        run_data_quality_checks(cursor)

        # 9. LIMPIAR STAGING
        clear_staging(cursor)

        # 10. CIERRE
        cursor.close()
        cnx.close()

        logger.info("ETL completado exitosamente")

    except Exception as e:
        logger.error(f"Error en pipeline: {e}", exc_info=True)

# ==============================
# EJECUCIÓN
# ==============================
if __name__ == "__main__":
    run_pipeline()