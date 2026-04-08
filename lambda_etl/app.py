import json
import boto3
import mysql.connector
import csv
import tempfile
import os
from datetime import datetime

s3 = boto3.client('s3')


def parse_date(value):
    if not value or value.strip() == "":
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p")
    except Exception as e:
        print(f"⚠️ Error parseando fecha: {value} -> {e}")
        return None


def lambda_handler(event, context):
    try:
        # 1. EVENTO S3
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"📥 Archivo recibido: {key}")

        # 2. DESCARGAR ARCHIVO
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        s3.download_file(bucket, key, tmp_file.name)
        print("✅ Archivo descargado correctamente")

        # 3. CONEXIÓN RDS + CREACIÓN DB/TABLA
        conn = mysql.connector.connect(
            host=os.environ.get('DB_HOST'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            port=3306
        )
        cursor = conn.cursor()

        db_name = os.environ.get('DB_NAME')
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.execute(f"USE {db_name}")
        print(f"📦 Usando base de datos: {db_name}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY,
                store_id INT,
                order_amount DECIMAL(10, 2),
                order_moment_created DATETIME
            )
        """)
        print("📊 Tabla 'orders' verificada/creada")

        # 4. LEER CSV + TRANSFORMACIÓN
        with open(tmp_file.name, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            print("🧾 Columnas detectadas:", reader.fieldnames)

            data_to_insert = []
            total_rows = 0
            error_rows = 0

            for row in reader:
                total_rows += 1

                # Solo imprime las primeras 5 filas (evita saturar logs)
                if total_rows <= 5:
                    print("➡️ Fila leída:", row)

                try:
                    val = (
                        int(row['order_id']),
                        int(row['store_id']),
                        float(row['order_amount']) if row['order_amount'] else None,
                        parse_date(row['order_moment_created'])
                    )
                    data_to_insert.append(val)

                except Exception as e:
                    error_rows += 1
                    print("❌ Error en fila:", row)
                    print("Detalle error:", str(e))

            print(f"📊 Total filas leídas: {total_rows}")
            print(f"⚠️ Filas con error: {error_rows}")
            print(f"✅ Filas válidas: {len(data_to_insert)}")

        # 5. INSERT MASIVO
        rows_inserted = 0

        if data_to_insert:
            query = """
                INSERT INTO orders (order_id, store_id, order_amount, order_moment_created)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    store_id = VALUES(store_id),
                    order_amount = VALUES(order_amount),
                    order_moment_created = VALUES(order_moment_created)
            """

            cursor.executemany(query, data_to_insert)
            rows_inserted = len(data_to_insert)

        conn.commit()
        print(f"🚀 Filas insertadas/actualizadas: {rows_inserted}")

        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "ETL completado",
                "rows_processed": rows_inserted,
                "total_rows": total_rows,
                "error_rows": error_rows
            })
        }

    except Exception as e:
        print("💥 ERROR GENERAL:", str(e))
        raise e