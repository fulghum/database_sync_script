import logging
import os

import pymysql
import pymysql.cursors

logging.basicConfig(
    format="%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


def sync(table_name, dst_curs, src_curs):
    src_curs.execute(f"""SELECT * FROM {table_name}""")
    src_result = src_curs.fetchall()

    # Update/Insert row from src-db into dst
    dst_curs.execute(f"""SELECT * FROM {table_name}""")
    dst_result = dst_curs.fetchall()

    diff = [row for row in src_result if row not in dst_result]
    for row in diff:
        log.info(f"Row id: {row['id']} missing from dst")

        insert_query = "INSERT INTO %s SET "
        insert_args = [table_name]

        update_query = "ON DUPLICATE KEY UPDATE "
        update_args = []

        for key, value in row.items():
            if value:
                insert_query += f""" {key} = '%s' ,"""
                update_query += f""" {key} = '%s' ,"""
            else:
                insert_query += f""" {key} = %s ,"""
                update_query += f""" {key} = %s ,"""

            insert_args.append(value)
            update_args.append(value)

        insert_query = insert_query.strip(",")
        update_query = update_query.strip(",")

        query = insert_query + update_query
        args = insert_args + update_args

        dst_curs.execute(query, tuple(args))

    # Delete row from dst that is not in src-db
    dst_curs.execute(f"""SELECT * FROM {table_name}""")
    dst_result = dst_curs.fetchall()

    diff = [row for row in dst_result if row not in src_result]
    for row in diff:
        log.info(f"Delete row {row['id']} from dst since missing in src")
        dst_curs.execute(
            """DELETE FROM %s WHERE id = %s LIMIT 1""",
            (
                table_name,
                row["id"],
            ),
        )


def main():
    tables = ["email_templates_v2"]
    src_host = os.environ.get("SHARED_DB_HOST", "")
    dst_host = os.environ.get("MYSQL_HOST", "127.0.0.1")

    src = pymysql.connect(
        host=src_host,
        port=int(os.environ.get("SHARED_DB_HOST_PORT", 3306)),
        user=os.environ.get("SHARED_DB_USER", "admin"),
        password=os.environ.get("SHARED_DB_PASSWORD", ""),
        db="nocd_v2",
        cursorclass=pymysql.cursors.DictCursor,
    )
    dst = pymysql.connect(
        host=dst_host,
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        db=os.environ.get("MYSQL_DB_NAME", "../nocd_v2"),
        cursorclass=pymysql.cursors.DictCursor,
    )

    with src.cursor() as src_curs, dst.cursor() as dst_curs:
        for table_name in tables:
            dst_curs.execute("START TRANSACTION;")

            try:
                log.info(f"Started sync {table_name} from {src_host} to {dst_host}")
                sync(table_name=table_name, dst_curs=dst_curs, src_curs=src_curs)
                dst.commit()
                log.info(f"Succesful sync {table_name} from {src_host} to {dst_host}")
            except Exception as ex:
                log.exception(f"Error in sync {table_name} from {src_host} to {dst_host}: {ex}")
                dst.rollback()


if __name__ == "__main__":
    main()