# Script to sync tables between databases. Reads all the rows from
# the table in the source database and all the rows from the table
# in the destination database and attempts to optimistically
# insert/update only the differences in the destination table,
# as well as deleting any extra rows from the destination table.
# If this optimistic approach fails (e.g. rows can't be updated
# because of constraint violations:
# https://nocd.hashnode.dev/updates-order-and-the-binlog-1 ), then
# the script will delete all the rows from the destination table
# and then attempt to insert all the rows from the source table.
#
# Note: Because this script pulls all the rows in a table on the source
#       and destination database, it should be used with care and not
#       used on very large tables.
#
# Note: This script currently assumes that the tables have the same
#       schema (e.g. columns, primary keys, constraints, etc.). It
#       does not currently
#
# Sample invocation:
#    python3 syncer_script.py \
#        --src mysql://root:$MYSQL_SRC_PWD@127.0.0.1:3306/myDatabase \
#        --dst mysql://root:$MYSQL_DST_PWD@127.0.0.1:3306/myDatabase
#
# TODO:
#   - add table option:      --table src_db.src_tbl:dst_db.dst_tbl:pk_name
#   - add no-dry-run option: --no-dry-run
#   - add verbose option:    --verbose

import logging
import re

import pymysql
import pymysql.cursors
import typer

app = typer.Typer()
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

        insert_query = f"INSERT INTO {table_name} SET "
        insert_args = []

        update_query = "ON DUPLICATE KEY UPDATE "
        update_args = []

        for key, value in row.items():
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
            f"""DELETE FROM {table_name} WHERE id = %s LIMIT 1""",
            (
                row["id"],
            ),
        )


def parse_connection_string(url: str):
    mysql_connection_url_regex = 'mysql://(.*?):(.*?)@(.*?):(.*?)/(.*)'
    if re.search(mysql_connection_url_regex, url):
        user, password, host, port, database = re.match(mysql_connection_url_regex, url).groups()
        return user, password, host, port, database
    else:
        raise typer.BadParameter('Invalid MySQL connection URL')


@app.command(no_args_is_help=True)
def main(
        src: str = typer.Option("", help="MySQL connection string for the source "
                                         "database containing the table to read from"),
        dst: str = typer.Option("", help="MySQL connection string for the destination "
                                         "database containing the table to update"),
):
    src_user, src_password, src_host, src_port, src_database = parse_connection_string(src)
    dst_user, dst_password, dst_host, dst_port, dst_database = parse_connection_string(dst)
    tables = ["email_templates_v2"]

    src = pymysql.connect(
        host=src_host,
        port=int(src_port),
        user=src_user,
        password=src_password,
        db=src_database,
        cursorclass=pymysql.cursors.DictCursor,
    )
    dst = pymysql.connect(
        host=dst_host,
        port=int(dst_port),
        user=dst_user,
        password=dst_password,
        db=dst_database,
        cursorclass=pymysql.cursors.DictCursor,
    )

    with src.cursor() as src_curs, dst.cursor() as dst_curs:
        for table_name in tables:
            try:
                dst_curs.execute("START TRANSACTION;")
                log.info(f"Starting sync of {table_name} from {src_host} to {dst_host}")
                sync(table_name=table_name, dst_curs=dst_curs, src_curs=src_curs)
                dst.commit()
                log.info(f"Successful sync of {table_name} from {src_host} to {dst_host}")
            except Exception as ex:
                log.exception(f"Error in sync of {table_name} from {src_host} to {dst_host}: {ex}")
                dst.rollback()
                try:
                    log.info(f"Deleting all data and retrying sync of {table_name} from {src_host} to {dst_host}")
                    dst_curs.execute("START TRANSACTION;")
                    dst_curs.execute(f"""DELETE FROM {table_name}""")
                    sync(table_name=table_name, dst_curs=dst_curs, src_curs=src_curs)
                    dst.commit()
                    log.info(f"Successful sync of {table_name} from {src_host} to {dst_host}")
                except Exception as ex2:
                    log.exception(f"Error in replacing all data in {table_name} from {src_host} to {dst_host}: {ex2}")
                    dst.rollback()


if __name__ == "__main__":
    app()
