# TODO : wrap this with --do and --really-do options

all_tables = [ 'authuser', 'passwordlink',
               'processing_version', 'snapshot',
               'host_galaxy', 'diaobject', 'diasource', 'diaforcedsource',
               'diasource_snapshot', 'diaforcedsource_snapshot',
               'query_queue', 'migrations_applied' ]

import psycopg2
import config

with open ( config.dbpasswdfile ) as ifp:
    dbpasswd = ifp.read().strip()

conn = psycopg2.connect( host=config.dbhost, port=config.dbport, dbname=config.dbdatabase,
                         user=config.dbuser, password=dbpasswd )
try:
    cursor = conn.cursor()
    for table in all_tables:
        cursor.execute( f"DROP TABLE IF EXISTS {table} CASCADE" )
    conn.commit()
finally:
    conn.rollback()
    conn.close()
