# TODO : wrap this with --do and --really-do options

# Empty out all data tables, don't remove users
data_tables = [ 'processing_version', 'snapshot',
                'host_galaxy', 'diaobject', 'diasource', 'diaforcedsource',
                'diasource_snapshot', 'diaforcedsource_snapshot',
                'query_queue' ]

import psycopg2
import config

with open ( config.dbpasswdfile ) as ifp:
    dbpasswd = ifp.read().strip()

conn = psycopg2.connect( host=config.dbhost, port=config.dbport, dbname=config.dbdatabase,
                         user=config.dbuser, password=dbpasswd )
try:
    cursor = conn.cursor()
    for table in data_tables:
        cursor.execute( f"TRUNCATE TABLE {table} CASCADE" )
    conn.commit()
finally:
    conn.rollback()
    conn.close()
