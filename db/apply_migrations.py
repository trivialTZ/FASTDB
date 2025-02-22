import sys
import io
import argparse
import pathlib
import subprocess

import psycopg2


def main():
    parser = argparse.ArgumentParser( 'apply_migrations', description='Apply .sql migration files' )
    parser.add_argument( '-H', '--host', default='postgres', help="Postgres host" )
    parser.add_argument( '-P', '--port', type=int, default=5432, help="Postgres port" )
    parser.add_argument( '-d', '--db', default="fastdb", help="Postgres database" )
    parser.add_argument( '-u', '--user', default="postgres", help="Postgres user" )
    parser.add_argument( '-p', '--password', default="fragile", help="Postgres password" )
    args = parser.parse_args()

    direc = pathlib.Path( __file__ ).parent
    sqlfiles = list( direc.glob( "*.sql" ) )
    sqlfiles.sort()

    with psycopg2.connect( host=args.host, port=args.port, dbname=args.db,
                           user=args.user, password=args.password ) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute( "SELECT filename,applied_time FROM migrations_applied ORDER BY filename" )
            rows = cursor.fetchall()
            applied = [ row[0] for row in rows ]
            when = [ row[1] for row in rows ]
        except psycopg2.errors.UndefinedTable:
            applied = []
            when = []

        strio = io.StringIO()
        strio.write( "Previously applied:\n" )
        for a, w in zip( applied, when ):
            strio.write( f"   {a:32s}  ({w})\n" )
        sys.stderr.write( strio.getvalue() )

        for i, a in enumerate( applied ):
            if sqlfiles[i].name != a:
                raise ValueError( f"Mismatch between applied and files at file {sqlfiles[i].name}, "
                                  f"applied logged {a}" )

        for i in range( len(applied), len(sqlfiles) ):
            sys.stderr.write( f"Applying {sqlfiles[i]}...\n" )
            rval = subprocess.run( [ "psql", "-h", args.host, "-p", str(args.port), "-U", args.user,
                                     "-v", "ON_ERROR_STOP=on",
                                     "-f", sqlfiles[i],
                                     "--single-transaction", "-b",
                                     args.db ],
                                   env={ 'PGPASSWORD': args.password },
                                   capture_output=True )
            if rval.returncode != 0:
                sys.stderr.write( f"Error processing {sqlfiles[i]}:\n{rval.stderr.decode('utf-8')}\n" )
                raise RuntimeError( "SQL error" )


# ======================================================================
if __name__ == "__main__":
    main()
