import flask
import psycopg2

from server import BaseView
import db


# ======================================================================]
# Utility function used by multiple views

def _extract_queries( data ):
    if 'query' not in data:
        raise ValueError( "POST data must include 'query'" )

    if isinstance( data['query'], list ):
        queries = data['query']
    elif not isinstance( data['query'], str ):
        raise TypeError( "query must be either a list of strings or a string" )
    else:
        queries = [ data['query'] ]
    if not all( [ isinstance(q, str) for q in queries  ] ):
        raise TypeError( "queries must all be strings" )

    if 'subdict' not in data:
        subdicts = [ {} for i in range(len(queries)) ]
    else:
        if isinstance( data['subdict'], list ):
            subdicts = data['subdict']
        elif not isinstance( data['subdict'], dict ):
            raise TypeError( "subdict must be either a list of dicts or a dict" )
        else:
            subdicts = [ data['subdict'] ]
        if len(subdicts) != len(queries):
            raise ValueError( "number of queries and subdicts must match" )
        if not all( [ isinstance(s, dict) for s in subdicts ] ):
            raise TypeError( "subdicts must all be dicts" )

    # Have to convert lists to tuples in the substitution dictionaries
    for subdict in subdicts:
        for key in subdict.keys():
            if isinstance( subdict[key], list ):
                subdict[key] = tuple( subdict[key] )

    # Look for a return format
    return_format = 0
    if 'return_format' in data:
        return_format = data['return_format']

    return queries, subdicts, return_format


# ======================================================================
# Interface for short SQL queries that return results directly.

class RunSQLQuery( BaseView ):
    def do_the_things( *args, **kwargs ):
        logger = flask.current_app.logger

        if not flask.request.is_json:
            raise TypeError( "POST data was not JSON" )
        data= flask.request.json

        try:
            # TODO : make these configurable?
            dbuser = "postgres_ro"
            pwfile = "/secrets/postgres_ro_password"
            with open( pwfile ) as ifp:
                password = ifp.readline().strip()

            queries, subdicts, return_format = _extract_queries( data )

            conn = psycopg2.connect( dbname=db.dbname, host=db.dbhost, port=db.dbport, user=dbuser, password=password )
            try:
                cursor = conn.cursor()

                logger.debug( "Starting query sequence" )
                logger.debug( f"queries={queries}" )
                logger.debug( f"subdicts={subdicts}" )

                for query, subdict in zip( queries, subdicts ):
                    logger.debug( f'Query is {query}, subdict is {subdict}, dbuser is {dbuser}' )
                    cursor.execute( query, subdict )
                    logger.debug( 'Query done' )

                logger.debug( "Fetching" )
                columns = [ c.name for c in cursor.description ]
                rows = cursor.fetchall()

            finally:
                conn.rollback()
                conn.close()

            if return_format == 0:
                retval = { 'status': 'ok',
                           'rows': [ { c: r[i] for i, c in enumerate(columns) } for r in rows ]
                          }
            elif return_format == 1:
                retval = { 'status': 'ok',
                           'data': { c: [ r[i] for r in rows ] for i, c in enumerate(columns) }
                          }
            else:
                raise ValueError( f"Unknown return format {return_format}" )

            logger.debug( f"Returning {len(rows)} rows from query sequence." )
            return retval

        except Exception as ex:
            logger.exception( ex )
            return { 'status': 'error', 'error': str(ex) }





# ======================================================================
# ======================================================================
# ======================================================================

bp = flask.Blueprint( 'dbapp', __name__, url_prefix='/db' )

urls = {
    "runsqlquery": RunSQLQuery,
}

usedurls = {}
for url, cls in urls.items():
    if url not in usedurls.keys():
        usedurls[ url ] = 0
        name = url
    else:
        usedurls[ url ] += 1
        name = f'{url}.{usedurls[url]}'

    bp.add_url_rule (url, view_func=cls.as_view(name), methods=['POST'], strict_slashes=False )
