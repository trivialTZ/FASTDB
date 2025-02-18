import logging
import uuid
from types import SimpleNamespace

from contextlib import contextmanager

import simplejson
import psycopg2

import flask
import flask_session
import flask.views

import rkauth_flask

# ======================================================================
# Global config

import apconfig
with open( apconfig.secretkeyfile ) as ifp:
    _flask_session_secret_key = ifp.readline().strip()
with open( apconfig.dbpasswdfile ) as ifp:
    _dbpasswd = ifp.readline().strip()
_dbhost = apconfig.dbhost
_dbport = apconfig.dbport
_dbuser = apconfig.dbuser
_dbname = apconfig.dbdatabase


# ======================================================================
# UUID encoder for simplejson

class UUIDJSONEncoder( simplejson.JSONEncoder ):
    def default( self, obj ):
        if isinstance( obj, uuid.UUID ):
            return str(obj)
        else:
            return super().default( obj )



# ======================================================================

class BaseView( flask.views.View ):
    _admin_required = False

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

    @contextmanager
    def dbcon( self, conn=None ):
        global _dbhost, _dbport, _dbuser, _dbpasswd, _dbname

        if conn is not None:
            yield conn
            return
        else:
            conn = psycopg2.connect( host=_dbhost, port=_dbport, dbname=_dbname, user=_dbuser, password=_dbpasswd )
            yield conn
            conn.rollback()
            conn.close()


    def check_auth( self ):
        self.username = flask.session['username'] if 'username' in flask.session else '(None)'
        self.displayname = flask.session['userdisplayname'] if 'userdisplayname' in flask.session else '(None)'
        self.authenticated = ( 'authenticated' in flask.session ) and flask.session['authenticated']
        self.user = None
        if self.authenticated:
            with self.dbcon() as conn:
                cursor = conn.cursor()
                cursor.execute( "SELECT id,username,displayname,email FROM authuser WHERE username=%(username)s",
                                {'username': self.username } )
                rows = cursor.fetchall()
                if len(rows) > 1:
                    self.authenticated = False
                    raise RuntimeError( f"Error, more than one {self.username} in database, "
                                        f"this should never happen." )
                if len(rows) == 0:
                    self.authenticated = False
                    raise ValueError( f"Error, failed to find user {self.username} in database" )
                row = rows[0]
                self.user = SimpleNamespace( id=row[0], username=row[1], displayname=row[2], email=row[3] )
                # Verify that session displayname and database displayname match?  Eh.  Whatevs.
        return self.authenticated

    def dispatch_request( self, *args, **kwargs ):
        if not self.check_auth():
            return "Not logged in", 500
        if ( self._admin_required ) and ( not self.user.isadmin ):
            return "Action requires admin", 500
        try:
            retval = self.do_the_things( *args, **kwargs )
            # Can't just use the default JSON handling, because it
            #   writes out NaN which is not standard JSON and which
            #   the javascript JSON parser chokes on.  Sigh.
            if isinstance( retval, dict ) or isinstance( retval, list ):
                return ( simplejson.dumps( retval, ignore_nan=True, cls=UUIDJSONEncoder ),
                         200, { 'Content-Type': 'application/json' } )
            elif isinstance( retval, str ):
                return retval, 200, { 'Content-Type': 'text/plain; charset=utf-8' }
            elif isinstance( retval, tuple ):
                return retval
            else:
                return retval, 200, { 'Content-Type': 'application/octet-stream' }
        except Exception as ex:
            # sio = io.StringIO()
            # traceback.print_exc( file=sio )
            # app.logger.debug( sio.getvalue() )
            app.logger.exception( str(ex) )
            return str(ex), 500


# ======================================================================

class MainPage( BaseView ):
    def dispatch_request( self ):
        return flask.render_template( "fastdb_webap.html" )


# **********************************************************************
# **********************************************************************
# **********************************************************************
# Configure and create the web app in global variable "app"


app = flask.Flask(  __name__ )
# app.logger.setLevel( logging.INFO )
app.logger.setLevel( logging.DEBUG )

app.config.from_mapping(
    SECRET_KEY=_flask_session_secret_key,
    SESSION_COOKIE_PATH='/',
    SESSION_TYPE='filesystem',
    SESSION_PERMANENT=True,
    SESSION_USE_SIGNER=True,
    SESSION_FILE_DIR=apconfig.sessionstore,
    SESSION_FILE_THRESHOLD=1000,
)

server_session = flask_session.Session( app )

rkauth_flask.RKAuthConfig.setdbparams(
    db_host=_dbhost,
    db_port=_dbport,
    db_name=_dbname,
    db_user=_dbuser,
    db_password=_dbpasswd,
    email_from = apconfig.emailfrom,
    email_subject = 'fastdb password reset',
    email_system_name = 'fastdb',
    smtp_server = apconfig.smtpserver,
    smtp_port = apconfig.smtpport,
    smtp_use_ssl = apconfig.smtpusessl,
    smtp_username = apconfig.smtpusername,
    smtp_password = apconfig.smtppassword
)

app.register_blueprint( rkauth_flask.bp )

urls = {
    "/": MainPage,
}

usedurls = {}
for url, cls in urls.items():
    if url not in usedurls.keys():
        usedurls[ url ] = 0
        name = url
    else:
        usedurls[ url ] += 1
        name = f'{url}.{usedurls[url]}'

    app.add_url_rule (url, view_func=cls.as_view(name), methods=['GET', 'POST'], strict_slashes=False )
