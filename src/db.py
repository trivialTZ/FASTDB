# IMPORTANT : make sure that everything in here stays synced with the
#   database schema managed by migrations in ../db

import uuid
import collections

from contextlib import contextmanager

import numpy as np
import psycopg2
import psycopg2.extras

import util

# ======================================================================
# Global config

import apconfig
with open( apconfig.dbpasswdfile ) as ifp:
    _dbpasswd = ifp.readline().strip()
_dbhost = apconfig.dbhost
_dbport = apconfig.dbport
_dbuser = apconfig.dbuser
_dbname = apconfig.dbdatabase


# ======================================================================

@contextmanager
def DB( dbcon=None ):
    """Get a database connection in a context manager.

    Always call this as "with DB() as ..."

    Parameters
    ----------
       dbcon: psycopg2.connection or None
          If not None, just returns that.  (Doesn't check the type, so
          don't pass the wrong thing.)  Otherwise, makes a new
          connection, and then rolls back and closes that connection
          after it goes out of scope.

    Returns
    -------
       psycopg2.connection

    """

    if dbcon is not None:
        yield dbcon
        return

    try:
        global _dbuser, _dbpasswd, _dbhost, _dbport, _dbname
        conn = None
        conn = psycopg2.connect( dbname=_dbname, user=_dbuser, password=_dbpasswd, host=_dbhost, port=_dbport )
        yield conn
    finally:
        if conn is not None:
            conn.rollback()
            conn.close()


# ======================================================================
class DBBase:
    """A base class from which all other table classes derive themselves.

    All subclasses must include:

    __tablename__ = "<name of table in databse>"
    _tablemeta = None
    _pk = <list>

    _pk must be a list of strings with the names of the primary key
    columns.  Uusally (but not always) this will be a single-element
    list.

    """

    # A dictionary of postgres type to type of the object in Python
    typedict = {
        'uuid': uuid.UUID,
        'smallint': np.int16,
        'integer': np.int32,
        'bigint': np.int64,
        'text': str,
        'jsonb': dict,
        'boolean': bool,
        'real': np.float32,
        'double precision': np.float64
    }

    # A dictionary of "<colum name>": <2-element tuple>
    # The first element is the converter that converts a value into something you can throw to postgres.
    # The second element is the converter that takes what you got from postgres and turns it into what
    #   you want the object to have.
    # Often this can be left as is, but subclasses might want to override it.
    colconverters = {}

    # A dictionary of "<type">: <2-element tuple>
    # The first elment is the data type as it shows up postgres-side.
    # The second element is the same as in colconverters.
    # Usually, subclasses will not want to override this.
    typeconverters = {
        'uuid': ( str, util.asUUID ),
        'jsonb': ( psycopg2.extras.Json, None )
    }

    @property
    def tablemeta( self ):
        if self._tablemeta is None:
            self._load_table_meta()
        return self._tablemeta

    @property
    def pks( self ):
        return [ getattr( self, k ) for k in self._pk ]


    @classmethod
    def load_table_meta( cls, dbcon=None ):
        if cls._tablemeta is not None:
            return

        with DB( dbcon ) as con:
            cursor = con.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
            cursor.execute( "SELECT column_name,data_type,column_default,is_nullable "
                            "FROM information_schema.columns WHERE table_name=%(table)s",
                            { 'table': cls.__tablename__ } )
            cols = cursor.fetchall()

            cls._tablemeta = { c['column_name']: c for c in cols }


    def __init__( self, dbcon=None, cols=None, vals=None, _noinit=False, **kwargs):
        if _noinit:
            return

        self.load_table_meta( dbcon=dbcon )
        mycols = set( self._tablemeta.keys() )

        if not ( ( cols is None ) and ( vals is None ) ):
            if ( cols is None ) or ( vals is None ):
                raise ValueError( "Both or neither of cols and vals must be none." )
            if ( ( not isinstance( cols, collections.abc.Sequence ) ) or ( isinstance( cols, str ) ) or
                 ( not isinstance( vals, collections.abc.Sequence ) ) or ( isinstance( vals, str ) ) or
                 ( len( cols ) != len( vals ) )
                ):
                raise ValueError( "cols and vals most both be lists of the same length" )

        if cols is not None:
            if len(kwargs) > 0:
                raise ValueError( "Can only column values as named arguments if cols and vals are both None" )
        else:
            cols = kwargs.keys()
            vals = kwargs.values()

        keys = set( cols )
        if not keys.issubset( mycols ):
            raise RuntimeError( f"Unknown columns for {self.__tablename__}: {keys-mycols}" )

        for col in mycols:
            setattr( self, col, None )

        self._set_self_from_fetch_cols_row( cols, vals )


    def _set_self_from_fetch_cols_row( self, cols, fetchrow, dbcon=None ):
        if self._tablemeta is None:
            self.load_table_meta( dbcon=dbcon )

        for col, val in zip( cols, fetchrow ):
            dtyp = self._tablemeta[col]['data_type']
            if ( col in self.colconverters ) and ( self.colconverters[col][1] is not None ):
                setattr( self, col, self.colconverters[col][1]( val ) )
            elif ( dtyp in self.typeconverters ) and ( self.typeconverters[dtyp][1] is not None ):
                setattr( self, col, self.typeconverters[dtyp][1]( val ) )
            else:
                setattr( self, col, val )


    def _build_subdict( self ):
        """Create a substitution dictionary that could go into a cursor.execute() statement."""

        subdict = {}
        for colinfo in self.tablemeta.values():
            col = colinfo['column_name']
            typ = colinfo['data_type']
            if hasattr( self, col ):
                val = getattr( self, col )
                if val is None:
                    # What to do when val is None is not necessarily obvious.  There are a couple
                    #  of possibilities:
                    # (1) We really want to set this field to NULL in the database
                    # (2) It just hasn't been set yet in the object, so we want the
                    #     database row to keep what it has, or (in the case of an insert)
                    #     get the default value.
                    # How to know which is the case?  Assume that if the column_default is None,
                    # then we're in case (1), but if it's not None, we're in case (2).
                    if colinfo['column_default'] is None:
                        subdict[ col ] = None
                else:
                    if ( col in self.colconverters ) and ( self.colconverters[col][0] is not None ):
                        val = self.colconverters[col][0]( val )
                    elif ( typ in self.typeconverters ) and ( self.typeconverters[typ][0] is not None ):
                        val = self.typeconverters[typ][0]( val )
                    subdict[ col ] = val

        return subdict

    @classmethod
    def _construct_pk_query_where( cls, *args, me=None ):
        if cls._tablemeta is None:
            cls.load_table_meta()

        if me is not None:
            if len(args) > 0:
                raise ValueError( "Can't pass both me and arguments" )
            args = me.pks

        if len(args) != len( cls._pk ):
            raise ValueError( f"{cls.__tablename__} has a {len(cls._pk)}-element compound primary key, but "
                              f"you passed {len(args)} values" )
        q = "WHERE "
        _and = ""
        subdict = {}
        for k, v in zip( cls._pk, args ):
            if k in cls.colconverters:
                v = cls.colconverters[k][0]( v )
            elif cls._tablemeta[k]['data_type'] in cls.typeconverters:
                v = cls.typeconverters[cls._tablemeta[k]['data_type']][0]( v )
            q += f"{_and} {k}=%({k})s "
            subdict[k] = v
            _and = "AND"

        return q, subdict

    @classmethod
    def get( cls, *args, dbcon=None ):
        """Get an object from a table row with the specified primary key(s)."""

        q, subdict = cls._construct_pk_query_where( *args )
        q = f"SELECT * FROM {cls.__tablename__} {q}"
        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

        if len(rows) > 1:
            raise RuntimeError( f"Found multiple rows of {cls.__tablename__} with primary keys {args}; "
                                f"this should never happen." )
        if len(rows) == 0:
            return None

        obj = cls( cols=cols, vals=rows[0] )
        return obj

    @classmethod
    def get_batch( cls, pks, dbcon=None ):
        """Get a list of objects based on primary keys.

        Arguments
        ---------
          pks : list of lists
            Each element of the list must be a list whose length matches
            the length lf self._pk.

        Returns
        -------
          list of objects
            Each object will be an instance of the class this class
            method was called on.

        """

        if ( not isinstance( pks, collections.abc.Sequence ) ) or ( isinstance( pks, str ) ):
            raise TypeError( f"Must past a list of lists, each list having {len(cls._pk)} elwements." )

        if cls._tablemeta is None:
            cls.load_table_meta( dbcon )

        comma = ""
        mess = ""
        subdict = {}
        pktypes = [ cls._tablemeta[k]['data_type'] for k in cls._pk ]
        for dex, pk in enumerate( pks ):
            if len( pk ) != len( cls._pk ):
                raise ValueError( f"{pk} doesn't have {len(cls._pk)} elements, should match {cls._pk}" )
            mess += f"{comma}("
            subcomma=""
            for subdex, ( pkval, pkcol, pktyp ) in enumerate( zip( pk, cls._pk, pktypes ) ):
                mess += f"{subcomma}%(pk_{dex}_{subdex})s"
                if ( pkcol in cls.colconverters ) and ( cls.colconverters[pkcol][0] is not None ):
                    subdict[f'pk_{dex}_{subdex}'] = cls.colconverters[pkcol][0]( pkval )
                elif ( pktyp in cls.typeconverters ) and ( cls.typeconverters[pktyp][0] is not None ):
                    subdict[f'pk_{dex}_{subdex}'] = cls.typeconverters[pktyp][0]( pkval )
                else:
                    subdict[f'pk_{dex}_{subdex}'] = pkval
                subcomma = ","
            mess += ")"
            comma = ","
        comma = ""
        _and = ""
        collist = ""
        onlist = ""
        for subdex, ( pk, pktyp ) in enumerate( zip( cls._pk, pktypes ) ):
            collist += f"{comma}{pk}"
            # SCARY.  Specific coding for uuid.  Really I probably ought to
            #   do something with a converter dictionary to make this more
            #   general, but I know that the only case I'll need it (at leasdt
            #   as of this writing) is with uuids.
            if pktyp == 'uuid':
                onlist += f"{_and} CAST( t.{pk} AS uuid)={cls.__tablename__}.{pk}"
            else:
                onlist += f"{_and} t.{pk}={cls.__tablename__}.{pk}"
            _and = "AND"
            comma = ","

        with DB( dbcon ) as con:
            cursor = con.cursor()
            q = f"SELECT * FROM {cls.__tablename__} JOIN (VALUES {mess}) AS t({collist}) ON {onlist} "
            cursor.execute( q, subdict )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

        objs = []
        for row in rows:
            obj = cls( _noinit=True )
            obj._set_self_from_fetch_cols_row( cols, row )
            objs.append( obj )

        return objs

    @classmethod
    def getbyattrs( cls, dbcon=None, **attrs ):
        if cls._tablemeta is None:
            cls.load_table_meta( dbcon )
        types = [ cls._tablemeta[k]['data_type'] for k in attrs.keys() ]

        q = f"SELECT * FROM {cls.__tablename__} WHERE "
        _and = ""
        for k, typ in zip( attrs.keys(), types ):
            if ( k in cls.colconverters ) and ( cls.colconverters[k][0] is not None ):
                attrs[k] = cls.colconverters[k][0]( attrs[k] )
            elif ( typ in cls.typeconverters ) and ( cls.typeconverters[typ][0] is not None ):
                attrs[k] = cls.typeconverters[typ][0]( attrs[k] )
            q += f"{_and} {k}=%({k})s "
            _and = "AND"

        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, attrs )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

        objs = []
        for row in rows:
            obj = cls( _noinit=True )
            obj._set_self_from_fetch_cols_row( cols, row )
            objs.append( obj )

        return objs

    def refresh( self, dbcon=None ):
        q, subdict = self._construct_pk_query_where( *self.pks )
        q = f"SELECT * FROM {self.__tablename__} {q}"

        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

        if len(rows) > 1:
            raise ValueError( f"Found more than one row in {self.__tablename__} with primary keys {self.pks}; "
                              f"this probably shouldn't happen." )
        if len(rows) == 0:
            raise RuntimeError( f"Failed to find row in {self.__tablename__} with primary keys {self.pks}" )

        self._set_self_from_fetch_cols_row( cols, rows[0] )


    def insert( self, dbcon=None, refresh=True, nocommit=False ):
        if refresh and nocommit:
            raise RuntimeError( "Can't refresh with nocommit" )

        subdict = self._build_subdict()

        q = ( f"INSERT INTO {self.__tablename__}({','.join(subdict.keys())}) "
              f"VALUES ({','.join( [ f'%({c})s' for c in subdict.keys() ] )})" )

        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            if not nocommit:
                con.commit()
                if refresh:
                    self.refresh( con )


    def update( self, dbcon=None, refresh=False, nocommit=False ):
        if refresh and nocommit:
            raise RuntimeError( "Can't refresh with nocommit" )

        subdict = self._build_subdict()
        q = ( f"UPDATE {self.__tablename__} SET "
              f"{','.join( [ f'{c}=%({c})s' for c in subdict.keys() if c not in self._pk ] )} " )
        where, wheresubdict = self._construct_pk_query_where( me=self )
        subdict.update( wheresubdict )
        q += where

        with DB( dbcon) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            if not nocommit:
                con.commit()
                if refresh:
                    self.refresh( con )


# ======================================================================

class AuthUser( DBBase ):
    __tablename__ = "authuser"
    _tablemeta = None
    _pk = [ 'id' ]

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )


# ======================================================================

class PasswordLink( DBBase ):
    __tablename__ = "paswordlink"
    _tablemeta = None
    _pk = [ 'id' ]


# ======================================================================

class ProcessingVersion( DBBase ):
    __tablename__ = "processingversion"
    _tablemeta = None
    _pk = [ 'id' ]
