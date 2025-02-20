import sys
import io
import pandas

sys.path.insert( 0, '/code/client' )
from fastdb_client import FASTDBClient


def test_short_query( obj1, src1, src1_pv2, test_user ):
    fastdb = FASTDBClient( 'http://webap:8080', username='test', password='test_password' )

    res = fastdb.submit_short_sql_query( "SELECT * FROM diasource" )
    assert len(res) == 2
    assert set( [ r['diasourceid'] for r in res ] ) == { src1.diasourceid, src1_pv2.diasourceid }
    assert set( [ r['processing_version'] for r in res ] ) == { src1.processing_version, src1_pv2.processing_version }


def test_synchronous_long_query( obj1, src1, src1_pv2, test_user ):
    fastdb = FASTDBClient( 'http://webap:8080', username='test', password='test_password' )

    res = fastdb.synchronous_long_sql_query( "SELECT * FROM diasource", checkeach=1, maxwait=20 )
    strio = io.StringIO( res )
    df = pandas.read_csv( strio, sep=',', header=0 )
    assert len(df) == 2
    assert all( df.diasourceid.values == [42,42] )
    assert set( df.processing_version.values ) == { 23, 42 }
