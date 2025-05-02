import datetime
import pytest
import numpy
import psycopg.rows

import ltcv
import db

@pytest.fixture( scope='module' )
def ask_for_spectra( procver, alerts_90days_sent_received_and_imported, fastdb_client ):
    try:
        # Get some hot lightcurves
        df, hostdf = ltcv.get_hot_ltcvs( procver.description, mjd_now=60328., source_patch=True )
        assert df.midpointmjdtai.max() < 60328.
        assert len(df.rootid.unique()) == 14
        assert len(df) == 310

        # Pick out five objects to ask for spectra

        chosenobjs = [ str(i) for i in df.rootid.unique()[ numpy.array([1, 5, 7]) ] ]

        # Ask

        res = fastdb_client.post( '/spectrum/askforspectrum',
                                  json={ 'requester': 'testing',
                                         'objectids': chosenobjs,
                                         'priorities': [3, 5, 2] } )
        yield chosenobjs, res

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM wantedspectra" )
            con.commit()


def test_ask_for_spectra( ask_for_spectra ):
    chosenobjs, res = ask_for_spectra
    assert isinstance( res, dict )
    assert res['status'] == 'ok'
    assert res['num'] == 3

    with db.DB() as con:
        cursor = con.cursor( row_factory=psycopg.rows.dict_row )
        cursor.execute( "SELECT * FROM wantedspectra" )
        rows = cursor.fetchall()

    assert set( str(r['root_diaobject_id']) for r in rows ) == set( chosenobjs )
    prios = { str(r['root_diaobject_id']) : r['priority'] for r in rows }
    assert prios[ chosenobjs[0] ] == 3
    assert prios[ chosenobjs[1] ] == 5
    assert prios[ chosenobjs[2] ] == 2

    assert all( r['requester'] == 'testing' for r in rows )
    now = datetime.datetime.now( tz=datetime.UTC )
    before = now - datetime.timedelta( minutes=10 )
    assert all( r['wanttime'] < now for r in rows )
    assert all( r['wanttime'] > before for r in rows )
