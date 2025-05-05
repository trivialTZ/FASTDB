import datetime
import pytz
import uuid
import numpy
import psycopg.rows

import astropy.time

import ltcv
import db


def test_ask_for_spectra( procver, alerts_90days_sent_received_and_imported, fastdb_client ):
    try:
        # Get some hot lightcurves
        df, _hostdf = ltcv.get_hot_ltcvs( procver.description, mjd_now=60328., source_patch=True )
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
        assert isinstance( res, dict )
        assert res['status'] == 'ok'

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
    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM wantedspectra" )
            con.commit()


def test_get_wanted_spectra( procver, alerts_90days_sent_received_and_imported, fastdb_client, test_user ):
    # Prime the database with some wanted spectra
    # Some objects of interest:
    #    1696949 — 5 detections, 5 forced
    #                  last forced r = 60359.35 (21.48), last forced = 60359.36 (i, 21.49)
    #                  last source r = 60359.35 (21.48), last source = 60362.33 (z, 21.36)
    #    1981540 — 30 detections, 38 forced
    #                  last forced r = 60352.13 (23.38), last forced = 60355.11 (g, 24.63)
    #                  last source r = 60352.13 (23.38), last source = 60360.09 (z, 21.59)
    #     191776 — 12 detections, 37 forced
    #                  last forced r = 60345.20 (22.31), last forced = 60345.25 (g, 23.36)
    #                  last source r = 60353.24 (22.75), last source = 60353.26 (i, 22.25)
    #    1747042 —  8 detections, 12 forced
    #                  last forced r = 60322.34 (22.35), last forced = 60341.35 (Y, 22.21)
    #                  last source r = 60322.34 (22.35), last source = 60343.31 (i, 23.04)
    #    1173200 — 13 detections, 29 forced
    #                  last forced r = 60322.20 (23.83), last forced = 60326.10 (Y, 22.78)
    #                  last source r = 60316.10 (23.57), last source = 60327.20 (i, 23.44)

    # The latest detection at all to make it into daisource is from
    #  MJD 60362.33 = 2024-02-22T07:55:12Z

    mjdnow = 60326.5
    now = datetime.datetime.utcfromtimestamp( astropy.time.Time( mjdnow, format='mjd', scale='tai' ).unix_tai )
    now = pytz.utc.localize( now )
    try:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT rootid,diaobjectid FROM diaobject_root_map "
                            "WHERE diaobjectid=ANY(%(obj)s) AND processing_version=%(procver)s",
                            { 'obj': [ 1696949, 1981540, 191776, 1747042, 1173200 ],
                              'procver': procver.id } )
            idmap = { r[1]: r[0] for r in cursor.fetchall() }
            assert len(idmap) == 5

            # requester1 has asked for all five
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1696949],
                              't': now,
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 3 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1981540],
                              't': now - datetime.timedelta( days=1 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 4 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[191776],
                              't': now - datetime.timedelta( days=5 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 2 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1747042],
                              't': now - datetime.timedelta( days=10 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 1 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1173200],
                              't': now - datetime.timedelta( days=40 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 5 } )
            # requester2 very recently asked for a spectrum of a source that requester1 asked for a long time ago
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1173200],
                              't': now - datetime.timedelta( days=2 ),
                              'uid': test_user.id,
                              'req': 'requester2',
                              'prio': 5 } )

            # Put in a couple of spectrum claims
            cursor.execute( "INSERT INTO plannedspectra(plannedspec_id,root_diaobject_id,facility,created_at,plantime) "
                            "VALUES (%(pid)s,%(rid)s,%(fac)s,%(ct)s,%(pt)s)",
                            { 'pid': uuid.uuid4(),
                              'rid': idmap[1747042],
                              'fac': 'test facility',
                              'ct': now - datetime.timedelta( days=9 ),
                              'pt': now - datetime.timedelta( days=8 )
                             } )
            cursor.execute( "INSERT INTO plannedspectra(plannedspec_id,root_diaobject_id,facility,created_at,plantime) "
                            "VALUES (%(pid)s,%(rid)s,%(fac)s,%(ct)s,%(pt)s)",
                            { 'pid': uuid.uuid4(),
                              'rid': idmap[1696949],
                              'fac': 'test facility',
                              'ct': now,
                              'pt': now + datetime.timedelta( days=1 )
                             } )
            cursor.execute( "INSERT INTO plannedspectra(plannedspec_id,root_diaobject_id,facility,created_at,plantime) "
                            "VALUES (%(pid)s,%(rid)s,%(fac)s,%(ct)s,%(pt)s)",
                            { 'pid': uuid.uuid4(),
                              'rid': idmap[191776],
                              'fac': 'test facility',
                              'ct': now - datetime.timedelta( days=4 ),
                              'pt': now - datetime.timedelta( days=3 )
                             } )

            # One of the planned spectra was observed
            cursor.execute( "INSERT INTO spectruminfo(specinfo_id,root_diaobject_id,facility,inserted_at,"
                            "                         mjd,z,classid) "
                            "VALUES (%(sid)s,%(rid)s,%(fac)s,%(t)s,%(mjd)s,%(z)s,%(class)s)",
                            { 'sid': uuid.uuid4(),
                              'rid': idmap[191776],
                              'fac': 'test facility',
                              't': now - datetime.timedelta( days=1 ),
                              'mjd': mjdnow - 2,
                              'z': 0.25,
                              'class': 2222 } )

            con.commit()


        # Test 1 : If we pass nothing (except for mjd_now, which we need
        #   for the test), we should get all spectra ever requested that
        #   have not been claimed in the last 7 days, that have no
        #   observed spectra in the last 7 days, and that have been detected
        #   in the last 7 days.  That should throw out 1696949 and 191776
        #   (both requested in the last 7 days), as well as 1747042 and
        #   1173200, leaving only 1981540

        res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow } )
        import pdb; pdb.set_trace()
        pass

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM spectruminfo" )
            cursor.execute( "DELETE FROM plannedspectra" )
            cursor.execute( "DELETE FROM wantedspectra" )
            con.commit()
