import datetime

import astropy.time
import pandas

import db


def get_hot_ltcvs( processing_version, detected_since_mjd=None, detected_in_last_days=None,
                   mjd_now=None, include_hostinfo=False ):

    mjd0 = None

    if detected_since_mjd is not None:
        if detected_in_last_days is not None:
            raise ValueError( "Only specify at most one of detected_since_mjd and detected_in_last_days" )
        mjd0 = float( detected_since_mjd )
    else:
        lastdays = 30
        if detected_in_last_days is not None:
            lastdays = float( detected_in_last_days )

    if mjd_now is not None:
        mjd_now = float( mjd_now )
        if mjd0 is None:
            mjd0 = mjd_now - lastdays
    elif mjd0 is None:
        mjd0 = astropy.time.Time( datetime.datetime.now( tz=datetime.UTC )
                                  - datetime.timedelta( days=lastdays ) ).mjd

    bands = [ 'u', 'g', 'r', 'i', 'z', 'y' ]

    with db.DB() as con:
        with con.cursor() as cursor:
            # Figure out the processing version
            cursor.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                            { 'procver': processing_version } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                { 'procver': processing_version } )
                rows = cursor.fetchall()
            if len(rows) == 0:
                raise ValueError( f"Could not find processing version '{processing_version}'" )
            procver = rows[0][0]

            # Try to wrangle postgres into doing this more
            #  efficiently than it would by default.  I'm not sure
            #  I've done it all right here (in particular, the
            #  NoBitmapScan), but I do think that postgres makes bad
            #  choices about indexes into the forced source tables.
            #  When looking at the forced source table, we really
            #  want it to filter on object id *first*, before trying
            #  to do any mjd or other filters, because that's what's
            #  going to cut down the number of things to think about
            #  the most (by up to a factor of 10⁶).  Postgres
            #  sometimes seems to think that another index scan
            #  first is better.  Perhaps we could try to play games
            #  with postgres' table statistics collection to avoid
            #  misinforming Postgres' query optimizer, but it's
            #  easier just to use the hinting extension.

            q = ( "/*+ NoBitmapScan(elasticc2_diasource)\n"
                  "*/\n"
                  "SELECT DISTINCT ON(dorm.rootid) rootid "
                  "INTO TEMP TABLE tmp_objids "
                  "FROM diaobject_root_map dorm "
                  "INNER JOIN diasource s ON (s.diaobjectid=dorm.diaobjectid AND "
                  "                           s.processing_version=dorm.processing_version )"
                  "WHERE s.processing_version=%(procver)s AND s.midpointmjdtai>=%(t0)s" )
            if mjd_now is not None:
                q += "  AND midpointmjdtai<=%(t1)s"
            cursor.execute( q, { 'procver': procver, 't0': mjd0, 't1': mjd_now } )

            q = ( "/* IndexScan(f diaobjectid)\n"
                  "   IndexScan(o)\n"
                  "*/\n"
                  "SELECT r.rootid AS rootid, o.ra AS ra, o.dec AS dec,"
                 )
            if include_hostinfo:
                for bandi in range( len(bands)-1 ):
                    q += ( f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]},"
                           f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]}_err," )
                q += "h.petroflux_r,h.petroflux_r_err,o.nearbyextobj1sep,h.pzmean,h.pzstd,"
            q += ( "     f.diaforcedsourceid,f.visit,f.detector,f.midpointmjdtai,f.band,"
                   "     f.psfflux,f.psffluxerr "
                   "FROM diaforcedsource f "
                   "INNER JOIN diaobject o ON (f.diaobjectid=o.diaobjectid AND "
                   "                           f.diaobject_procver=o.processing_version )"
                   "INNER JOIN diaobject_root_map r ON (o.diaobjectid=r.diaobjectid AND "
                   "                                    o.processing_version=r.processing_version) " )
            if include_hostinfo:
                q += "INNER JOIN host_galaxy h ON o.nearbyextobj1id=h.id "
            q += ( "WHERE r.rootid IN (SELECT rootid FROM tmp_objids) "
                   "  AND f.processing_version=%(procver)s" )
            if mjd_now is not None:
                q += "  AND f.midpointmjdtai<=%(t1)s "
            q += "ORDER BY r.rootid,f.midpointmjdtai"

            cursor.execute( q, { "procver": procver, "t0": mjd0, "t1": mjd_now } )
            columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]
            df = pandas.DataFrame( cursor.fetchall(), columns=columns )

    return df
