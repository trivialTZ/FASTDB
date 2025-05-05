import re
import datetime
import pytz
import flask

import psycopg
import pandas
import astropy.time

import db
from webserver.baseview import BaseView


# ======================================================================
# /spectrum/askforspectrum

class AskForSpectrum( BaseView ):
    def do_the_things( self ):
        # logger = flask.current_app.logger
        userid = flask.session['useruuid']

        data = flask.request.json
        if ( ( 'requester' not in data ) or
             ( 'objectids' not in data ) or
             ( 'priorities' not in data ) or
             ( not isinstance( data['objectids'], list ) ) or
             ( not isinstance( data['priorities'], list ) ) or
             ( len( data['objectids'] ) != len( data['priorities'] ) ) ):
            return "Mal-formed data for askforspectrum", 500

        now = datetime.datetime.now( tz=datetime.UTC )
        tocreate = [ { 'requester': data['requester'],
                       'root_diaobject_id': data['objectids'][i],
                       'wantspec_id': f"{data['objectids'][i]} ; {data['requester']}",
                       'user_id': userid,
                       'priority': ( 0 if int(data['priorities'][i]) < 0
                                     else 5 if int(data['priorities'][i]) > 5
                                     else int(data['priorities'][i] )),
                       'wanttime': now }
                       for i in range(len(data['objectids'])) ]

        n = db.WantedSpectra.bulk_insert_or_upsert( tocreate, upsert=True )

        return { 'status': 'ok',
                 'message': 'wanted spectra created',
                 'num': n }


# ======================================================================
# /spectrum/spectrawanted
#
# TODO : database hints

class WhatSpectraAreWanted( BaseView ):
    def do_the_things( self ):
        logger = flask.current_app.logger
        data = flask.request.json

        procver = data['processing_version'] if 'processing_version' in data.keys() else None

        lim_mag_band = data['lim_mag_band'] if 'lim_mag_band' in data else None
        lim_mag = float( data['lim_mag'] ) if 'lim_mag' in data else None

        if 'requested_since' in data.keys():
            match = re.search( r'^ *(?P<y>\d+)-(?P<m>\d+)-(?P<d>\d+)'
                               r'(?P<time>[ T]+(?P<H>\d+):(?P<M>\d+):(?P<S>\d+))? *$',
                               data['requested_since'] )
            if match is None:
                return f"Failed to parse YYYY-MM-DD HH:MM:SS from {data['requestedsince']}", 500

            y = int( match.group('y') )
            m = int( match.group('m') )
            d = int( match.group('d') )
            if match.group('time') is not None:
                hour = int( match.group( 'H' ) )
                minute = int( match.group( 'M' ) )
                second = int( match.group( 'S' ) )
            else:
                hour = 0
                minute = 0
                second = 0
            wantsince = datetime.datetime( y, m, d, hour, minute, second, tzinfo=datetime.UTC )
        else:
            wantsince = None

        if 'mjd_now' in data:
            # I hate almost every language's default handling of time
            # zones (or lack thereof) Python's is annoying, but at least
            # I can do what I want with some pain.  Javascript... don't
            # talk to me about Javascript.
            mjdnow = data[ 'mjd_now' ]
            now = datetime.datetime.utcfromtimestamp( astropy.time.Time( mjdnow, format='mjd', scale='tai' ).unix_tai )
            now = pytz.utc.localize( now )
        else:
            now = datetime.datetime.now( tz=datetime.UTC )
            mjdnow = astropy.time.Time( now ).mjd


        notclaimedindays = ( 7 if 'not_claimed_in_last_days' not in data.keys()
                             else int( data['not_claimed_in_last_days'] ) )
        claimsince = ( now - datetime.timedelta( days=notclaimedindays )
                       if notclaimedindays is not None else None )

        nospecindays = 7 if 'no_spectra_in_last_days' not in data.keys() else int( data['no_spectra_in_last_days'] )
        nospecsince = ( astropy.time.Time( now - datetime.timedelta( days=nospecindays ) ).mjd
                        if nospecindays is not None else None )

        if 'detected_since_mjd' in data.keys():
            detsince = None if data['detected_since_mjd'] is None else float( data['detected_since_mjd'] )
        else:
            if 'detected_in_last_days' in data.keys():
                detected_in_last_days = int( data['detected_in_last_days' ] )
            else:
                detected_in_last_days = 14
            detsince = astropy.time.Time( now - datetime.timedelta( days=detected_in_last_days ) ).mjd

        with db.DB() as con:
            cursor = con.cursor()

            # If a processing version was given, turn it into a number
            if procver is not None:
                # TODO : validity date range?
                cursor.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                                { 'procver': procver } )
                rows = cursor.fetchall()
                if len(rows) == 0:
                    cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                    { 'procver': procver } )
                    rows = cursor.fetchall()
                if len(rows) == 0:
                    return f"Error, unknown processing version {procver}", 500
                procver = rows[0][0]

            # Create a temporary table things that are wanted but that have not been claimed.
            #
            # ROB THINK : the distinct on stuff.  What should / will happen if the same
            #   requester requests the same spectrum more than once?  Maybe a unique
            #   constraint in wantedspectra?

            cursor.execute( "CREATE TEMP TABLE tmp_wanted( root_diaobject_id UUID, requester text, priority int )" )
            q = ( f"INSERT INTO tmp_wanted ( "
                  f"  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester, priority "
                  f"  FROM ( "
                  f"    SELECT w.root_diaobject_id, w.requester, w.priority, w.wanttime "
                  f"           {',r.plannedspec_id' if claimsince is not None else ''} "
                  f"    FROM wantedspectra w " )
            if claimsince is not None:
                q += ( "    LEFT JOIN plannedspectra r "
                       "      ON r.root_diaobject_id=w.root_diaobject_id AND r.created_at>%(reqtime)s "
                       "  ) subq "
                       "  WHERE plannedspec_id IS NULL "
                      )
                whereand = "AND"
            else:
                q += "  ) subq "
                whereand = "WHERE"
            q += f"  {whereand} subq.wanttime<=%(now)s "
            if wantsince is not None:
                q += "    AND subq.wanttime>=%(wanttime)s "
            q += "  GROUP BY root_diaobject_id,requester,priority )"
            subdict =  { 'wanttime': wantsince, 'reqtime': claimsince, 'now': now }
            tmpcur = psycopg.ClientCursor( con )
            logger.debug( f"Sending query: {tmpcur.mogrify(q,subdict)}" )
            cursor.execute( q, subdict )

            cursor.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted" )
            row = cursor.fetchall()
            if row[0][0] == 0:
                logger.debug( "Empty table tmp_wanted" )
                return { 'status': 'ok', 'wantedspectra': [] }
            else:
                logger.debug( f"{row[0][0]} rows in tmp_wanted" )

            # Filter that table by throwing out things that have a spectruminfo whose mjd is greater than
            #   obstime.
            if nospecsince is None:
                cursor.execute( "ALTER TABLE tmp_wanted RENAME TO tmp_wanted2" )
            else:
                cursor.execute( "CREATE TEMP TABLE tmp_wanted2( root_diaobject_id UUID, "
                                "                               requester text, priority int ) " )
                q = ( "INSERT INTO tmp_wanted2 ( "
                      "  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester, "
                      "                                                           priority "
                      "  FROM ( "
                      "    SELECT t.root_diaobject_id, t.requester, t.priority, s.specinfo_id "
                      "    FROM tmp_wanted t "
                      "    LEFT JOIN spectruminfo s "
                      "      ON s.root_diaobject_id=t.root_diaobject_id AND s.mjd>=%(obstime)s AND s.mjd<=%(now)s "
                      "  ) subq "
                      "  WHERE specinfo_id IS NULL "
                      "  GROUP BY root_diaobject_id, requester, priority )" )
                cursor.execute( q, { 'obstime': nospecsince, 'now': mjdnow } )

            cursor.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted2" )
            row = cursor.fetchall()
            if row[0][0] == 0:
                logger.debug( "Empty table tmp_wanted2" )
                return { 'status': 'ok', 'wantedspectra': [] }
            else:
                logger.debug( f"{row[0][0]} rows in tmp_wanted2" )

            # Filter that table by throwing out things that do not have a detection since detsince
            if detsince is None:
                cursor.execute( "ALTER TABLE tmp_wanted2 RENAME TO tmp_wanted3" )
            else:
                cursor.execute( "CREATE TEMP TABLE tmp_wanted3( root_diaobject_id UUID, requester text, "
                                "                               priority int ) " )
                q = ( "INSERT INTO tmp_wanted3 ( "
                      "  SELECT DISTINCT ON(t.root_diaobject_id,requester,priority) "
                      "    t.root_diaobject_id, requester, priority "
                      "  FROM tmp_wanted2 t "
                      "  INNER JOIN diaobject_root_map dorm ON t.root_diaobject_id=dorm.rootid "
                      "  INNER JOIN diasource s ON ( dorm.diaobjectid=s.diaobjectid AND "
                      "                              dorm.processing_version=s.diaobject_procver " )
                if procver is not None:
                    q += "                           AND s.processing_version=%(procver) "
                q += ( "                                                                )"
                       " WHERE s.midpointmjdtai>=%(detsince)s AND s.midpointmjdtai<=%(now)s"
                       " ORDER BY root_diaobject_id,requester,priority )" )
                cursor.execute( q, { 'detsince': detsince, 'procver': procver, 'now': mjdnow } )

            cursor.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted3" )
            row = cursor.fetchall()
            if row[0][0] == 0:
                logger.debug( "Empty table tmp_wanted3" )
                return { 'status': 'ok', 'wantedspectra': [] }
            else:
                logger.debug( f"{row[0][0]} rows in tmp_wanted3\n" )


            # Get the latest *detection* (source) for the objects
            cursor.execute( "CREATE TEMP TABLE tmp_latest_detection( root_diaobject_id UUID, "
                            "                                        mjd double precision, mag real ) " )
            q = ( "INSERT INTO tmp_latest_detection ( "
                  "  SELECT root_diaobject_id, mjd, band, mag "
                  "  FROM ( "
                  "    SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id,"
                  "           s.band AS band, s.midpointmjdtai AS mjd, "
                  "           CASE WHEN s.psfflux>0 THEN -2.5*LOG(s.psfflux)+31.4 ELSE 99 END AS mag "
                  "    FROM tmp_wanted_3 t "
                  "    INNER JOIN diaobject_root_map dorm ON t.root_diaobject_id=r.rootid "
                  "    INNER JOIN diasource s ON dorm.diaobjectid=s.diaobjectid "
                  "                           AND dorm.processing_version=s.diaobject_procver "
                  "    WHERE s.midpointmjdtai<=%(now)s " )
            if procver is not None:
                q += "    AND s.processing_version=%(procver)s "
            if lim_mag_band is not None:
                q += "    AND s.band=%(band)s "
            q += "    GROUP BY t.root_diaobjecct_id ORDER BY mjd DESC ) subq ) "
            cursor.execute( q, { 'procver': procver, 'band': lim_mag_band, 'now': mjdnow } )

            # Get the latest forced source for the objects
            cursor.execute( "CREATE TEMP TABLE tmp_latest_forced( root_diaobject_id UUID, "
                            "                                     mjd double precision, mag real ) " )
            q = ( "INSERT INTO tmp_latest_forced ( "
                  "  SELECT root_diaobject_id, mjd, band, mag "
                  "  FROM ( "
                  "    SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id,"
                  "           f.band AS band, f.midpointmjdtai AS mjd, "
                  "           CASE WHEN f.psfflux>0 THEN -2.5*LOG(f.psfflux)+31.4 ELSE 99 END AS mag "
                  "    FROM tmp_wanted3 t "
                  "    INNER JOIN diaobject_root_map dorm ON t.root_diaobject_id=r.rootid "
                  "    INNER JOIN diaforcedsource f ON dorm.diaobjectid=f.diaobjectid "
                  "                                 AND dorm.processing_version=f.diaobject_procver "
                  "    WHERE f.midpointmjdnai<=%(now)s " )
            if procver is not None:
                q += "      AND f.processing_version=%(procver)s "
            if lim_mag_band is not None:
                q += "     AND f.band=%(band)s "
            q += "    GROUP BY t.root_diaobjecct_id ORDER BY mjd DESC ) "
            cursor.execute( q, { 'procver': procver, 'band': lim_mag_band, 'now': mjdnow } )

            # Get object info.  Notice that if a processing version wasn't requested,
            #   you get a semi-random one....
            q = ( "INSERT INTO tmp_object_info ( "
                  "  SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id, t.requester, "
                  "                                           t.priority, o.diaobjectid, o.processing_version, "
                  "                                           o.ra, o.dec "
                  "  FROM tmp_wanted3 t "
                  "  INNER JOIN diaobject_root_map dorm ON dorm.rootid=t.root_diaobject_id "
                  "  INNER JOIN diaobject o ON dorm.diaobjectid=o.diaobjectid "
                  "                         AND dorm.processing_version=o.processing_version " )
            if procver is not None:
                q += "  WHERE o.processing_version=%(procver)s "
            q += ")"
            cursor.execute( q, { 'procver': procver } )

            # Join all the things and pull
            q = ( "SELECT t.root_diaobject_id, t.requester, t.priority, o.ra, o.dec, "
                  "       s.mjd AS src_mjd, s.band AS src_band, s.mag AS src_mag, "
                  "       f.mjd AS frced_mjd, s.band AS frced_band, f.mag AS frced_mag "
                  "FROM tmp_wanted3 "
                  "INNER JOIN tmp_object_info o ON t.root_diaobject_id=o.root_diaobject_id "
                  "LEFT JOIN tmp_latest_detection s ON t.root_diaobject_id=s.root_diaobject_id "
                  "LEFT JOIN tmp_latest_forced f ON t.root_diaobject_id=f.root_diaobject_id" )
            cursor.execute( q )
            columns = [ c.name for c in cursor.description ]
            df = pandas.DataFrame( cursor.fetchall(), columns=columns )

        # Filter by limiting magnitude if necessary
        if lim_mag is not None:
            df['forcednewer'] = ( ( (~df['src_mjd'].isnull()) & (~df['frced_mjd'].isnull())
                                    & (df['frced_mjd']>=df['srced_mjd'] ) )
                                  |
                                  ( (df['src_mjd'].isnull()) & (~df['forced_mjd'].isnull()) ) )
            df = df[ ( df['forcednewer'] & ( df['frced_mag'] <= lim_mag ) )
                     |
                     ( (~df['forcednewer']) & ( df['src-mag'] <= lim_mag ) ) ]


        # Build the return structure
        retarr = []
        for row in df.itertuples():
            retarr.append( { 'oid': row.root_diaobject_id,
                             'ra': float( row.ra ),
                             'dec': float( row.dec ),
                             'req': row.requester,
                             'prio': int( row.priority ),
                             'latest_source_band': row.src_band,
                             'latest_source_mjd': row.src_mjd,
                             'latest_source_mag': row.src_mag,
                             'latest_forced_band': row.forced_band,
                             'latest_forced_mjd': row.forced_mjd,
                             'latest_forced_mag': row.forced_mag } )

        return { 'status': 'ok', 'wantedspectra': retarr }




# **********************************************************************
# **********************************************************************
# **********************************************************************

bp = flask.Blueprint( 'spectrumapp', __name__, url_prefix='/spectrum' )

urls = {
    "/askforspectrum": AskForSpectrum,
    "/spectrawanted": WhatSpectraAreWanted,
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
