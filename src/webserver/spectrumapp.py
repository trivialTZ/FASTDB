import datetime
import flask

from db import WantedSpectra
from webserver.baseview import BaseView


# ======================================================================
# /spectrum/askforspectrum

class AskForSpectrum( BaseView ):
    def do_the_things( self ):
        logger = flask.current_app.logger
        userid = flask.session['useruuid']

        data = flask.request.json
        if ( ( 'requester' not in data ) or
             ( 'objectids' not in data ) or
             ( 'priorities' not in data ) or
             ( not isinstance( data['objectids'], list ) ) or
             ( not isinstance( data['priorities'], list ) ) or
             ( len( data['objectids'] ) != len( data['priorities'] ) ) ):
            return "Mal-formed data for askforspectrum", 500

        now = datetime.datetime.now( tz=datetime.timezone.utc )
        tocreate = [ { 'requester': data['requester'],
                       'root_diaobject_id': data['objectids'][i],
                       'wantspec_id': f"{data['objectids'][i]} ; {data['requester']}",
                       'user_id': userid,
                       'priority': ( 0 if int(data['priorities'][i]) < 0
                                     else 5 if int(data['priorities'][i]) > 5
                                     else int(data['priorities'][i] )),
                       'wanttime': now }
                       for i in range(len(data['objectids'])) ]

        n = WantedSpectra.bulk_insert_or_upsert( tocreate, upsert=True )

        return { 'status': 'ok',
                 'message': f'wanted spectra created',
                 'num': n }
        

# ======================================================================
# /spectrum/spectrawanted

class WhatSpectraAreWanted( BaseView ):
    def do_the_things( self ):
        logger = flask.current_app.logger
        data = flask.request.json

        procver = data['processing_version'] if 'processing_version' in data.keys() else None
        
        if 'lim_mag' in data.keys():
            limmag = float( data['lim_mag'] )
            if 'lim_mag_band' in data.keys():
                lim_mag_band = data['lim_mag_band']
        else:
            limmag = None

        if 'requested_since' in data.keys():
            match = re.search( '^ *(?P<y>\d+)-(?P<m>\d+)-(?P<d>\d+)'
                               '(?P<time>[ T]+(?P<H>\d+):(?P<M>\d+):(?P<S>\d+))? *$',
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
            wantsince = datetime.datetime( y, m, d, hour, minute, second, tzinfo=datetime.timezone.utc )
        else:
            wantsince = datetime.datetime.now( tz=datetime.timezone.utc ) - datetime.timedelta( days=14 )

        if 'not_claimed_in_last_days' in data.keys():
            notclaimedinlastdays = int( data['not_claimed_in_last_days'] )
        else:
            notclaimedinlastdays = 7
        claimsince = datetime.datetime.now() - datetime.timedelta( days=notclaimedinlastdays )

        if 'detected_since_mjd' in data.keys():
            detsince = float( data['detected_since_mjd'] )
        else:
            if 'detected_in_last_days' in data.keys():
                detected_in_last_days = int( data['detected_in_last_days' ] )
            else:
                detected_in_last_days = 14
            detsince = astropy.time.Time( datetime.datetime.now()
                                          - datetime.timedelta( days=detected_in_last_days ) ).mjd

        if 'no_spectra_in_last_days' in data.keys():
            no_spectra_in_last_days = int( data['not_observed_in_last_days'] )
        else:
            no_spectra_in_last_days = 7
        nospecsince = astropy.time.Time( datetime.datetime.now()
                                         - datetime.timedelta( days=no_spectra_in_last_days ) ).mjd

        with db.DB() as con:
            cursor = conn.cursor()

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
            
            # Create a temporary table things that are wanted but that have not been claimed
            #   (Note that if claimsince is 0, then the left join will never return anything
            #   and all wanted spectra will be returned.)
            cursor.execute( "CREATE TEMP TABLE tmp_wanted( root_diaobject_id UUID, requester text, priority int )" )
            q = ( "INSERT INTO tmp_wanted ( "
                  "  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester, priority "
                  "  FROM ( "
                  "    SELECT w.root_diaobject_id, w.requester, w.priority, r.plannedspec_id "
                  "    FROM wantedspectra w "
                  "    LEFT JOIN plannedspectra r "
                  "      ON r.root_diaobject_id=w.root_diaobject_id AND r.created_at>%(reqtime)s "
                  "    WHERE w.wanttime>=%(wanttime)s "
                  "  ) subq "
                  "  WHERE plannedspec_id IS NULL "
                  "  GROUP BY root_diaobject_id,requester,priority )" )
            # sys.stderr.write( f"Sending query: {cursor.mogrify(q,{'wanttime':wantsince,'reqtime':claimsince})}\n" )
            cursor.execute( q, { 'wanttime': wantsince, 'reqtime': claimsince } )

            cursor.execute( "SELECT COUNT(diaobject_id) FROM tmp_wanted" )
            row = cursor.fetchall()
            if row[0]['count'] == 0:
                # sys.stderr.write( "Empty table tmp_wanted\n" )
                return { 'status': 'ok', 'wantedspectra': [] }
            # else:
            #     sys.stderr.write( f"{row[0]['count']} rows in tmp_wanted\n" )

            # Filter that table by throwing out things that have a spectruminfo whose mjd is greater than
            #   obstime.  (Again, setting obstime to now or the future will mean nothing gets thrown out.)
            cursor.execute( "CREATE TEMP TABLE tmp_wanted2( root_diaobject_id bigint, requester text, priority int ) " )
            q = ( "INSERT INTO tmp_wanted2 ( "
                  "  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester, priority "
                  "  FROM ( "
                  "    SELECT t.root_diaobject_id, t.requester, t.priority, s.specinfo_id "
                  "    FROM tmp_wanted t "
                  "    LEFT JOIN spectruminfo s "
                  "      ON s.root_diaobject_id=t.root_diaobject_id AND s.mjd>=%(obstime)s "
                  "  ) subq "
                  "  WHERE specinfo_id IS NULL "
                  "  GROUP BY diaobject_id, requester, priority )" )
            cursor.execute( q, { 'obstime': nospecsince } )

            cursor.execute( "SELECT COUNT(diaobject_id) FROM tmp_wanted2" )
            row = cursor.fetchall()
            if row[0]['count'] == 0:
                # sys.stderr.write( "Empty table tmp_wanted2\n" )
                return { 'status': 'ok', 'wantedspectra': [] }
            # else:
            #     sys.stderr.write( f"{row[0]['count']} rows in tmp_wanted2\n" )

            # Filter that table by throwing out things that do not have a detection since detsince
            cursor.execute( "CREATE TEMP TABLE tmp_wanted3( root_diaobject_id bigint, requester text, priority int ) " )
            q = ( "INSERT INTO tmp_wanted3 ( "
                  "  SELECT DISTINCT ON(t.root_diaobject_id,requester,priority) "
                  "    t.root_diaobject_id, requester, priority "
                  "  FROM tmp_wanted2 t "
                  "  INNER JOIN diaobject_root_map dorm ON t.root_diaobject_id=dorm.rootid "
                  "  INNER JOIN diasource s ON ( dorm.diaobject_id=s.diaobject_id AND "
                  "                              dorm.processing_version=s.diaobject_procver " )
            if proc verr is not None:
                q += "                           AND s.processing_version=%(procver) "
            q += ( "                                                                )"
                   " WHERE s.midpointtai>%(detsince)s "
                   " ORDER BY root_diaobject_id,requester,priority )" )
            cursor.execute( q, { 'detsince': detsince, 'procver': procver } )

            cursor.execute( "SELECT COUNT(diaobject_id) FROM tmp_wanted3" )
            row = cursor.fetchall()
            if row[0]['count'] == 0:
                # sys.stderr.write( "Empty table tmp_wanted3\n" )
                return { 'status': 'ok', 'wantedspectra': [] }
            # else:
            #     sys.stderr.write( f"{row[0]['count']} rows in tmp_wanted3\n" )

            cursor.execute( "CREATE TEMP TABLE tmp_wanted4( diaobject_id bigint, mjd real, "
                            "                               filtername text, mag real )" )

            # Filter for things whose latest detection goes above a minimum magnitude
            # The zeropoint of 31.4 is for nJy, which is what table fluxes are defined to be in
            cursor.execute( "INSERT INTO tmp_wanted4 ( "
                            "  SELECT diaobject_id,mjd,filtername,mag "
                            "  FROM ( "

             ROB YOU ARE HERE, THIS QUERY NEEDS A LOT OF WORK

                            "    SELECT DISTINCT ON(f.diaobject_id,f.filtername) "
                            "        f.diaobject_id,f.filtername,f.midpointtai as mjd,-2.5*LOG(f.psflux)+27 AS mag"
                            "    FROM tmp_wanted3 t "
                            "    INNER JOIN elasticc2_diaforcedsource f "
                            "      ON t.diaobject_id=f.diaobject_id "
                            "    WHERE f.psflux > 0 AND f.psflux > 3.*f.psfluxerr "
                            "    ORDER BY f.diaobject_id,f.filtername "
                            "  ) subq "
                            "  ORDER BY mjd DESC )" )

            cursor.execute( "SELECT COUNT(diaobject_id) FROM tmp_wanted4" )
            row = cursor.fetchall()
            if row[0]['count'] == 0:
                # sys.stderr.write( "empty table tmp_wanted4\n" )
                return JsonResponse( { 'status': 'ok', 'wantedspectra': [] } )

            cursor.execute( "SELECT w3.diaobject_id AS objid, w3.requester, w3.priority, "
                            "       w4.filtername, w4.mjd, w4.mag, o.ra, o.decl AS dec "
                            "FROM tmp_wanted3 w3 "
                            "INNER JOIN tmp_wanted4 w4 "
                            "  ON w3.diaobject_id=w4.diaobject_id "
                            "INNER JOIN elasticc2_diaobject o "
                            "  ON w3.diaobject_id=o.diaobject_id "
                            "ORDER BY w3.priority DESC,w3.diaobject_id" )
            df = pandas.DataFrame( cursor.fetchall() ).set_index( 'objid', 'filtername' )

        pgconn.rollback()

        if limmag is not None:
            subdf = df.xs( lim_mag_band, level='filtername' )
            subdf = subdf[ subdf.mag < limmag ].reset_index()
            df[ df.index.get_level_values('objid').isin( list( subdf.objid ) ) ]

        tmpretvals = {}
        retval = []
        for row in df.reset_index().itertuples():
            objid = int( row.objid )
            if objid not in tmpretvals.keys():
                tmpretvals[objid] = { 'oid': objid,
                                      'ra': float( row.ra ),
                                      'dec': float( row.dec ),
                                      'req': row.requester,
                                      'prio': int( row.priority ),
                                      'latest': {} }

            tmpretvals[ objid ]['latest'][row.filtername] = { 'mjd': float( row.mjd ),
                                                              'mag': float( row.mag ) }
        retval = [ v for v in tmpretvals.values() ]
        return JsonResponse( { 'status': 'ok',
                               'wantedspectra': retval } )

    except Exception as ex:
        sys.stderr.write( "Exception in WhatSpectraAreWanted" )
        traceback.print_exc( file=sys.stderr )
        return HttpResponse( f"Exception in WhatSpectraAreWanted: {ex}",
                             status=500, content_type='text/plain; charset=utf-8' )

        
    

# **********************************************************************
# **********************************************************************
# **********************************************************************

bp = flask.Blueprint( 'spectrumapp', __name__, url_prefix='/spectrum' )

urls = {
    "/askforspectrum": AskForSpectrum
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


