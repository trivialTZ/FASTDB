import datetime
import astropy.time

import flask

import db
from webserver.baseview import BaseView


# ======================================================================

class GetHotTransients( BaseView ):
    def do_the_things( self ):
        logger = flask.current_app.logger

        if not flask.request.is_json:
            raise TypeError( "POST data was not JSON" )
        data = flask.request.json

        mjdnow = None
        mjd0 = None
        include_object_hostinfo = False
        procver = 'default'
        procvergiven = False

        if 'processing_version' in data:
            procver = data[ 'processing_version' ].strip()
            procvergiven = True
            del data[ 'processing_version' ]

        if 'return_format' in data:
            return_format = int( data['return_format'] )
            if return_format not in ( 0, 1, 2 ):
                return "GetHotTransients: unknown return format {data['return_format']}", 500
            del data['return_format']
        else:
            return_format = 0

        if 'detected_since_mjd' in data:
            if 'detected_in_last_days' in data:
                return "Error, only give one of detected_since_mjd or detected_in_last_days", 500
            mjd0 = float( data['detected_since_mjd'] )
            del data['detected_since_mjd']
        else:
            lastdays = 30
            if 'detected_in_last_days' in data:
                lastdays = float( data['detected_in_last_days'] )
                del data['detected_in_last_days']

        if 'mjd_now' in data:
            mjdnow = float( data['mjd_now'] )
            if mjd0 is None:
                mjd0 = mjdnow - lastdays
                del data['mjd_now']
        elif mjd0 is None:
            mjd0 = astropy.time.Time( datetime.datetime.now( datetime.UTC  )
                                      - datetime.timedelta( days=lastdays ) ).mjd

        if 'include_hostinfo' in data:
            if data[ 'include_hostinfo' ]:
                include_object_hostinfo = True
            del data[ 'include_hostinfo' ]

        if len(data) != 0:
            return f"Error, unknown parameters passed in request body: {list(data.keys())}", 500

        bands = [ 'u', 'g', 'r', 'i', 'z', 'y' ]

        with db.DB() as con:
            with con.cursor() as cursor:
                # Figure out the processing version
                cursor.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                                { 'procver': procver } )
                rows = cursor.fetchall()
                if len(rows) == 0:
                    cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                    { 'procver': procver } )
                    rows = cursor.fetchall()
                if len(rows) == 0:
                    if procvergiven:
                        return f"Could not find processing version '{procver}'", 500
                    else:
                        return "No processing version, and could not find processing version 'default'", 500
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
                      "SELECT DISTINCT ON(dorm.id) root_diaobject_id "
                      "INTO TEMP TABLE tmp_objids "
                      "FROM diaobject_root_map dorm.id "
                      "INNER JOIN diasource s ON (s.diaobjectid=dorm.diabojectid AND "
                      "                           s.processing_version=dorm.processing_version )"
                      "WHERE s.processing_version=%(procver)s AND s.midpointmjdtai>=%(t0)s" )
                if mjdnow is not None:
                    q += "  AND midpointmjdtai<=%(t1)s"
                cursor.execute( q, { 'procver': procver, 't0': mjd0, 't1': mjdnow } )


                q = ( "/* IndexScan(f diaobject_id)\n"
                      "   IndexScan(o)\n"
                      "*/\n"
                      "SELECT r.root_diaobject_id AS root_diaobject_id, o.ra AS ra, o.dec AS dec,"
                     )
                if include_object_hostinfo:
                    for bandi in range( len(bands)-1 ):
                        q += ( f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]},"
                               f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]}_err," )
                    q += "h.petroflux_r,h.petroflux_r_err,o.nearbyextobj1sep,h.pzmean,h.pzstd,"
                q += ( "     f.diaforcedsourceid,f.visit,f.detector,f.midpointmjdtai,f.band,"
                       "     f.psfflux,f.psffluxerr"
                       "FROM diaforcedsource f "
                       "INNER JOIN diaobject o ON (f.diaobjectid=o.diaobjectid AND "
                       "                           f.diaobject_procver=o.processing_version )"
                       "INNER JOIN diaobject_root_map r ON (o.diaobjectid=r.diaobject_id AND "
                       "                                    o.processing_version=r.processing_version) " )
                if include_object_hostinfo:
                    q += "INNER JOIN host_galaxy h ON o.nearbyextobj1id=h.id "
                q += ( "WHERE r.rootid IN (SELECT root_diaobject_id FROM tmp_objids) "
                       "  AND f.processing_verison=%(procver)s" )
                if mjdnow is not None:
                    q += "  AND f.midpointmjdtai<=%(t1)s "
                q += "ORDER BY r.root_diaobject_id,f.midpointmjdtai"

                cursor.execute( q, { "procver": procver, "t0": mjd0, "t1": mjdnow } )
                columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]

        if ( return_format == 0 ) or ( return_format == 1 ):
            sne = []
        elif ( return_format == 2 ):
            sne = { 'objectid': [],
                    'ra': [],
                    'dec': [],
                    'mjd': [],
                    'band': [],
                    'flux': [],
                    'fluxerr': [],
                    'zp': [],
                    'redshift': [],
                    'sncode': [] }
            if include_object_hostinfo:
                sne[ 'hostgal_petroflux_r' ] = []
                sne[ 'hostgal_petroflux_r_err' ] = []
                sne[ 'hostgal_snsep' ] = []
                sne[ 'hostgal_pzmean' ] = []
                sne[ 'hostgal_pzstd' ] = []
                for bandi in range( len(bands)-1 ):
                    sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] = []
                    sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ] = []
        else:
            raise RuntimeError( "This should never happen." )

        # ZEROPOINT
        #
        # https://sdm-schemas.lsst.io/apdb.html claims that all fluxes are in nJy.
        # Wikipedia tells me that mAB = -2.5log_10(f_ν) + 8.90
        #   with f_ν in Jy, or, better stated, since arguments of logs should not have units:
        #     mAB = -2.5 log_10( f_ν / 1 Jy ) + 8.90
        # Converting units:
        #    mAB = -2.5 log_10( f_ν / 1 Jy * ( 1 Jy / 10⁹ nJy ) ) + 8.90
        #        = -2.5 log_10( f_ν / nJy * 10⁻⁹ ) +  8.90
        #        = -2.5 ( log_10( f_ν / nJy ) - 9 ) + 8.90
        #        = -2.5 log_10( f_ν / nJy ) + 31.4

        if len(df) > 0:
            objids = df['root_diaobject_id'].unique()
            logger.info( f"GetHotSNEView: got {len(objids)} objects" )
            df.set_index( [ 'root_diaobject_id', 'diaforcedsource_id' ], inplace=True )

            for objid in objids:
                subdf = df.xs( objid, level='root_diaobject_id' )
                if ( return_format == 0 ) or ( return_format == 1 ):
                    toadd = { 'objectid': str(objid),
                              'ra': subdf.ra.values[0],
                              'dec': subdf.dec.values[0],
                              'zp': 31.4,
                              'redshift': -99,
                              'sncode': -99 }
                    if include_object_hostinfo:
                        toadd[ 'hostgal_petroflux_r' ] = subdf.hostgal_petroflux_r.values[0]
                        toadd[ 'hostgal_petroflux_r_err' ] = subdf.hostgal_petroflux_r_err.values[0]
                        toadd[ 'hostgal_snsep' ] = subdf.nearbyextobj1sep.values[0]
                        toadd[ 'hostgal_pzmean' ] = subdf.pzmean.values[0]
                        toadd[ 'hostgal_pzstd' ] = subdf.pzstd.values[0]
                        for bandi in range( len(bands)-1 ):
                            toadd[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] = (
                                subdf[ f'stdcolor_{bands[bandi]}_{bands[bandi+1]}' ].values[0] )
                            toadd[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] = (
                                subdf[ f'stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ].values[0] )

                    if return_format == 0:
                        toadd['photometry'] = { 'mjd': list( subdf['midpointmjdtai'] ),
                                                'band': list( subdf['band'] ),
                                                'flux': list( subdf['psfflux'] ),
                                                'fluxerr': list( subdf['psffluxerr'] ) }
                    else:
                        toadd['mjd'] = list( subdf['mjd'] ),
                        toadd['band'] = list( subdf['band'] )
                        toadd['flux'] = list( subdf['flux'] )
                        toadd['fluxerr'] = list( subdf['fluxerr'] )
                    sne.append( toadd )
                elif return_format == 2:
                    sne['objectid'].append( str(objid) )
                    sne['ra'].append( subdf.ra.values[0] )
                    sne['dec'].append( subdf.dec.values[0] )
                    sne['mjd'].append( list( subdf['midpointmjdtai'] ) )
                    sne['band'].append( list( subdf['band'] ) )
                    sne['flux'].append( list( subdf['psfflux'] ) )
                    sne['fluxerr'].append( list( subdf['psffluxerr'] ) )
                    sne['zp'].append( 31.4 )
                    sne['redshift'].append( -99 )
                    sne['sncode'].append( -99 )
                    if include_object_hostinfo:
                        sne[ 'hostgal_petroflux_r' ].append( subdf['hostgal_petroflux_r'].values[0] )
                        sne[ 'hostgal_petroflux_r_err'] .append( subdf['hostgal_petroflux_r_err'].values[0] )
                        sne[ 'hostgal_snsep' ].append( subdf['nearbyexstobj1sep'].values[0] )
                        sne[ 'hostgal_pzmean' ].append( subdf['pzmean'].values[0] )
                        sne[ 'hostgal_pzstd' ].append( subdf['pzstd'].values[0] )
                        for bandin in range( len(bands) ):
                            sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}' ].append(
                                subdf[f'stdcolor_{bands[bandi]}_{bands[bandi+1]}'].values[0] )
                            sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ].append(
                                subdf[f'stdcolor_{bands[bandi]}_{bands[bandi+1]}_err'].values[0] )
                else:
                    raise RuntimeError( "This should never happen." )


        # logger.info( "GetHotTransients; returning" )
        return { 'status': 'ok',
                 'diaobject': sne }
        return resp



# **********************************************************************
# **********************************************************************
# **********************************************************************

bp = flask.Blueprint( 'ltcvapp', __name__, url_prefix='/ltcv' )

urls = {
    "gethottransients": GetHotTransients
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
