# CURRENTLY BROKEN, in the middle of being edited as fucntionality was moved to src/ltcv.py

import flask

import ltcv
from webserver.baseview import BaseView


# ======================================================================

class GetHotTransients( BaseView ):
    def do_the_things( self ):
        logger = flask.current_app.logger

        if not flask.request.is_json:
            raise TypeError( "POST data was not JSON" )
        data = flask.request.json
        kwargs = {}
        if 'procesing_version' not in data:
            kwargs['processing_version'] = 'default'
        kwargs.update( data )
        if 'return_format' in kwargs:
            return_format = kwargs['return_format']
            del kwargs['return_format']
        else:
            return_format = 0
        df = ltcv.get_hot_ltcvs( **kwargs )

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
            if include_hostinfo:
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
                    if include_hostinfo:
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
                    if include_hostinfo:
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
