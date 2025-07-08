

def test_gethottransients( test_user, fastdb_client, procver, alerts_90days_sent_received_and_imported ):
    # This tests gets the same information as ../test_ltcv.py, only via the webap.

    # TODO : look at some of the actual returned values to make sure they're right.
    # (Query the database and compare?)

    # ****************************************
    # return format 0
    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328. } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 88


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328.,
                                     'source_patch': True } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 91
    assert sum( sum( res[i]['photometry']['is_source'] ) for i in range( len(res) ) ) == 3


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 91
    assert sum( sum( res[i]['photometry']['is_source'] ) for i in range( len(res) ) ) == 3


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'mjd_now': 60328.,
                                     'source_patch': True } )
    assert isinstance( res, list )
    assert len( res ) == 14
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 310
    assert sum( sum( res[i]['photometry']['is_source'] ) for i in range( len(res) ) ) == 12


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'include_hostinfo': True } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry',
                                     'hostgal_petroflux_r', 'hostgal_petroflux_r_err',
                                     'hostgal_stdcolor_u_g', 'hostgal_stdcolor_g_r', 'hostgal_stdcolor_r_i',
                                     'hostgal_stdcolor_i_z', 'hostgal_stdcolor_z_y', 'hostgal_stdcolor_u_g_err',
                                     'hostgal_stdcolor_g_r_err', 'hostgal_stdcolor_r_i_err',
                                     'hostgal_stdcolor_i_z_err', 'hostgal_stdcolor_z_y_err',
                                     'hostgal_snsep', 'hostgal_pzmean', 'hostgal_pzstd' }


    # ****************************************
    # return format 1
    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'return_format': 1 } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode',
                                     'mjd', 'band', 'flux', 'fluxerr', 'is_source' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i][pkey] ) for i in range( len(res) ) ) == 91
    assert sum( sum( res[i]['is_source'] ) for i in range( len(res) ) ) == 3

    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'include_hostinfo': True,
                                     'return_format': 1 } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode',
                                     'mjd', 'band', 'flux', 'fluxerr', 'is_source',
                                     'hostgal_petroflux_r', 'hostgal_petroflux_r_err',
                                     'hostgal_stdcolor_u_g', 'hostgal_stdcolor_g_r', 'hostgal_stdcolor_r_i',
                                     'hostgal_stdcolor_i_z', 'hostgal_stdcolor_z_y', 'hostgal_stdcolor_u_g_err',
                                     'hostgal_stdcolor_g_r_err', 'hostgal_stdcolor_r_i_err',
                                     'hostgal_stdcolor_i_z_err', 'hostgal_stdcolor_z_y_err',
                                     'hostgal_snsep', 'hostgal_pzmean', 'hostgal_pzstd' }


    # ****************************************
    # return format 2
    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'return_format': 2 } )
    assert isinstance( res, dict )
    assert set( res.keys() ) == { 'objectid', 'ra','dec', 'zp', 'redshift', 'sncode',
                                  'mjd', 'band', 'flux', 'fluxerr', 'is_source' }
    assert all( len(v) == 4 for v in res.values() )
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[pkey][i] ) for i in range(4) ) == 91
    assert sum( sum( res['is_source'][i] ) for i in range(4) ) == 3

    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'include_hostinfo': True,
                                     'return_format': 2 } )
    assert set( res.keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode',
                                  'mjd', 'band', 'flux', 'fluxerr', 'is_source',
                                  'hostgal_petroflux_r', 'hostgal_petroflux_r_err',
                                  'hostgal_stdcolor_u_g', 'hostgal_stdcolor_g_r', 'hostgal_stdcolor_r_i',
                                  'hostgal_stdcolor_i_z', 'hostgal_stdcolor_z_y', 'hostgal_stdcolor_u_g_err',
                                  'hostgal_stdcolor_g_r_err', 'hostgal_stdcolor_r_i_err',
                                  'hostgal_stdcolor_i_z_err', 'hostgal_stdcolor_z_y_err',
                                  'hostgal_snsep', 'hostgal_pzmean', 'hostgal_pzstd' }
