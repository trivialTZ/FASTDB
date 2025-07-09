import datetime

import astropy.time
import pandas

import db
import util
import rkwebutil


def object_search( processing_version, return_format='json', **kwargs ):
    knownargs = { 'ra', 'dec',
                  'mint_firstdetection', 'maxt_firstdetection',
                  'mint_lastdetection', 'maxt_lastdetection'
                  'min_numdetections', 'mindt_firstlastdetection','maxdt_firstlastdetection',
                  'min_bandsdetected', 'min_lastmag', 'max_lastmag' }
    unknownargs = set( kwargs.keys() ) - knownargs
    if len( unknownargs ) != 0:
        raise ValueError( f"Unknown search keywords: {unknownargs}" )

    if return_format not in [ 'json', 'pandas' ]:
        raise ValueError( "Unknown return format 'return_format'" )
    
    with db.DB() as con:
        cursor = con.cursor()

        # Figure out processing version
        try:
            procver = int( processing_version)
        except Exception:
            cursor.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                            { 'procver': processing_version } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                { 'procver': processing_version } )
                rows = cursor.fetchall()
                if len(rows) == 0:
                    raise ValueError( f"Unknown processing version {processing_version}" )
            procver = rows[0][0]
        
        # Filter by ra and dec if given
        ra = util.float_or_none_from_dict_float_or_hms( kwargs, 'ra' )
        dec = util.float_or_none_from_dict_float_or_dms( kwargs, 'dec' )
        if ( ra is None ) != ( dec is None ):
            raise ValueError( "Must give either both or neither of ra and dec, not just one." )

        nexttable = 'diaobject'
        if ra is not None:
            radius = util.float_or_none_from_dict( kwargs, 'radius' )
            radius = radius if radius is not None else 10.
            cursor.execute( "SELECT * INTO TEMP TABLE objsearch_tmp1 "
                            "FROM diaobject "
                            "WHERE processing_version=%(pv) "
                            "AND q3c_radial_query( ra, dec, %(ra), %(dec), %(rad) )"
                            { 'pv': procver, 'ra': ra, 'dec': dec, 'rad': radius/3600. } )
            nexttable = 'objsearch_tmp1'

        mint_firstdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'mint_firstdetection' )
        maxt_firstdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'maxt_firstdetection' )
        mint_lastdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'mint_lastdetection' )
        maxt_lastdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'mint_lastdetection' )
        if any( i is not None for i in [ mint_firstdet, maxt_firstdet, mint_lastdet, maxt_lastdet ] ):
            raise NotImplementedError( "Filtering by detection times not yet implemented" )

        min_numdets = util.int_or_none_from_dict( kwargs, 'min_numdetections' )
        if min_numdets is not None:
            raise NotImplementedError( "Filtering by number of detections not yet implemented" )

        mindt = util.float_or_none_from_dict( kwargs, 'mindt_firstlastdetection' )
        maxdt = util.float_or_none_from_dict( kwargs, 'maxdt_firstlastdetection' )
        if ( mindt is not None ) or ( maxdt is not None ):
            raise NotImplementedError( "Filtering by time between first and last detection not yet implemented" )

        min_bands = util.int_or_none_from_dict( kwargs, 'min_bandsdetected' )
        if min_bands is not None:
            raise NotImplementedError( "Filtering by number of bands detected is not yet implemented" )

        min_lastmag = util.float_or_none_from_dict( kwargs, 'min_lastmag' )
        max_lastmag = util.float_or_none_from_dict( kwargs, 'max_lastmag' )
        if ( min_lastmag is not None ) or ( max_lastmag is not None ):
            raise NotImplementedError( "Filtering by last magnitude not yet implemented" )


        if nexttable == 'diaobject':
            raise RuntimeError( "Error, no search criterion given" )
        
        q = ( f"SELECT o.diaobjectid, o.ra, o.dec, s.psfflux AS srcflux, s.midpointmjdtai AS srct, s.band AS srcband, "
              f"INTO TEMP TABLE objsearch_sources "
              f"FROM {nexttable} o "
              f"INNER JOIN diasource s ON o.diaobjectid=s.diaobjectid AND s.processing_version=%(pv)s "
              f"ORDER BY diaobjectid, srct" )
        cursor.execute( q, { 'pv': procver } )
        
        cursor.execute( "SELECT diaobjectid, ra, dec, COUNT(srcflux) AS ndet, MAX(srcflux) AS maxflux, "
                        "        LAST(psfflux) AS lastflux, LAST(srcband) AS lastband, LAST(srct) AS lastt "
                        "INTO TEMP TABLE objsearch_srcstats "
                        "FROM objsearch_sources "
                        "GROUP BY diaobjectid, ra, dec" )

        cursor.execute( "SELECT t.diaobjectid, t.ra, t.dec, t.ndet, t.maxflux, t.lastflux, t.lastband, t.lastt "
                        "       f.midpointmjdtai AS frct, f.psfflux AS frcflux, f.psffluxerr AS frcdflux, "
                        "       f.band AS frcband "
                        "INTO TEMP TABLE objsearch_forced "
                        "FROM objsearch_srcstats "
                        "INNER JOIN diaforcedsource f ON t.diaobjectid=f.diaobjectid AND f.processing_version=%(pv)s "
                        "ORDER BY diaobjectid, frct",
                        { 'pv': procver } )
        cursor.execute( "SELECT diaobjectid, ra, dec, ndet, maxflux, lastflux, lastband, lastt, "
                        "       LAST(frct) AS lastfrcedt, LAST(frcflux) AS lastfrcflux, "
                        "       LAST(lastfrcdflux) AS lastfrcdflux, LAST(lastfrband) AS lastfrcband "
                        "FROM objsearch_forced "
                        "GROUP BY diaobjectid, ra, dec, ndet, maxflux, lastflux, lastband, lastt" )
        columns = [ d[0] for d in cursor.description ]
        colummap = { cursor.description[i][0]: i for i in range( len(cursor.description) ) }
        rows = cursor.fetchall()
        

    if return_format == 'json':
        return { c: [ r[ colummap[c] ] ] for c in columns }

    elif return_format == 'pandas':
        return pandas.DataFrame( rows, columns=columns )

    else:
        raise RuntimeError( "This should never happen." )


def get_hot_ltcvs( processing_version, detected_since_mjd=None, detected_in_last_days=None,
                   mjd_now=None, source_patch=False, include_hostinfo=False ):
    """Get lightcurves of objects with a recent detection.

    Parameters
    ----------
      processing_version: string
        The description of the processing version, or processing version
        alias, to use for searching all tables.

      detected_since_mjd: float, default None
        If given, will search for all objects detected (i.e. with an
        entry in the diasource table) since this mjd.

      detected_in_last_days: float, default 30
        If given, will search for all objects detected since this many
        days before now.  Can't explicitly pass both this and
        detected_since_mjd.  If detected_since_mjd is given, the default
        here is ignored.

      mjd_now : float, default None
        What "now" is.  By default, this does an MJD conversion of
        datetime.datetime.now(), which is usually what you want.  But,
        for simulations or reconstructions, you might want to pretend
        it's a different time.

      source_path : bool, default Fals
        Normally, returned light curves only return fluxes from the
        diaforcedsource table.  However, during the campaign, there will
        be sources detected for which there is no forced photometry.
        (Alerts are sent as sources are detected, but forced photometry
        is delayed.)  Set this to True to get more complete, but
        heterogeneous, lightcurves.  When this is True, it will look for
        all detections that don't have a corresponding forced photometry
        point (i.e. observation of the same ojbject in the same visit),
        and add the detections to the lightcurve.  Be aware that these
        photometry points don't mean exactly the same thing, as forced
        photometry is all at one position, but detections are where they
        are.  This is useful for doing real-time filtering and the like,
        but *not* for any kind of precision photometry or lightcurve fitting.

      include_hostinfo : bool, default False
        If true, return a second data frame with information about the hosts.

      Returns
      -------
        ( pandas.DataFrame, pandas.DataFrame or None )

        A 2-element tuple.  The first will be a pandas DataFrame, the
        second will either be another DataFrame or None.  No indexes
        will have been set in the dataframes.

        The first one has the lightcurves.  It has columns:
           rootid -- the object root ID from the database.  (TODO: document.)
           sourceid -- either the diaforcedsourceid or the diasourceid
                        from the database (see below)
           ra -- the ra of the *object* (interpretation complicated).
                   Will be the same for all rows with the same rootid.
                   Decimal degrees, J2000.
           dec -- the dec of the *object* (goes with ra).  Decmial degrees, J2000.
           visit -- the visit number
           detector -- the detector number
           midpointmjdtai -- the MJD of the obervation
           band -- the filter (u, g, r, i, z, or Y)
           psfflux -- the PSF flux in nJy
           psffluxerr --- uncertaintly on psfflux in nJy
           is_source -- bool.  If you specified source_aptch=False, this
                          will be False for all rows.  Otherwise, it's
                          True for rows pulled from the diasource table,
                          and False for rows pulled from the
                          diaforcedsource table.  If is_source is True,
                          then sourceid is the diasourceid; if is_source
                          is False, then sourceid is the
                          diaforcedsourceid.

       The second member of the tuple will be None unless you specified
       include_hostinfo.  If include_hostinfo is true, then it's a
       dataframe with the following columns.  Note that the root id does
       *not* uniquely specify the host properties!  The
       processing_version you gave will affect which rows were actually
       pulled from the diaobject table.  (And it's potentially more
       complicated than that....)  These are mostly defined based on
       looking at the Object table as defined in the 2023-07-10 version
       of the DPDD in Table 4.3.1, with some columns coming from the
       DiaObject table defined by https://sdm-schemas.lsst.io/apdb.html
       (accessed on 2024-04-30).

           rootid --- the object root ID from the database.  Use this to
                        match to the lightcurve data frame.
           stdcolor_u_g -- "standard" colors as (not really) defined by the DPDD, in AB mags
           stdcolor_g_r --
           stdcolor_r_i --
           stdcolor_i_z --
           stdcolor_z_y --
           stdcolor_u_g_err -- uncertainty on standard colors
           stdcolor_g_r_err --
           stdcolor_r_i_err --
           stdcolor_i_z_err --
           stdcolor_z_y_err --
           petroflux_r -- the flux in nJy within some standard multiple of the petrosian radius
           petroflux_r_err -- uncertainty on petroflux_r
           nearbyextobj1sep -- "Second moment-based separation of nearbyExtObj1 (unitless)" [????]
                               For SNANA-based sims, this is ROB FIGURE THIS OUT
           pzmean -- mean photoredshift (nominally from the "photoZ_pest" column of the DPD Object table)
           pzstd -- standard deviation of photoredshift (also nominally from "photoZ_pest")

    """

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

            # First : get a table of all the object ids (root object ids)
            #   that have a detection (i.e. a diasource) in the
            #   desired time period.

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

            # Second : pull out host info for those objects if requested
            # TODO : right now it just pulls out nearby extended object 1.
            # make it configurable to get up to all three.
            hostdf = None
            if include_hostinfo:
                q = "SELECT DISTINCT ON (r.rootid) r.rootid,"
                for bandi in range( len(bands)-1 ):
                    q += ( f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]},"
                           f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]}_err," )
                q += ( "h.petroflux_r,h.petroflux_r_err,o.nearbyextobj1sep,h.pzmean,h.pzstd "
                       "FROM diaobject_root_map r "
                       "INNER JOIN diaobject o ON ( r.diaobjectid=o.diaobjectid AND "
                       "                            r.processing_version=o.processing_version ) "
                       "INNER JOIN host_galaxy h ON o.nearbyextobj1id=h.id "
                       "WHERE r.rootid IN (SELECT rootid FROM tmp_objids) "
                       "  AND o.processing_version=%(procver)s "
                       "ORDER BY r.rootid" )
                cursor.execute( q, { 'procver': procver} )
                columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]
                hostdf = pandas.DataFrame( cursor.fetchall(), columns=columns )


            # Third : pull out all the forced photometry
            # THOUGHT REQUIRED : do we want midmpointmjdtai to stop at mjd_now-1 rather
            #   than mjd_now?  It depends what you mean.  If you want mjd_now to mean
            #   "data through this date" then don't stop a day early.  If you mean
            #   "simulate what we knew on this date"), then do stop a day early, because
            #   forced photometry will be coming out with a delay of a ~day.
            q = ( "/*+ IndexScan(f idx_diaforcedsource_diaobjectidpv)\n"
                  "    IndexScan(o)\n"
                  "*/\n"
                  "SELECT r.rootid AS rootid, o.ra AS ra, o.dec AS dec,"
                   "     f.diaforcedsourceid AS sourceid,f.visit,f.detector,f.midpointmjdtai,f.band,"
                   "     f.psfflux,f.psffluxerr "
                   "FROM diaforcedsource f "
                   "INNER JOIN diaobject o ON (f.diaobjectid=o.diaobjectid AND "
                   "                           f.diaobject_procver=o.processing_version) "
                   "INNER JOIN diaobject_root_map r ON (o.diaobjectid=r.diaobjectid AND "
                   "                                    o.processing_version=r.processing_version) "
                   "WHERE r.rootid IN (SELECT rootid FROM tmp_objids) "
                   "  AND f.processing_version=%(procver)s" )
            if mjd_now is not None:
                q += "  AND f.midpointmjdtai<=%(t1)s "
            q += "ORDER BY r.rootid,f.midpointmjdtai"
            cursor.execute( q, { "procver": procver, "t1": mjd_now } )
            columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]
            forceddf = pandas.DataFrame( cursor.fetchall(), columns=columns )
            forceddf['is_source'] = False

            # Fourth: if we've been asked to patch in sources where forced sources are
            #   missing, pull those down and concatenate them into the dataframe.
            # TODO : figure out the right hints to give when these tables
            #   are big!
            sourcedf = None
            if source_patch:
                q = ( "/*+ IndexScan(s idx_diasource_diaobjectidpv)\n"
                      "    IndexScan(f idx_diaforcedsource_diaobjectidpv)\n"
                      "    IndexScan(o)\n"
                      "*/\n"
                      "SELECT r.rootid,o.ra,o.dec,s.diasourceid AS sourceid,s.visit,s.detector,"
                      "       s.midpointmjdtai,s.band,s.psfflux,s.psffluxerr "
                      "FROM diasource s "
                      "INNER JOIN diaobject o ON (s.diaobjectid=o.diaobjectid AND "
                      "                           s.diaobject_procver=o.processing_version) "
                      "INNER JOIN diaobject_root_map r ON (o.diaobjectid=r.diaobjectid AND "
                      "                                    o.processing_version=r.processing_version) "
                      "LEFT JOIN diaforcedsource f ON (f.diaobjectid=s.diaobjectid AND "
                      "                                f.diaobject_procver=s.diaobject_procver AND "
                      "                                f.visit=s.visit AND "
                      "                                f.detector=s.detector) "
                      "WHERE r.rootid IN (SELECT rootid FROM tmp_objids) "
                      "  AND s.processing_version=%(procver)s "
                      "  AND f.diaobjectid IS NULL " )
                if mjd_now is not None:
                    q += "  AND s.midpointmjdtai<=%(t1)s "
                q += "ORDER BY r.rootid,s.midpointmjdtai"
                cursor.execute( q, { "procver": procver, "t1": mjd_now } )
                columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]
                sourcedf = pandas.DataFrame( cursor.fetchall(), columns=columns )
                sourcedf['is_source'] = True
                forceddf = pandas.concat( [ forceddf, sourcedf ], axis='index' )
                forceddf.sort_values( [ 'rootid', 'midpointmjdtai' ], inplace=True )

    return forceddf, hostdf
