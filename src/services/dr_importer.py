import db


class DRImporter:
    """Import stuff from an LSST database.

    Currently this is all BS because it's using the SNANA PPDB tables.
    This will need to get completely rewritten when we know what
    interfaces will be and how to actually use it.

    """

    # Not including processing_version
    host_galaxy_cols = [ 'id', 'objectid', 'ra', 'dec', 'petroflux_r', 'petroflux_r_err',
                         'stdcolor_u_g', 'stdcolor_g_r', 'stdcolor_r_i', 'stdcolor_i_z', 'stdcolor_z_y',
                         'stdcolor_u_g_err', 'stdcolor_g_r_err', 'stdcolor_r_i_err', 'stdcolor_i_z_err',
                         'stdcolor_z_y_err',
                         'pzmode', 'pzmean', 'pzstd', 'pzskew', 'pzkurt',
                         'pzquant000', 'pzquant010', 'pzquant020', 'pzquant030', 'pzquant040', 'pzquant050',
                         'pzquant060', 'pzquant070', 'pzquant080', 'pzquant090', 'pzquant100',
                         'flags' ]

    def __init__( self, processing_version ):
        """Make

        Parameters
        ----------
          processing_version : int
            The processing version of the objects and hosts to look at.

        """
        self.processing_version = int( processing_version )


    # This is all written to the SNANA PPDB simulation tables.
    def import_host_info( self ):
        with db.DB() as conn:
            cursor = conn.cursor()

            # Figure out which host galaxies we don't know about yet.
            # This procedure will potentially create some duplicate entries,
            # but whatevs.
            # TODO : think about ids, processing versions, etc.

            for i in range( 1, 4 ):
                if i == 1:
                    q = "CREATE TEMP TABLE temp_missing_hosts AS "
                else:
                    q = "INSERT INTO temp_missing_hosts "
                q += ( f"( SELECT o.nearbyextobj{i} FROM diaobject o "
                       f"  LEFT JOIN host_galaxy h ON ( o.nearbyextobj{i}=h.objectid AND "
                       f"                               o.processing_version=h.processing_version ) "
                       f"  WHERE o.processing_version=%(procver)s "
                       f"    AND h.objectid IS NULL "
                       f"    AND o.nearbyextobj{i} IS NOT NULL )" )
                cursor.execute( q, { 'procver': self.processing_version } )

            q = ( f"INSERT INTO host_galaxy({','.join(self.host_galaxy_cols)},processing_version) "
                  f"( SELECT {','.join(self.host_galaxy_cols)},%(procver)s FROM ppdb_host_galaxy "
                  f"  WHERE objectid IN (SELECT * FROM temp_missing_hosts) )" )
            cursor.execute( q, { 'procver': self.processing_version } )
            nhosts = cursor.rowcount

            # Update the objects table to have the host uuids
            # (TODO: check how slow this query is; is postgres smart enough
            #  to do the merge right?  I think so...)
            for i in range( 1, 4 ):
                q = ( f"UPDATE diaobject o SET nearbyextobj{i}id="
                      f"( SELECT id FROM host_galaxy h WHERE ( h.objectid=o.nearbyextobj{i} AND "
                      f"                                       h.processing_version=o.processing_version ) ) "
                      f"WHERE o.nearbyextobj{i} IN ( SELECT * FROM temp_missing_hosts ) "
                      f"  AND o.processing_version=%(procver)s" )
                cursor.execute( q, { 'procver': self.processing_version } )

            conn.commit()

            return nhosts

