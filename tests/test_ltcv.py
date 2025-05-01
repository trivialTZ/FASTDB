import ltcv


# The fixture takes a while (30-60 seconds) to run.  Note that it's a module-scope fixture!
def test_get_hot_ltcvs( procver, alerts_90days_sent_received_and_imported ):
    df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_since_mjd=60325., mjd_now=60328. )

    # Should have found 88 lightcurve points on 4 objects
    assert len(df.rootid.unique()) == 4
    assert len(df) == 88
    # Make sure we don't have anything newer than now
    assert df.midpointmjdtai.max() < 60328.
    # We didn't ask for hosts
    assert hostdf is None

    # Now patch in sources where there aren't forced sources
    df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_since_mjd=60325., mjd_now=60328., source_patch=True )

    # Should have picked up 3 additional light curve points
    assert len(df.rootid.unique()) == 4
    assert len(df) == 91
    assert df.is_source.sum() == 3

    # Make sure everything was detected since 60325.  (We don't
    #   actually have a detected flag, but check S/N > 5.)
    # (Note that this next assert will *not* pass with the df you get
    #   with source_patch=False, because there are some detections that
    #   don't have corresponding forced photometry.)
    assert ( set( df[ ( df.midpointmjdtai >= 60325. )
                      & ( df.psfflux/df.psffluxerr > 5. )
                    ].rootid.unique() )
             == set( df.rootid.unique() ) )

    # Make sure that we get the same thing using detected_in_last_days
    df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_in_last_days=3., mjd_now=60328., source_patch=True )
    assert hostdf is None
    assert len(df.rootid.unique()) == 4
    assert len(df) == 91
    assert df.is_source.sum() == 3
    assert ( set( df[ ( df.midpointmjdtai >= 60325. )
                      & ( df.psfflux/df.psffluxerr > 5. )
                    ].rootid.unique() )
             == set( df.rootid.unique() ) )

    # We should get more without passing a date limit, since it will do detected in the last 30 days
    df, hostdf = ltcv.get_hot_ltcvs( procver.description, mjd_now=60328., source_patch=True )
    assert df.midpointmjdtai.max() < 60328.
    assert len(df.rootid.unique()) == 14
    assert len(df) == 310
    # In case you are surprised that this next value is more than before (since we're
    # stopping at the same day, so we should have the same missing forced photometry),
    # remember that at least as of right now, we don't have any "import updated forced
    # photometry from the PPDB" routine going.  (We'll eventually want to think about
    # that!  Issue #10.)  As such, there are going to be an number of objects whose last
    # alert was not in the last three days, and the latest point from all of those alerts
    # will not have forced photometry because there will not have been a later alert
    # that would have it!
    # (....in fact, that's the explanation for *all* of the missing forced photometry,
    # because the database has times later than mjd 60328 based on what we ran in the fixture.)
    assert df.is_source.sum() == 12

    # Empirically, there's a detection that only has a S/N of ~2.8.  This highlights that
    #   detection is more complicated than S/N > 5.  So, throw in "is_source=True" to
    #   capture this detection.  is_source=True by itself is not enough to tell if it's a
    #   detection, because we will only have included sources for which there was no
    #   diaforcedsource.
    assert ( set( df[ ( df.midpointmjdtai >= 60297. )
                      & ( ( df.psfflux/df.psffluxerr > 5. ) | ( df.is_source ) )
                    ].rootid.unique() )
             == set( df.rootid.unique() ) )


    # Now lets get hosts
    df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_since_mjd=60325., mjd_now=60328.,
                                     source_patch=True, include_hostinfo=True )
    assert len(df) == 91
    assert len(hostdf) == 4
    assert set( hostdf.rootid ) == set( df.rootid.unique() )

    # TODO : more stringent tests
