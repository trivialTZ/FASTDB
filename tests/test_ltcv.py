import ltcv


# The fixture takes a while (30-60 seconds) to run.  Note that it's a module-scope fixture!
def test_get_hot_ltcvs( procver, alerts_90days_sent_received_and_imported ):
    df = ltcv.get_hot_ltcvs( procver.description, detected_since_mjd=60325., mjd_now=60328. )

    # Should have found 88 lightcurve points on 4 objects
    assert len(df.rootid.unique() == 4 )
    assert len(df == 88 )
    # Make sure we don't have anything newer than now
    assert df.midpointmjdtai.max() < 60328.
    # Make sure everything was detected since 60325.  (We don't
    #   actually have a detected flag, but check S/N > 5.)



    import pdb; pdb.set_trace()
    pass
