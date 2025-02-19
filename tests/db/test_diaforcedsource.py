import datetime
import pytest

from db import DiaForcedSource

from basetest import BaseTestDB


class TestDiaForcedSource( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver1, obj1 ):
        self.cls = DiaForcedSource
        self.columns = {
            'diaforcedsourceid',
            'diaobjectuuid',
            'processing_version',
            'visit',
            'detector',
            'midpointmjdtai',
            'band',
            'ra',
            'dec',
            'psfflux',
            'psffluxerr',
            'scienceflux',
            'sciencefluxerr',
            'time_processed',
            'time_withdrawn',
        }
        self.safe_to_modify = [
            'visit',
            'detector',
            'midpointmjdtai',
            'band',
            'ra',
            'dec',
            'psfflux',
            'psffluxerr',
            'scienceflux',
            'sciencefluxerr',
            'time_processed',
            'time_withdrawn',
        ]
        self.uniques = []

        t0 = datetime.datetime.now( tz=datetime.UTC )
        self.obj1 = DiaForcedSource( diaforcedsourceid=1,
                                     diaobjectuuid=obj1.id,
                                     processing_version=procver1.id,
                                     visit=1,
                                     detector=1,
                                     midpointmjdtai=60000.,
                                     band='r',
                                     ra=42.,
                                     dec=13.,
                                     psfflux=123.4,
                                     psffluxerr=5.6,
                                     scienceflux=234.5,
                                     sciencefluxerr=7.8,
                                     time_processed=t0,
                                     time_withdrawn=None
                                    )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaForcedSource( diaforcedsourceid=2,
                                     diaobjectuuid=obj1.id,
                                     processing_version=procver1.id,
                                     visit=2,
                                     detector=2,
                                     midpointmjdtai=60001.,
                                     band='i',
                                     ra=42.0001,
                                     dec=13.0001,
                                     psfflux=123.5,
                                     psffluxerr=5.7,
                                     scienceflux=235.5,
                                     sciencefluxerr=7.9,
                                     time_processed=t0 + datetime.timedelta( days=1 ),
                                     time_withdrawn=t0 + datetime.timedelta( days=365 )
                                    )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaforcedsourceid': 3,
                       'diaobjectuuid': obj1.id,
                       'processing_version': procver1.id,
                       'visit': 3,
                       'detector': 3,
                       'midpointmjdtai': 600002.,
                       'band': 'g',
                       'ra': 41.9999,
                       'dec': 12.9999,
                       'psfflux': 122.3,
                       'psffluxerr': 4.5,
                       'scienceflux': 233.4,
                       'sciencefluxerr': 5.6,
                       'time_processed': t0 + datetime.timedelta( days=2 ),
                       'time_withdrawn': t0 + datetime.timedelta( weeks=3 )
                      }
