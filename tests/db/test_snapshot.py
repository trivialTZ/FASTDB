import datetime
import pytest

from db import Snapshot

from basetest import BaseTestDB


class TestSnapshot( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = Snapshot
        self.columns = { 'id', 'description', 'creation_time' }
        self.safe_to_modify = [ 'creation_time' ]
        self.uniques = [ 'description' ]
        t0 = datetime.datetime.now( tz=datetime.UTC )
        self.obj1 = Snapshot( id=1,
                              description='ss1',
                              creation_time=t0 )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = Snapshot( id=2,
                              description='ss2',
                              creation_time = t0 + datetime.timedelta( hours=1 ) )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': 3,
                       'description': 'ss3',
                       'creation_time': t0 + datetime.timedelta( days=1 ) }
