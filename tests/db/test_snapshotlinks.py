import pytest

from db import DiaSourceSnapshot, DiaForcedSourceSnapshot

from basetest import BaseTestDB


class TestDiaSourceSnapshot( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver1, procver2, snapshot1, snapshot2, obj1, src1, src1_pv2 ):
        self.cls = DiaSourceSnapshot
        self.columns = { 'diasourceid', 'processing_version', 'snapshot' }
        self.safe_to_modify = []
        self.uniques = []

        self.obj1 = DiaSourceSnapshot( diasourceid=src1.diasourceid,
                                       processing_version=procver1.id,
                                       snapshot=snapshot1.id )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaSourceSnapshot( diasourceid=src1_pv2.diasourceid,
                                       processing_version=procver2.id,
                                       snapshot=snapshot2.id )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        # All the self.dict3 stuff will be skipped
        self.dict3 = {}

    # Overloading doesn't work with pytest the way you would expect.
    # Have to hack this differently

    # @pytest.mark.skip( reason="Meaningless test" )
    # def test_full_update( self, obj1_inserted, obj2_inserted ):
    #     pass

    # @pytest.mark.skip( reason="Meaningless test" )
    # def test_some_update( self, obj1_inserted, obj2_inserted ):
    #     pass


class TestDiaForcedSourceSnapshot( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver1, procver2, snapshot1, snapshot2, obj1, forced1, forced1_pv2 ):
        self.cls = DiaForcedSourceSnapshot
        self.columns = { 'diaforcedsourceid', 'processing_version', 'snapshot' }
        self.safe_to_modify = []
        self.uniques = []

        self.obj1 = DiaForcedSourceSnapshot( diaforcedsourceid=forced1.diaforcedsourceid,
                                             processing_version=procver1.id,
                                             snapshot=snapshot1.id )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaForcedSourceSnapshot( diaforcedsourceid=forced1_pv2.diaforcedsourceid,
                                             processing_version=procver2.id,
                                             snapshot=snapshot2.id )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        # All the self.dict3 stuff will be skipped
        self.dict3 = {}
