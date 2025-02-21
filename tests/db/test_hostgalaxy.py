import uuid
import pytest

from db import HostGalaxy

from basetest import BaseTestDB


class TestDiaObject( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver1 ):
        self.cls = HostGalaxy
        self.columns = {
            'id',
            'processing_version',
            'objectid',
            'psradectai',
            'psra',
            'psdec',
            'stdcolor_u',
            'stdcolor_g',
            'stdcolor_r',
            'stdcolor_i',
            'stdcolor_z',
            'stdcolor_u_err',
            'stdcolor_g_err',
            'stdcolor_r_err',
            'stdcolor_i_err',
            'stdcolor_z_err',
            'pzmode',
            'pzmean',
            'pzstd',
            'pzskew',
            'pskurt',
            'pzquant01',
            'pzquant05',
            'pzquant25',
            'pzquant50',
            'pzquant75',
            'pzquant99',
            'flags',
        }
        self.safe_to_modify = [
            'objectid',
            'psradectai',
            'psra',
            'psdec',
            'stdcolor_u',
            'stdcolor_g',
            'stdcolor_r',
            'stdcolor_i',
            'stdcolor_z',
            'stdcolor_u_err',
            'stdcolor_g_err',
            'stdcolor_r_err',
            'stdcolor_i_err',
            'stdcolor_z_err',
            'pzmode',
            'pzmean',
            'pzstd',
            'pzskew',
            'pskurt',
            'pzquant01',
            'pzquant05',
            'pzquant25',
            'pzquant50',
            'pzquant75',
            'pzquant99',
            'flags',
        ]
        self.uniques = []

        self.obj1 = HostGalaxy( id=uuid.uuid4(),
                                processing_version=procver1.id,
                                objectid=42,
                                psradectai=60000.,
                                psra=13.,
                                psdec=-66.
                               )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = HostGalaxy( id=uuid.uuid4(),
                                processing_version=procver1.id,
                                objectid=23,
                                psradectai=60100.,
                                psra=137.,
                                psdec=42.,
                               )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'processing_version': procver1.id,
                       'objectid': 31337,
                       'psra': 32.,
                       'psdec': 64.
                       }
