import pytest
import uuid
import datetime

from db import ProcessingVersion, DiaObject, DiaSource, DiaForcedSource, Snapshot, DB


@pytest.fixture
def procver1():
    pv = ProcessingVersion( id=42,
                            description='pv42',
                            validity_start=datetime.datetime( 2025, 2, 14, 1, 2, 3 ),
                            validity_end=datetime.datetime( 2030, 2, 14, 1, 2, 3 )
                           )
    pv.insert()

    yield pv
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM processingversion WHERE id=%(id)s",
                        { 'id': pv.id } )
        con.commit()


@pytest.fixture
def procver2():
    pv = ProcessingVersion( id=23,
                            description='pv23',
                            validity_start=datetime.datetime( 2015, 10, 21, 12, 15, 0 ),
                            validity_end=datetime.datetime( 2045, 10, 21, 12, 15, 0 )
                           )
    pv.insert()

    yield pv
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM processingversion WHERE id=%(id)s",
                        { 'id': pv.id } )
        con.commit()


@pytest.fixture
def snapshot1():
    ss = Snapshot( id=23,
                   description='ss23',
                   creation_time=datetime.datetime( 2000, 1, 1, 0, 0, 0 )
                  )
    ss.insert()

    yield ss
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM snapshot WHERE id=%(id)s",
                        { 'id': ss.id } )
        con.commit()


@pytest.fixture
def snapshot2():
    ss = Snapshot( id=42,
                   description='ss42',
                   creation_time=datetime.datetime( 2001, 12, 31, 11, 59, 59 )
                  )
    ss.insert()

    yield ss
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM snapshot WHERE id=%(id)s",
                        { 'id': ss.id } )
        con.commit()


@pytest.fixture
def obj1( procver1 ):
    obj = DiaObject( id=uuid.uuid4(),
                     processing_version=procver1.id,
                     diaobjectid=42,
                     radecmjdtai=60000.,
                     ra=42.,
                     dec=13
                    )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaobject WHERE id=%(id)s",
                        { 'id': str(obj.id) } )
        con.commit()


@pytest.fixture
def src1( obj1, procver1 ):
    src = DiaSource( diasourceid=42,
                     processing_version=procver1.id,
                     diaobjectuuid=obj1.id,
                     visit=64,
                     detector=9,
                     band='r',
                     midpointmjdtai=59000.,
                     ra=obj1.ra + 0.0001,
                     dec=obj1.dec + 0.0001,
                     psfflux=3.,
                     psffluxerr=0.1
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE diasourceid=%(id)s",
                        { 'id': src.diasourceid } )
        con.commit()


@pytest.fixture
def src1_pv2( obj1, procver2 ):
    src = DiaSource( diasourceid=42,
                     processing_version=procver2.id,
                     diaobjectuuid=obj1.id,
                     visit=64,
                     detector=9,
                     band='r',
                     midpointmjdtai=59000.,
                     ra=obj1.ra + 0.0001,
                     dec=obj1.dec + 0.0001,
                     psfflux=3.,
                     psffluxerr=0.1
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE diasourceid=%(id)s",
                        { 'id': src.diasourceid } )
        con.commit()


@pytest.fixture
def forced1( obj1, procver1 ):
    src = DiaForcedSource( diaforcedsourceid=42,
                           processing_version=procver1.id,
                           diaobjectuuid=obj1.id,
                           visit=128.,
                           detector=10.,
                           midpointmjdtai=59100.,
                           band='i',
                           ra=obj1.ra - 0.0001,
                           dec=obj1.dec - 0.0001,
                           psfflux=4.,
                           psffluxerr=0.2,
                           scienceflux=7.,
                           sciencefluxerr=0.5,
                           time_processed=None,
                           time_withdrawn=None
                          )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE diaforcedsourceid=%(id)s",
                        { 'id': src.diaforcedsourceid } )
        con.commit()


@pytest.fixture
def forced1_pv2( obj1, procver2 ):
    src = DiaForcedSource( diaforcedsourceid=42,
                           processing_version=procver2.id,
                           diaobjectuuid=obj1.id,
                           visit=128.,
                           detector=10.,
                           midpointmjdtai=59100.,
                           band='i',
                           ra=obj1.ra - 0.0001,
                           dec=obj1.dec - 0.0001,
                           psfflux=4.,
                           psffluxerr=0.2,
                           scienceflux=7.,
                           sciencefluxerr=0.5,
                           time_processed=None,
                           time_withdrawn=None
                          )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE diaforcedsourceid=%(id)s",
                        { 'id': src.diaforcedsourceid } )
        con.commit()
