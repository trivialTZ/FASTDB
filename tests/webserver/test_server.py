import pytest
import db


@pytest.fixture( scope='module' )
def server_test_processing_versions():
    try:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "INSERT INTO processing_version(id,description,validity_start,validity_end) "
                            "VALUES (64738, 'test_server_1', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version(id,description,validity_start,validity_end) "
                            "VALUES (64739, 'test_server_2', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version(id,description,validity_start,validity_end) "
                            "VALUES (64740, 'test_server_3', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version_alias(id,description) "
                            "VALUES (64738, 'test_server_1_alias_1', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version_alias(id,description) "
                            "VALUES (64738, 'test_server_1_alias_2', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version_alias(id,description) "
                            "VALUES (64739, 'test_server_2_alias_1', NOW(), NULL)" )
            con.commit()

        yield True

    finally:
        with db.DB() as con:
            cursor.execute( "DELETE FROM processing_version_alias WHERE id=ANY(64738,64739,64740)" )
            cursor.execute( "DELETE FROM processing_version WHERE id=ANY(64738,64739,64740)" )
            con.commit()


def test_getprocvers( server_test_processing_versions, test_user, fastdb_client ):
    res = fastdb_client.post( '/getprocvers' )
    assert isinstance( res, dict )
    assert res['status'] == 'ok'
    assert res['procvers'] == [ 'test_server_1', 'test_server_1_alias_1', 'test_server_1_alias_2',
                                'test_server_2', 'test_server_2_alias_1' ]


def test_procver( server_test_processing_versions, test_user, fastdb_client ):
    for suffix in [ '64738', 'test_server_1', 'test_server_1_alias_1', 'test_server_1_alias_2' ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'
        assert res['id'] == 64738
        assert res['description'] == 'test_server_1'
        assert res['aliases'] == [ 'test_server_1_alias_1', 'test_server_1_alias_2' ]

    for suffix in [ '64738', 'test_server_2', 'test_server_2_alias_1' ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'
        assert res['id'] == 64739
        assert res['description'] == 'test_server_2'
        assert res['aliases'] == [ 'test_server_2_alias_1' ]

    for suffix in [ '64740', 'test_server_3' ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'
        assert res['id'] == 64740
        assert res['description'] == 'test_server_3'
        assert res['aliases'] == []

    res = fastdb_client.post( '/procver/64741' )
    pass
    res = fastdb_client.post( '/procver/does_not_exist' )
    pass
