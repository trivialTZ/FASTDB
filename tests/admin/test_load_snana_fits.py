import pathlib
import subprocess
import db
import uuid


def _truncate_main_tables():
    """Truncate relevant FASTDB tables before/after testing."""
    tables = [
        'diaobject_snapshot', 'diasource_snapshot', 'diaforcedsource_snapshot',
        'diasource', 'diaforcedsource', 'diaobject', 'host_galaxy',
        'snapshot', 'processing_version'
    ]
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE " + ", ".join(tables) + " RESTART IDENTITY CASCADE")
        conn.commit()


def test_load_snana_fits():
    _truncate_main_tables()

    # Use random unique names so test is rerunnable
    pv_name = f"test_procver_{uuid.uuid4().hex[:8]}"
    ss_name = f"test_snapshot_{uuid.uuid4().hex[:8]}"

    e2td = pathlib.Path("elasticc2_test_data")
    assert e2td.is_dir()
    dirs = [d for d in e2td.glob("*") if d.is_dir()]
    assert len(dirs) > 0

    try:
        com = [
            "python", "/code/src/admin/load_snana_fits.py",
            "-n", "5",
            "--pv", pv_name,
            "-s", ss_name,
            "-v",
            "-d", *[str(d) for d in dirs],
            "--do"
        ]
        res = subprocess.run(com, capture_output=True)
        assert res.returncode == 0, res.stderr.decode()

        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM processing_version WHERE description = %s", (pv_name,))
            pv_id = cursor.fetchone()[0]
            cursor.execute("SELECT id FROM snapshot WHERE description = %s", (ss_name,))
            ss_id = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM host_galaxy WHERE processing_version = %s", (pv_id,))
            assert cursor.fetchone()[0] == 356

            cursor.execute("SELECT COUNT(*) FROM diaobject WHERE processing_version = %s", (pv_id,))
            assert cursor.fetchone()[0] == 346

            cursor.execute("SELECT COUNT(*) FROM diaobject_snapshot WHERE processing_version = %s AND snapshot = %s", (pv_id, ss_id))
            assert cursor.fetchone()[0] == 346

            cursor.execute("SELECT COUNT(*) FROM diasource WHERE processing_version = %s", (pv_id,))
            assert cursor.fetchone()[0] == 1862

            cursor.execute("SELECT COUNT(*) FROM diasource_snapshot WHERE processing_version = %s AND snapshot = %s", (pv_id, ss_id))
            assert cursor.fetchone()[0] == 1862

            cursor.execute("SELECT COUNT(*) FROM diaforcedsource WHERE processing_version = %s", (pv_id,))
            assert cursor.fetchone()[0] == 52172

            cursor.execute("SELECT COUNT(*) FROM diaforcedsource_snapshot WHERE processing_version = %s AND snapshot = %s", (pv_id, ss_id))
            assert cursor.fetchone()[0] == 52172

            # Confirm that ppdb tables remain empty
            for tab in ['ppdb_host_galaxy', 'ppdb_diaobject', 'ppdb_diasource', 'ppdb_diaforcedsource']:
                cursor.execute(f"SELECT COUNT(*) FROM {tab}")
                assert cursor.fetchone()[0] == 0

    finally:
        _truncate_main_tables()


def test_load_snana_fits_ppdb(snana_fits_ppdb_loaded):
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ppdb_host_galaxy")
        assert cursor.fetchone()[0] == 356
        cursor.execute("SELECT COUNT(*) FROM ppdb_diaobject")
        assert cursor.fetchone()[0] == 346
        cursor.execute("SELECT COUNT(*) FROM ppdb_diasource")
        assert cursor.fetchone()[0] == 1862
        cursor.execute("SELECT COUNT(*) FROM ppdb_diaforcedsource")
        assert cursor.fetchone()[0] == 52172

        # Ensure non-ppdb tables remain empty
        for tab in [
            'processing_version', 'snapshot', 'host_galaxy',
            'diaobject', 'diaobject_snapshot',
            'diasource', 'diasource_snapshot',
            'diaforcedsource', 'diaforcedsource_snapshot'
        ]:
            cursor.execute(f"SELECT COUNT(*) FROM {tab}")
            assert cursor.fetchone()[0] == 0