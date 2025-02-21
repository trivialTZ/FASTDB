import pathlib

import subprocess


def test_load_snana_fits():
    e2td = pathlib.Path( "elasticc2_test_data" )
    assert e2td.is_dir()
    dirs = e2td.glob( "*" )
    dirs = [ d for d in dirs if d.is_dir() ]
    assert len(dirs) > 0

    com = [ "python", "/code/src/admin/load_snana_fits.py",
            "-n", "5",
            "--pv", "test_procver",
            "-s", "test_snapshot",
            "-v",
            "-d"
           ]
    com.extend( dirs )
    com.append( "--do" )

    import pdb; pdb.set_trace()
    res = subprocess.run( com, capture_output=True )
    import pdb; pdb.set_trace()
    pass
