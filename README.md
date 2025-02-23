# FASTDB
Development of the Fast Access to Survey Transients Database (FASTDB).


## Rob Notes

This README file needs to be orgnized better.

### Installing fastdb_rknop_dev

The base directory is
```
cd /global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev
```

In that directory there should be subdirectories `install`, `query_results`, and `sessions`.  There should also be a `FASTDB` directory which was generated with
```
git clone git@github.com:LSSTDESC/FASTDB
cd FASTDB
git checkout <version>
git submodule update --init
```

(With `git pull` updates and such as usual.)

To rebuild, do the following.  The "touch" steps are necessary because a `git clone` or `git pull` doesn't preserve timestamps, so autotools make think stuff needs to be rebuilt that doesn't.  If you've actually changed `configure.ac` or one of the `Makefile.am` files, make sure that you've done the necessary `autoreconf` steps, otherwise this will probably just break everything.

```
cd /global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev/FASTDB
touch aclocal.m4 configure
find . -name Makefile.am -exec touch \{\} \;
find . -name Makefile.in -exec touch \{\} \;
./configure \
  --with-installdir=/global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev/install \
  --with-smtp-server=smtp.lbl.gov \
  --with-smtp-port=25 \
  --with-email-from=raknop@lbl.gov
make install
```


### Building for tests

If you've edited `configure.ac` or any of the `Makefile.am` files, run
```
autoreconf --install
```

Then, run

```
./configure --with-installdir=$PWD/install \
            --with-smtp-server=mailhog \
            --with-smtp-port=1025
make install
```

Should put lots of stuff underneath `install`.


            