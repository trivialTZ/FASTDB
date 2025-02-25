# FASTDB
Development of the Fast Access to Survey Transients Database (FASTDB).

* [Overview](#overview)
* [Using the FASTDB client](#using-the-fastdb-client)
* [Rob Notes](#rob-notes)
  * [Installing fastdb_rknop_dev)(#installing-fastdb_rknop_dev)
  * [Building for tests](#building for tests)

## Overview

FASTDB runs with two database backends, a PostgreSQL server and a Mongodb server.  Neither database server is directly accessible; rather, you access FASTDB through a webserver.  As of this writing, only one instance of FASTDB exists at https://fastdb-rknop-dev exists; that is Rob's development server, so it's state is always subject to radical change.

While there will be an interactive UI on the webserver, the primary way you connect to FASTDB is using the web API.  To simplify this, there is a [python client library](#using-the-fastdb-client) that handles logging in and sending requests to the web server.  As of this writing, the only web API endpoints defined are ones that allow you to send raw SQL to the PostgreSQL web server.  (It's a readonly connection, so you can only read the database, not modify it.)

To use a FASTDB instance, you must have an account on it.  Contact Rob to ask for an account; he will need the username you want, and the email you want associated with it.  When first created, your account will no thave a password.  Point your web browser at the webserver's URL, and you will see an option to request a password reset link.


## Using the FASTDB Client

See the instructions and example at <a href="examples/using_fastdb_client.ipynb">using_fastdb_client.ipynb</a>. (That's a Jupyter notebook that you can copy and try running yourself.)

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


            