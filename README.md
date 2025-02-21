# FASTDB
Development of the Fast Access to Survey Transients Database (FASTDB).


## Rob Notes

This README file needs to be orgnized better.

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


            