# FASTDB
Development of the Fast Access to Survey Transients Database (FASTDB) 


## Rob Notes

This needs to be moved somewhere better.

### Building for tests

```
./configure --with-webapdir=$PWD/install/webap \
            --with-smtp-server=mailhog \
            --with-smtp-port=1025
make install
```

Should put lots of stuff underneath `install/webap`.


            