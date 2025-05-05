.. _usage-docs:
.. contents::

============
Using FASTDB
============

This documentation is for people who want to *use* FASTDB.  There is a FASTDB server installed somewhere that you wish to connect to in order to pull date from or push data to.  Since FASTDB is currently under heavy development, there is no global production server.  As such, if you are working with an instance of FASTDB for your own development, probably Rob set that up for you and you already know where it is.  Alternatively, you might set up a local test environment (see :ref:`developers-docs`) to use to develop code on your own machine.

Access to FASTDB is designed to be entirely through a web API.  By design, the underlying PostgreSQL and MongoDB servers cannot be connected to directly.  (There are a variety of reasons for this; talk to Rob if you're interested.)

.. _the-fastdb-client:

The FASTDB Client
=================

While you can access the FASTDB web API using any standard way of accessing web APIs (e.g. the python ``requests`` module), there is a FASTDB client designed to make this a little bit easier.

Getting Set Up to Use the FASDTB Client
----------------------------------------

The FASDTB client is entirely contained in the file ``client/fastdb_client.py`` in the github checkout.  You can just refer to this directly in your checkout by adding something to your `PYTHONPATH`, or you can copy it somewhere.  (**Warning**: if you copy it somewhere, then be aware that eventually stuff might break as your copied version falls out of date!)

The `fastdb_client.py` requires some python modules that are always installed in various environments.  The specific packages required that may not be included in base python installs are:

  * ``requests`` (though this is very often included in python installs)
  * ``pycryptodome``

Both of these are easily installable in virtual environments with ``pip``.  It's possible if you're on a Linux machine (or if you're using something like Macports) that you will be able to find them in your system's packager manager.  (On Debian and close derivatives, the packages are ``python3-requests`` and ``python3-pycryptodome``.) ``pycryptodome`` includes libraries used for the user authentication to FASTDB, for more information see [Rob put in a link if you ever describe the internal details of the user authentication system].

On NERSC Perlmutter
********************

On NERSC Perlmutter, you can find a "current" version designed to work with at least some installs of FASTDB at::

  /dvs_ro/cfs/cdirs/desc-td/SOFTWARE/fastdb_deployment/fastdb_client

We recommend that you add this to your ``PYTHONPATH``, or set up an alias, but do *not* copy the `fastdb_client.py` file out of that directory.  That way, later, if it is updated, you will automatically get the updated version.

As of this writing, the ``desc_td`` enviornment on Perlmutter does not include the needed python packages describe above (see `Issue #108 <https://github.com/LSSTDESC/td_env/issues/108>`_.  To get ``fastdb_client`` to work on Perlmutter, you can go into an enviornment created specifically for FASTDB with::

  source /dvs_ro/cfs/desc-td/SOFTWARE/fastdb_deployment/fastdb_client_venv/bin/activate

(Then later run just ``deactivate`` to leave this environment.)  If you're setting yourself up on another machine,

Using the Client
----------------

The client is currently documented in `this Jupyter notebook <https://github.com/LSSTDESC/FASTDB/blob/main/examples/using_fastdb_client.ipynb>`_.  The same notebook may of course be found in the ``examples`` subdirectory of a FASDTB github checkout.  This notebook also includes some examples of using API endpoints described below in :ref:`The Web API`.


Interactive Web Pages
======================

TODO


The Web API
===========

Direct SQL Queries
------------------

The FASDTB web interface includes a front-end for direct read-only SQL queries to the backend PostgreSQL database.  (Note that "read-only" means that you can't commit changes to the database.  You *can* use temporary tables with this interface, and that is often a very useful thing to do.)

TODO document this.  In the mean time, see the `examples FASDTB client Juypyter notebook <https://github.com/LSSTDESC/FASTDB/blob/main/examples/using_fastdb_client.ipynb>`_ for documentation on this interface.

Lightcurve Endpoints
--------------------

``ltcv/gethottransients``
*************************

TODO


Spectrum Endpoints
------------------

``spectrum/askforspectrum``
***************************

TODO

``spectrum/spectrawanted``
**************************

TODO

