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

.. _ltcv-gethottransients:

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

This is the endpoint to query if you want to figure out which specific objects have had spectra requested.  You would use this if you've got access to a spectroscopic instrument, and you want to know what spectra are most useful to DESC.  This will *only* find spectra where somebody has requested it using ``spectrum/askforspectrum``; if what you're after is any active transient, then you want to use :ref:`ltcv/gethottransients <ltcv-gethottansients>` instead.

POST to the endpoint with dictionary in a JSON payload.  This may be an empty dictionary ``{}``; the following optional keys may be included:

* ``requested_since`` : string in the format ``YYYY-MM-DD`` or ``YYYY-MM-DD hh:mm:ss``; only find spectra that were requested since this time.  (This is so you can filter out old requests.)  You will usually want to specify this.  If you don't, it will give you anything that anybody has asked for ever.

* ``requester`` : string; if given, only get spectra requested by a specific requester.  If not given, get all spectra requested by everybody.
  
* ``not_claimed_in_last_days`` : int; only return spectra where nobody else has indicated a intention to take this spectrum.  Use this to coordinate between facilities, so that multiple facilities don't all get the same spectra.  This defaults to 7 if not specified.  If you don't want to consider whether anybody else has said they're going to take a spectrum, explicitly pass ``None`` for this value.

* ``no_spectra_in_last_days``: int; only return objects that have not had spectrum information reported in this many days.  This is also for coordination.  If you don't want to consider just what is planned, but what somebody actually claims to have observed, then use this.  If not given, it defaults to 7.  (This may be combined with ``not_claimed_in_last_days``.  It's entirely possible that people will report spectra that they have not claimed.)  To disable consideration of existing spectra, as with ``not_claimed_in_last_days`` set this parameter to ``None``.
  
* ``procver`` : string; the processing version to look at when finding photometry.  It will also filter out objects which are not defined in this procesing version.  If not given, will consider all data from all processing versions.  This is probably actually OK, because we're unlikely to have multiple processing versions of real-time data from the last couple of weeks.  However, to be safe, you might want to use [ROB FIGURE OUT THE PROCESSING VERSION ALIAS WE'RE GOING TO USE FOR REAL TIME DATA].

* ``detected_since_mjd`` : float.  Only return objects that have been *detected* (i.e. found as a source in DIA scanning) by Rubin since this MJD.  Be aware that an object may not have been detected in the last few days simply because it's field hasn't been observed!  If not passed, then the server will use ``detected_in_last_days`` (below) instead.  Pass ``None`` to explicilty disable consideration of recent detections.

* ``detected_in_last_days``: float.  Only return objects that have been *detected* within this may previous days by LSST DIA.  Ignored if ``detected_since_mjd`` is specified.  If neither this nor ``detected_since_mjd`` is given, defaults to 14.

* ``lim_mag`` : float; a limiting magnitude; make sure that the last measurement or detection was at most this magnitude.

* ``lim_mag_band`` : str; one of u, g, r, i, z, or Y.  The band of ``lim_mag``.  If not given, will just look at the latest observation without regard to band.
  
* ``mjd_now`` : float; pretend that the current MJD is this date.  Normally, the server will use the current time, and normally this is what you want.  This parameter is here for testing purposes.  All database queries will cut off things that are later in time than this time.
  
You will get back a ROB DOCUMENT THIS.

``spectrum/planspectrum``
*************************

Use this to declare your intent to take a spectrum.  This is here so that multiple observatories can coordinate.  ``spectrum/spectrawanted`` (see above) is able to filter out things that have a planned spectrum.

POST to the dictionary with a JSON payload.  Required keys are:

* ``oid``: string UUID; the object ID of the object you're going to take a spectrum of.  These UUIDs are returned by ``ltcv/gethottransients``.
* ``facility``: string; the name of the telescope or facility where you will take the spectrm.
* ``plantime``: string ``YYYY-MM-DD`` or ``YYYY-MM-DD HH:MM:SS``; when you expect to actuallyobtain the spectrum.

You may also include one optional key:

* ``comment``: string, any notes bout your planned spectrum.

