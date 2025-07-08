.. _developers-docs:
.. contents::

==========================
Information for Developers
==========================

This documentation is for people who want to install a test version of FASTDB on their local machine, edit the FASTDB code, or try to install FASTDB somewhere else.  (It is currently woefully incomplete for the last purpose.)


   
Submodules
==========

FASTDB uses at least one submodule. These are checked out in the ``extern`` subdirectory underneath the top-level of the checkout.  When first checking out the repository, things will not fully work unless you run::

  git submodule update --init

That command will check the appropriate commit of all needed submodules.

If later you pull a new revision, ``git status`` may show your submodule as modified, if somebody else has bumped the submodule to a newer verion.  In that case, just run::

  git submodule update

to get the current version of all submodules.


.. _installing-the-code:

Installing the Code
===================

(If you're reading this documentation for the first time, don't try to do what's in this section directly.  Rather, read on.  You will want to refer back to this section later.  First, though, you will probably want to do everything below about setting up a test environment.)

The code (for the most part) is not designed to be run out of the ``src`` directory where it exists, though you may be able to get that to work.  Ideally, you should install the code first.  This requires you to have things installed on your system which are *not* available inside the docker container; specifically, you will need to have GNU Autotools installed.  On Linux, this is usually a simple matter of installing one or more packages.  (On Debian and close derivatives, the packages are probably called things like ``autoconf``, ``automake``, and ``autotools-dev``.)  On NERSC's Perlmutter, these should already be available to you by default.

To install the code, you run two commands::

  ./configure --with-installdir=[DIR] --with-smtp-server=[SERVER] --with-smpt-port=[PORT]
  make install

The first ``[DIR]`` is the directory where you want to install the code.  The SMTP server setup requires you to know what you're doing.  (However, see :ref:`installing-for-tests`, including :ref:`unpacking-test-data`, below if you're just trying to get things working inside the docker compose environment.)  You can run::

  ./configure --help

as usual with GNU autotools to see what other options are available.  If you're making a production install of FASTDB somewhere, you will definitely want to do things like configure the database connection.

It's possible that after running the first command, you'll get errors about ``aclocal-1.16 is missing on your system`` or something similar.  There are two possibilites; one is that you do legimiately need to rebuild the autotools file, in which case see :ref:`autoreconf-install` below.  If you haven't, it may ba result of an unfortunate interaction between autotools and git; autotools (at least some versions) looks at timestamps, but git checkouts do not restore timestamps of files committed to the archive.  In this case, you can run::

  touch aclocal.m4 configure
  find . -name Makefile.am -exec touch \{\} \;
  find . -name Makefile.in -exec touch \{\} \;

and then retry the ``./configure`` command above.


Local Test Environment
=======================

The file ``docker-compose.yaml`` in the top-level directory contains (almost) everything necessary to bring up a test FASTDB environment on your local machine.  Make sure you have the docker container runtime and the docker compose extensions installed, make sure that your current working directory is the top-level directory of your git checkout, and run::

  docker compose build

Assuming no errors, you should now have built all of the docker images necessary to run the environment.  The first time you run this, it will take a while (several minutes at least).  Subsequent runs will be faster, unless something early in the docker file itself has changed (which does sometimes happen).  If you run it immediately again after you just ran it, it should only take several seconds for it to figure out that everything is up to date.

Once you've successfully built the docker environments, run::

  docker compose up -d webap
  docker compose up -d shell

(For those of you who know docker compose and are wondering why ``webap`` is not just a prerequisite for ``shell``, the reason is so one can get a debug environment up even when code errors prevent the web application from successfully starting.)

When you run these two commands, it will start a number of local servers on your machine, and will set up all the basic database tables.  Assuming you're running these commands on the same machine you're sitting at (i.e. you're running them on your laptop or desktop, not on a remote server you've connected to), and that everything worked, then after this you should be able to connect to the FASTDB web application with your browser by going to:

   ``http://localhost:8080``

(You can change the port on your local machine from ``8080`` to something else by setting the ``WEBPORT`` environment variable before running ``docker compose``.)  This will give you the interactive web pages; however, the same URL can be used for API calls documented on :ref:`Using FASTDB <usage-docs>`.  Right after bringing it up, you won't be able to do much with it, because there are no FASTDB users configured.  See :ref:`creating-a-persistent-test-user` below.

The servers that get started by ``docker compose`` are, as of this writing:

  * A ``kafka`` zookeeper and a ``kafka`` server.  (TODO: use ``kraft`` so we don't need the zookeeper any more.)
  * A ``postgresql`` server
  * A ``mongodb`` server
  * A "query runner", which is a custom process that handles the "long query" interface
  * A web server that is the FASTDB front end
  * A shell server to which you can connect and run things.

You may ``docker compose`` telling you that more than this was started.  There are some transitory servers, e.g. ``createdb``, that start, do their thing, and then stop.

Ideally, at this point you're done setting up your test/dev environment.  When you're finished with it, and want to clean up after yourself, just run::

  docker compose down -v

That will remove all of the started servers, and wipe out all disk space allocated for databases and such.
  
It's possible the shell server won't start, usually because the ``createdb`` step failed.  The first thing you should do is::

  docker compose logs createdb

to see if there's an obvious error message you know how to fix.  Failing that, you can run::

  docker compose up -d shell-nocreatedb

That will bring up a shell server you can connect to and work with that will have the Postgres and Mongo servers running, but which will (probably) not have the tables created on the Postgres server.  (It's also possible other steps will fail, in which more work may potentially be required.)

Please Don't Docker Push
------------------------

The `docker-compose.yaml` file will build docker images set up so that they can easily be pushed to Perlmutter's container image registrly.  Please do *not* run any docker push commands to push those images, unless you've tagged them differently and know what you're doing.  (If you really know what you're doing, you're always allowed to do *anything*.)


Architecture Note
-----------------

FASTDB is developed on, and expected to be run on, a Linux system running the ``x86_64`` architecture.  If you're on a different system (either OS or CPU architecture (e.g. ``ARM``)), it's possible you will have trouble building the Docker images, as some things needed may not be available for your system.


Working With the Test Installation
==================================

Assuming everything in the previous step worked, you can run, from the top level of the git checkout::

  docker compose exec -it shell /bin/bash

That will connect you to the shell container.  (You can tell you're inside the container because your prompt will start with "``I have no name!@``".)

If you want to run the tests in the ``tests`` subdirectory, you will first need to install the code to where it's expected; see :ref:`installing-the-code`.  Once you're ready, inside the container go to the ``/code/tests`` directory and run various tests with ``pytest``.  If you just run ``pytest -v``, it will try to run all of them, but you can, as usual with pytest, give it just the file (or just the file and test) you want to run.


.. _installing-for-tests:

Installing for tests
--------------------

:ref:`installing-the-code` above describes the general procedure for installing the code.  If you want to install the code on your local test enviroment for use with the tests in the docker compose environment, then you need to run, inside the container and from the top level of the git checkout::

  ./configure --with-installdir=$PWD/install \
              --with-smtp-server=mailhog \
              --with-smtp-port=1025
  make install

.. _autoreconf-install:

If you've modified the base autotools files
-------------------------------------------

Usually, the ``./configure`` and ``make`` commands in the previous section are sufficient for installing the tests.  However, if you've modified ``configure.ac`` in the top level directory, or ``Makefile.am`` in any directory, then you need to rerun autotools to build all the derivative Makefiles.  This is just a matter of running::

  autoreconf --install

before the ``./configure`` step above.

.. _unpacking-test-data:

Unpacking test data
-------------------

The tests will not yet run as-is.  Inside the ``tests`` subdirectory, you must run::

  bzip2 -d elasticc2_test_data.tar.bz2

in order create the expected test data on your local machine.  Note that ``bzip2`` is *not* installed inside the docker container, so you need to run this on your host machine.  You only need to do this once in your checkout; you do *not* have to do this every time you create a new set of docker containers.  (If the subdirectory ``tests/elasticc2_test_data`` has stuff in it, then you've probably already done this.)

Exiting the test environment
----------------------------

If you're inside the container, you can exit with ``exit`` (just like any other shell).  Once outside the container, assuming you're still in the ``tests`` subdirectory, you re-enter the (still-running) test container with another ``docker compose exec -it shell /bin/bash``.  If you want to tear down the test enviornment, run::

  docker compose down -v

This will completely tear down the environment.  All containers will be stopped, all volumes created for the environment (such as the backend storage for the test databases) will be wiped clean.  This is what you do if you want to make sure you're starting fresh.
  


Running the tests
-----------------

Once inside the container, cd into the ``tests`` directory (if you're not there already) and run::

  pytest -v

that will run all of the tests and tell you how they're doing.  As usually with ``pytest``, you can give filenames (and functions or classes/methods within those files) to just run some tests.

**WARNING**: it's possible the tests do not currently clean up after themselves (especially if some tests fail), so you may need to restart your environment after running tests before running them again.  If you hit ``CTRL-C`` while ``pytest`` is running, tests will almost certainly not have cleaned up after themselves.

What's more, right now, if you're running all of the tests, if an early test fails, it can cause a later test to fail, even though that later test wouldn't actually fail if the earlier tests had passed.  This is bad behvaior; if tests properly cleaned up after themselves (which they're supposed to do even if they fail), then the later tests shouldn't fail just because an earlier one does.  Until we get this behavior fixed, when looking at lots of tests at once, work on them in order, as the later tests might not "really" have failed.

Directly accessing the database
-------------------------------

If you want to directly access the database inside the test environment, inside the container run::

  psql -h postgres -U postgres fastdb

It will prompt you for a password, which is "fragile".  (This is a test environment local to your machine; never install a production environment with a password like that!)  You can now issue SQL commands, and do anything you might normally do with PostgreSQL using ``psql``.

TODO : instructions for accessing the mongo database.


.. _creating-a-persistent-test-user:


Creating a persistent test user
-------------------------------

TODO


Loading persistent test data
----------------------------

TODO



Note for Rob: Installing on Perlmutter
======================================

rknop_dev environment
---------------------

The base installation directory is::

  /global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev

In that directory, make sure there are subdirectories ``install``, ``query_results``, and ``sessions``, in additon to the ``FASTDB`` checkout generated with::

  git clone git@github.com::LSSTDESC/FASTDB
  cd FASTDB
  git checkout <version>
  git submodule update --init

The ``.yaml`` files defining the Spin workloads are in ``admin/spin/rknop_dev`` in the git archive.  (Note that, unless I've screwed up, the files ``secrets.yaml`` and ``webserver-cert.yaml`` will not be complete, because those are the kinds of things you don't want to commit to a public git archive.  Edit those files to put in the actual passwords and SSL key/certificates before using them, and **make sure to remove the secret stuff before committing anything to git**.)  To install the code to work with those ``.yaml`` files, run::

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

This is necessary because the docker image for the web ap does *not* have the fastdb code baked into it.  Rather, it bind mounds the ``install`` directory and uses the code there.  (This allows development without having to rebuild the docker image.)
