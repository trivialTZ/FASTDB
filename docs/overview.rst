.. contents::

FASTDB Overview
===============


FASTDB runs with two database backends, a PostgreSQL server and a Mongodb server.  Neither database server is directly accessible; rather, you access FASTDB through a webserver.  As of this writing, only one instance of FASTDB exists at `https://fastdb-rknop-dev.lbl.gov <https://fastdb-rknop-dev>`_; that is Rob's development server, so its state is always subject to radical change.  However, you may create your own instance of FASTDB on your own machine; see :doc:`developers`.

While there will be an interactive UI on the webserver, the primary way you connect to FASTDB is using the web API.  For more information, see :doc:`usage`.  To simplify this, there is a :ref:`python client <the-fastdb-client>` that handles logging in and sending requests to the web server.

To use a FASTDB instance, you must have an account on it.  At the moment, Rob is not setting up general users on any of the test installs, but hopefully that will change relatively soon.

Contact Rob to ask for an account; he will need the username you want, and the email you want associated with it.  When first created, your account will no thave a password.  Point your web browser at the webserver's URL, and you will see an option to request a password reset link.
