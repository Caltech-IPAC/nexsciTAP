Installation Instructions
=========================

Beforehand
----------
Python packages can specify that they depend on and the installer
will recursively install all of these as well.  For our nexsciTAP
package you will want whatever DB API 2.0 goes along with your DBMS
(*e.g.* cx_Oracle for Oracle, sqlite for SQLite3, *etc.*).  We can't 
know which DBMS you have or which adapter you want to use.  So we leave
it up to you to "pip install" the correct one separately.

If you forget, your Python will inform you what is missing when you 
try to run the code.


Quickstart
----------
To just run instances of the NExScI TAP service, it is not necessary to
download the source or build anything.

In fact, if you are using a web server (like Apache or NGINX) that supports 
CGI programs, you can just do the following::

    pip install nexsciTAP

then create a Python executable in your "/cgi-bin" directory tree (wherever
you have that set up (we use .../cgi-bin/TAP/nph-tap.py under Apache)::

    /bin/env python

    from TAP import tap

    tap.main()

You now have an operational TAP service (that can't do anything).  

Before it can do its job, you need to give it a bunch of configuration
information, including:

- Where to put working files,

- How to connect to your DBMS, and

- how you have set up your spatial indexing (if you have).

This is covered in the next section.  


Not-so-Quick Start
------------------
Rather than pip install nexsciTAP (which pulls along a couple of other 
packages), you can build them yourself from source in GitHub.

The three GitHub packages you will need are:

- `nexsciTAP`_,
- `ADQL`_, and
- `SpatialIndex`_


Once you have these downloaded, you can run the standard::

   python setup.py bdist_wheel

command for each package and then "pip install" the wheel file this builds in 
the ./dist subdirectory.  This gets us to the same place as the "pip install" in the
Quickstart.  We still have to add the same CGI program and configuration file.


.. _nexsciTAP:    https://github.com/Caltech-IPAC/nexsciTAP
.. _ADQL:         https://github.com/Caltech-IPAC/ADQL
.. _SpatialIndex: https://github.com/Caltech-IPAC/SpatialIndex
