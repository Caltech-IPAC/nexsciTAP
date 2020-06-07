Installation Instructions
=========================

Quickstart
----------
To just run instances of the NExScI TAP service, it is not necessary to
download the source or build anything.

In fact, if you are using a web server (like Apache or NGINX) that supports 
CGI programs, you can just do the following::

    pip install nexsciTAP

the create a Python executable in you "/cgi-bin" directory tree (whereever
you have that set up (we use .../cgi-bin/TAP/nph-tap.py under Apache)::

    /bin/env python

    from TAP import tap

    tap.main()

You now have an operational TAP service (that won't work).  

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

command and then "pip install" the wheel file in the ./dist subdirectory
for each package.  This gets us to the same place as the "pip install" in the
Quickstart.  We still have to add the same CGI program and configuration file.


.. _nexsciTAP:    https://github.com/Caltech-IPAC/nexsciTAP
.. _ADQL:         https://github.com/Caltech-IPAC/ADQL
.. _SpatialIndex: https://github.com/Caltech-IPAC/SpatialIndex
