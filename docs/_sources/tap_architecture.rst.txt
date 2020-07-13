Behind the Scenes: How the TAP Service Works
============================================

Once you get past issues dealing with databases, ADQL and the like, TAP itself is fairly straightforward.
There are multiple paths through the processing, so that again can look complicated but once you 
understand the basics those variants fall into place.

The simplest TAP query is a "synchronous" request, where you hand the service an ADQL statement,
it gets processed, and the result table streams back as the response.  But it is easier to 
understand the processing by examining an "asynchronous" request, then seeing the synchronous 
as a special case.

Asynchronous TAP Requests
-------------------------
The user first contacts the TAP service and hands it an ADQL query.  
This is done through a URL like::

    https://exoplanetarchive.ipac.caltech.edu/TAP/async?query=select+pl_name,ra,dec+from+ps

The service creates a workspace (with a random name) and a status.xml file in it containing
information on the query and the state of the processing::

    <uws:job xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 http://www.ivoa.net.xml/UWS/v1.0">
       <uws:jobId>tap_4pxj0j5c</uws:jobId>
       <uws:processId>10709</uws:processId>
       <uws:ownerId xsi:nil="true"/>
       <uws:phase>PENDING</uws:phase>
       <uws:quote xsi:nil="true"/>
       <uws:startTime/>
       <uws:endTime/>
       <uws:executionDuration>0</uws:executionDuration>
       <uws:destruction/>
       <uws:parameters>
          <uws:parameter id="format">votable</uws:parameter>
          <uws:parameter id="lang">ADQL</uws:parameter>
          <uws:parameter id="maxrec">-1</uws:parameter>
          <uws:parameter id="query"> select pl_name,ra,dec from ps </uws:parameter>
       </uws:parameters>
    </uws:job>

It returns the job ID string (here "tap_4pxj0j5c") and exits.  The job ID can be used to 
retrieve the status and in requests to change some aspects of the status or do further
processing.

The rest of the processing involves interacting with this job.  You can query the status
(including retrieving the whole status structure) but the obvious next step is to actually
start the query running.  The TAP specification requires that this be done through an 
HTTP POST request but we support HTTP GET as well::

    https://exoplanetarchive.ipac.caltech.edu/TAP/async/tap_4pxj0j5c/phase?phase=RUN

This also returns immediately; the "phase" in the status XML is changed to "EXECUTING" 
and a background process is started that runs the query.  When this process completes,
the result data is written to the workspace, the status phase is updated to "COMPLETED"
and a "results" section is added to the status::

    <uws:job xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 http://www.ivoa.net.xml/UWS/v1.0">
    <uws:jobId>tap_4pxj0j5c</uws:jobId>
    <uws:processId>13957</uws:processId>
    <uws:ownerId xsi:nil="true"/>
    <uws:phase>COMPLETED</uws:phase>
    <uws:quote xsi:nil="true"/>
    <uws:startTime>2020-06-06T08:33:10.76</uws:startTime>
    <uws:endTime>2020-06-06T08:33:39.28</uws:endTime>
    <uws:executionDuration>28.5</uws:executionDuration>
    <uws:destruction>2020-06-10T08:33:10.76</uws:destruction>
    <uws:parameters>
       <uws:parameter id="format">votable</uws:parameter>
       <uws:parameter id="lang">ADQL</uws:parameter>
       <uws:parameter id="maxrec">-1</uws:parameter>
       <uws:parameter id="query"> select pl_name,ra,dec from ps </uws:parameter>
    </uws:parameters>
    <uws:results>
       <uws:result id="result" xlink:type="simple" xlink:href="https://exoplanetarchive.ipac.caltech.edu:443/workspace/TAP/tap_4pxj0j5c/result.xml"/>
    </uws:results>
    </uws:job>

But we can't know this without asking.  So after submitting the RUN request we have to
poll the phase information (or the whole status) until it is COMPLETED (or errors off)::

    https://exoplanetarchive.ipac.caltech.edu/TAP/async/tap_4pxj0j5c/phase

The result link::

    https://exoplanetarchive.ipac.caltech.edu:443/workspace/TAP/tap_4pxj0j5c/result.xml

returns the final data.


Synchronous TAP Requests
------------------------
Blocking ("synchronous") requests simply shortcut much of the preceeding. We still maintain
all the same information in the workspace but the query starts running immediately and the
original web connection stays up until the results are available and streamed back.  

Obviously, this is much easier on the user but there is a big "but".

Simple HTTP requests time out, usually at somewhere aroung five minutes.  Database queries 
can literally last for days if you are doing something complex.  So unless you can be sure
your query will finish quickly, it is better to run asynchronously.

The example we have been using here is a query to the Exoplanet Archive for a list of 
planets with names and sky coordinates.  This table currently has a few thousand records
so in fact synchronous queries work fine.


Refinements
-----------
There are a variety of additional things you can do to an asynchronous query.  Before
it starts running you can adjust the maximum number of return records through the maxrec
parameter (this is different from including a TOP directive in the ADQL; that is handled
by the DBMS).  Likewise, you can adjust the maximum allowable execution duration.
While it is running, you can kill it by setting the phase to ABORT.  Refer to the
TAP spec for details.


Clients
-------
As you can see, it is perfectly possible to interact with TAP "manually" using either
a browser or WGET/CURL scripts.  However, there is enough stuff to keep track of, especially
in the asynchronous case with polling, that client support software is advisable.  

In Python, there are multiple options, notably Astroquery/TAPPlus and PyVO.  However,
none of these is (so far) perfect so be sure to test you use case thoroughly.
