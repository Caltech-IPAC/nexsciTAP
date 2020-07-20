Notes on TAP Clients
====================

While it is possible to interact with the nexsciTAP service (or any other TAP service)
"directly" (using a browser, wget, *etc.*) most users will probably use one of a small
set of existing tools or toolkits.  These tools layer their own behavior on top of
nexsciTAP, affecting what the user sees.  Here we present a few notes on the effects
of these various services.  We don't consider any of these behaviors "errors", just
things to be aware of as you use the tools.

Nor is this list comprehensive; these are just things we have encountered in our own use
and testing.  As we encounter other behavior or these packages change, we will update this
page accordingly.

**TAPPlus**.  A part of the Python Astroquery package, TAPPlus is a general library for
accessing TAP services. In general TAPPlus works very well but in synchronous mode there
is a hard-coded limit on the return of 2000 records.  There is no way to override this
without changing the TAPPlus code itself.

Some of the archives using nexsciTAP (specifically KOA and NEID) handle access to proprietary
data using a security cookie.  TAPPlus does not have any facility for including cookie file
use in the interaction. This isn't an error (the TAP specification does not address cookies)
but it does limit the usefulness of TAPPlus as a client for such archives.


**PyVO**.  A part of the international Virtual Observatory effort.  PyVO includes another
general Python TAP client.  It also does not support cookie passing, though it is a simpler
hack to add this.


**TopCat**.  TopCat is an interactive graphical viewer and editor for tabular data.  It 
includes a TAP client as one way to access input data.  While nexsciTAP tries to provide
facilities for properly formatting output data, DBMSs in general do this poorly.  TopCat
therefore includes its own formatting decisions which may sometime override what 
nexsciTAP has done.
