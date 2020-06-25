
A Note on Coordinates
=====================
The ADQL specification (or more accurately the Space-Time Coordinate 
Metadata (STC) specification) enumerates over forty coordinate systems,
including variant frames of reference.  We limit ourselves here to
a more basic set (ICRS, FK4, FK5, Galactic and Ecliptic), essentially
the values supported by packages like Astropy.

If the coordinate system is left as a blank string in functions like
POINT() ane POLYGON(), we default to 'ICRS'.

Moreover, since our main purpose is spatial searching, we will consider
ICRS, FK5 and 'J2000' to be identical (ICRS and FK5 differ in practice
by less than a tenth of an arcsecond).

Finally, our spatial index tesselation could be done in any coordinate
system but there is no practical purpose in allowing this, so we 
constrain the indexing to be in ICRS/FK5 (*e.g.,* POINT('icrs', ra, dec)).

However, reference locations are allowed to be in any of the above 
systems (*e.g.,* CIRCLE('galactic', 180. 0., 2.5)).
