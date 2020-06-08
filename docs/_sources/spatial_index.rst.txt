DBMS-Agnostic Spatial Indexing
==============================

B-Tree
------
There is a standard technique all DBMSs use to speed up queries.  If you have a constraint
like::

    where ra between 214. and 215. and dec between 34. and 35.

the DMBS checks to see if it has an "index" on either ra or dec.  The index itself is
based on a sorted list of the parameter values; going through this list in order between
the low and high value allows the database to jump from record to record in the table,
therefore only having to do two I/Os for every record touched (*i.e.,* the index and
the table record).  If this is a small fraction of the table total, the overall time
is much small than reading through the whole table (the alternative).

Only one of the constraints can be used for any given query (we can only step through
the records in one order).  The DBM also contains a histogram of the values for each
indexed columns and estimates up front which constraint will involve the smaller 
number of I/Os.  In the same way, it can determine whether the index chosen is worth
the effort.  For instance, a range that would touch half the records would actually
take longer than "scanning" just the data records.

This technology is referred to as a "B+-Tree".

There are a number of practical consequences of this approach.  Some columns have values
that are distributed in ways that make indexing ineffective.  Index too many columns and
you waste too much time deciding on the right index.  Indices on columns no one ever 
constrains are pointless.


Composite Indices
-----------------
DBMSs also support "composite" indices but these are of limited use.  Essentially, if 
you can make a sorted list on one parameter and this ends up collecting records into
groups where all the records have the same value for that parameter, you can the sort
inside the group by a second parameter.  And if the nested sorting helps in queries
then a composite index is useful.

Our primary goal here is to speed up spatial queries and for position parameters in
astronomical tables neither of the above conditions is satisfied:  If you sort on 
RA first you don't get large groups of equal RA so then sorting on Dec gets you nowhere
and even if the RAs did group, a composite index of this sort doesn't help with 
region queries.


Tesselation
-----------
Somewhat surprisingly, there is a technique that allows the basic B-Tree index to 
speed up 2D spatial queries.  It involves subdividing the sky into little regions
(*e.g.,* RA/Dec boxes though that particular "tesselation" has a bunch of problems)
and assigning a single integer ID to each region in such a way that IDs that are 
close together numerically have a strong tendency to be close together on the sky.

This is accomplished using a Z-ordered curve (https://en.wikipedia.org/wiki/Z-order_curve).
There are different "tesselations" of the sky (our library supports two) but the
choice has little effect on query speed.  More important is how "deep" you go in
the heirarchical subdivision though even this is a secondary effect.

Basically, for each record you identify a cell ID and save this in the database 
record.   All coordinates inside the same cell will get the same ID.  The ID column is 
indexed using the DBMS built-in B-Tree functionality.

Then later when processing search request that asks for all the records in a cone on the 
sky, you determine which cell IDs overlap the region (in whole or in part).  A further 
refinement is to turn this list into a set of ID ranges.

Sending this constraint on ID to the DBMS, you would get back all the records in all 
those cells, a superset of the data you want.  But if you also store in the database
columns giving the 3-vector coordinates of each point you can augment the query 
with a constraint that weeds out just the records matching the exact geometry.

Here's a concrete example.  Tesselating the sky using HTM cells (more on that later),
we can turn a constraint like::

    within 1 degree of (293.5607117,-23.1312775) 

into the following::

   where ((0.367585916751*x)+(-0.842946005983*y)+(-0.392839184405*z)>=9.998476951564e-01) 
         AND (   (htm20 BETWEEN 12301323534336 AND 12301390315519)
              OR (htm20 BETWEEN 12301457686528 AND 12301591642111) 
              OR (htm20 BETWEEN 12303202516992 AND 12303336472575) 
              OR (htm20 BETWEEN 12303411970048 AND 12303470133123) 
              OR (htm20 BETWEEN 12303604908032 AND 12303621685247) 
              OR (htm20 BETWEEN 12303739125760 AND 12303755902975) 
              OR (htm20 BETWEEN 12303776874496 AND 12303805452032) 
              OR (htm20 BETWEEN 12304074670080 AND 12304103830527) 
              OR (htm20 BETWEEN 12304130244608 AND 12304138559487) 
              OR (htm20 BETWEEN 12304276258816 AND 12304544432127) 
              OR (htm20 BETWEEN 12304814964736 AND 12304879861755) 
              OR (htm20 BETWEEN 12304947085312 AND 12305047748607) 
              OR (htm20 BETWEEN 12305065050112 AND 12305079143739))

So the DBMS does all the work.  This query may look long-winded but it is no problem
for the DBMS and quite fast. 


HTM and HPX
-----------
As we said, the choice of tessselation scheme doesn't matter too much.  In fact,
we have done a study for NASA that demonstrates this.  But since we have the 
capability (from that study) we have include two in our software suite.

Heirarchical Triangular Mesh (HTM) subdivides the sky into triangles.  The base
level is the set of eight triangles defined by the poles and the cardinal points
around the equator.  The nesting of the levels is done by finding the midpoints
of the upper triangle sides and connecting them.  This subdivides the triangle
into four roughly equal parts; three containing the original corners and one
in the center.

HPX is base on the HEALPix tesselation.  HEALPix was originally developed to 
support numerical integration on the sphere and has a number of interesting 
characteristics.  The HEALPix cells are diamonds on the sky and get divided into
four in such a way as to maintain equal area in each sub-cell.  This is important
for the original use but doesn't matter for database indexing.


Optimal Tesselation
-------------------
There is no best tesselation and the "best" tesselation depth depends on the 
set of queries that are most commonly sent to your DBMS.  Level 20 depth (as
we used above) results in ~arcsecond cells and level 7 is around half a degree.
In test we ran of the full range of cell sizes and a with cone searches on
the sky ranging from arcsecond to a few degrees, uniformly distributed 
in log(radius), the optimum was somewhere in the center (around level 14).

See the next section for information on software tools to help populate the
x, y, z and spatial-index columns.
