Converting ADQL to Local SQL
============================

SQL Variations Between DBMSs
----------------------------
Like most standards, SQL left enough ambiguity and there were enough holes
that existing implementations vary enough to be noticeable.  In defining the
Astronomical Data Query Language (ADQL) the VO community picked specific solutions
for some of this.

An example of this is the mechanism for cutting off the number of returned records.
Different systems use the following to achieve this effect::

    select * from mytbl LIMIT 100

    select TOP 100 * from mytbl

    select FIRST 100 * from mytbl

    select * from mytbl WHERE ROWNUM <= 100

ADQL chose to go with the TOP construct.

But the main difference between ADQL and SQL was the addition of a suite of
geometric functions to support spatial queries (and the assumption that there
would be spatial indexing underneath them to provide speed).  This is not 
supported intrinsically in any variant of SQL, though there are extensions
to several DBMSs that provide the underpinnings this functionality would need.

In the absence of any standard, the VO community defined a set of functions
to be implemented as necessary.  For example, a cone search would be 
accomplished with the "CONTAINS()" function (as in "circle contains database
point")::

    where CONTAINS(POINT('icrs', ra, dec), CIRCLE('icrs', 34., 45. 0.1)) = 1

Here we have defined a circle (in the ICRS coordinate system) centered at
(34.,45.) and having a radius of 0.1 degrees and are comparing it to the
set of points on the sky defined by the RA and Dec columns from a database
table (also in the ICRS coordinate system).  Since CONTAINS() is a function
it has to have a value and CONTAINS() value returns 1 or 0 for True/False.

In a simple system with a single CONTAINS() constraint, this could be
implemented as a post-filter but this approach rapidly falls apart, both
as the table gets large and if the function is used repeatedly in an 
AND/OR query.


Our Approach
------------
Rather than dive into the guts of all the DBMSs, we have chosen a simpler
approach.  Since the bulk of any ADQL query is going to match the local
DBMS SQL just fine, we decided to pre-process the ADQL into local SQL
before giving it to the engine.

We use Oracle internally (because of a Caltech site license), so that was
our first implementation.  We also occasionally use SQLite3, so that was 
the second.  We plan to implement PostgreSQL next and then MySQL but can
adjust the schedule in response to input from the community.

We will start with the following ADQL::

   select TOP 100 ra, dec 
   from iraspsc 
   where contains(point('icrs', ra, dec), circle('GALACTIC', 234.56, 34.567, 0.006)) = 1 
         and glat > 34.567
   order by dec desc

In order to modify the construction of the statement, we need to first
take the ADQL apart.  However, we don't need to fully characterize it in
the way a database engine would and in particular we don't need to 
"validate" the structure (or column names or anything else;  the DBMS
is going to do that later).  We just need to have the pieces of the query
in a form that can be put back together and where the principal components
are identified (*e.g.,* the beginning and end of the WHERE clause, where
each CONTAINS() function appears (and other functions like POINT(),
CIRCLE(), POLYGON() and DISTANCE()).

For this we can use a non-validating parser package like the pure
Python 'sqlparse'.  Sqlparse turns an SQL (ADQL) statement into a
memory structure like the following::

   [select]
   [ ]
   Identifier: [ra]
   TokenList:
      [ra]
   [,]
   [ ]
   [dec]
   [ ]
   [from]
   [ ]
   Identifier: [iraspsc]
   TokenList:
      [iraspsc]
   [ ]
   Where:
      [where]
      [ ]
      TokenList:
         TokenList:
            Identifier: [contains]
            TokenList:
               [contains]
            TokenList:
               [(]
               IdentifierList:
                  TokenList:
                     Identifier: [point]
                     TokenList:
                        [point]
                     TokenList:
                        [(]
                        IdentifierList:
                           ['icrs']
                           [,]
                           [ ]
                           Identifier: [ra]
                           TokenList:
                              [ra]
                        [,]
                        [ ]
                        [dec]
                        [)]
                  [,]
                  [ ]
                  TokenList:
                     Identifier: [circle]
                     TokenList:
                        [circle]
                     TokenList:
                        [(]
                        IdentifierList:
                           ['GALACTIC']
                           [,]
                           [ ]
                           [234.56]
                           [,]
                           [ ]
                           [34.567]
                           [,]
                           [ ]
                           [0.006]
                        [)]
               [)]
         [ ]
         [=]
         [ ]
         [1]
      [ ]
      [and]
      [ ]
      TokenList:
         Identifier: [glat]
         TokenList:
            [glat]
         [ ]
         [>]
         [ ]
         [34.567]
      [ ]
   [order by]
   [ ]
   [dec]
   [ ]
   [desc]

From this, we identify the "geometry" blocks (actually any specially identified
functions) and replace them with placeholder` tokens::

   token  0:   [select]
   token  1:   [ ]
   token  2:   [ra]
   token  3:   [,]
   token  4:   [ ]
   token  5:   [dec]
   token  6:   [ ]
   token  7:   [from]
   token  8:   [ ]
   token  9:   [iraspsc]
   token 10:   [ ]
   token 11:   [where]
   token 12:   [ ]
   token 13:   [GEOM]
   token 14:   []
   token 15:   []
   token 16:   []
   token 17:   []
   token 18:   [ ]
   token 19:   [and]
   token 20:   [ ]
   token 21:   [glat]
   token 22:   [ ]
   token 23:   [>]
   token 24:   [ ]
   token 25:   [34.567]
   token 26:   [ ]
   token 27:   [order by]
   token 28:   [ ]
   token 29:   [dec]
   token 30:   [ ]
   token 31:   [desc]

The geometry information is saved in a structure the will be converted into
a form usable by the DBMS::

   funcData:
   [
     {
       'name': 'contains',
       'args':
       [
         {'name': 'point', 
          'args': ["'icrs'", 'ra', 'dec']},

         {'name': 'circle',
          'args': ["'GALACTIC'", '234.56', '34.567', '0.006']}
       ], 
         
       'val': '1'
     }
   ]

With these data structures, we can fairly easily move the TOP specification inside the WHERE 
clause as a constraint on ROWNUM and convert each CONTAINS() block into the equivalent constraints 
on the (x,y,z) and spatial index (here 'htm20') columns using the tools 
described in :doc:`spatial_index`::

   select ra, dec
   from iraspsc
   where (((-0.797580403011*x)+(0.603104711077*y)+(-0.011410881210*z)>=9.999999945169e-01)
         AND (   (htm14 = 2569468753) 
              OR (htm14 = 2569468758)
              OR (htm14 = 2569468766)
              OR (htm14 BETWEEN 2569468865 AND 2569468879))
          and glat > 34.567)
         AND ROWNUM <= 100
   order by dec desc


Note that the spatial part of this translation is DBMS-agnostic; it would work just as
well with PostgreSQL or SQLite.  The conversion of the TOP directive is actually hardest
for Oracle given that it has to become part of the WHERE clause; for other DBMSs it would 
be easier.

Extending the Paradigm
----------------------
Our databases do not contain records which themselves have extended geometry and we 
can therefore forego ADQL functions like INTERSECTS() in this first implementation.
To address this later, we would first choose a DBMS with intrinsic multi-dimensional
support (*e.g.,* a R-Tree index).  Our translator could then convert the geometric
functions into the extended local DBMS syntax.

We tried to write the ADQL translation code in particular to facilitate extension
and reuse.  If you have a different DBMS or need for extended objects or even 
new special functions for your own use, we would be happy to work with you to
extend this capability.


What's Not Implemented in ADQL
------------------------------
There are a few ADQL geometry functions we have not implemented.  Here is a complete list:


- **INTERSECTS** Test whether two geometric objects intersect.  Specifically, whether a
  geometric object stored in the database intersects with a region defined by the user.
  This is a very useful capability but requires R-Tree indexing to implement correctly.
  We will address this in the future, probably usind DBMSs that have R-Trees built-in.

- **AREA, CENTROID**  Calculate the area/centroid of a geometric object. Most useful if
  applied to geometric objects in the database.  Less so (as here) where the user would
  be creating a region definition themselves and could easily do these calculations on
  it.  We could implement this if someone defines a use case but for now we are leaving
  it out.

- **COORD1, COORD2, COORDSYS**  Extract the coordinate values or coordinate system from
  a POINT() object.  Again, most useful if there are stored POINT objects in the database
  but we could implement if a use case is defined.

- **REGION** A generic approach to region specification in string form.  This may well be
  of great use in the future but at the moment there wouldn't appear to be a great call
  for it.

There are also constructs that are technically possible (like CONTAINS(CIRCLE(), CIRCLE())
but again this makes the most sense where we are talking about CIRCLE objects stored in 
the database.  Most other use can be achieve by using CONTAINS(POINT(), CIRCLE()) and padding
the size of the second circle with the radius of the first.  Again, if someone can define
a real need, we will revisit this.


Convex Polygons
---------------
In the ADQL specification there is no specification that polygons should be convex
(actually, it doesn't even specify that the polygon lines shouldn't cross).  This distinction
is important in practice since the usual way of checking whether a point is in a polygon
(*i.e.,* go around the outside of the polygon and see if the point is alway on the same side 
of the edge lines by doing cross- and dot-products) fails for a concave polygon.

There are various ways to compensate for this, including decomposing the concave 
polygon into a set of convex ones or finding the bounding convex polygon for the 
region and post-filtering the database points found.  None of these is easy to 
implement in practice, so for now we consider it an error to use anything but a
convex polygon.
