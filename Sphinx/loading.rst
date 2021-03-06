
Adding Spatial Indexing to Your Data
====================================

Do I Need a Spatial Index
-------------------------
If you have tables without sky coordinates, there is not much point in worrying about
spatial indexing.  Even if you have coordinates but no need to search spatially, you
might as well not bother.  

If you want to start with a table that doesn't have the spatial index column set, you
are free to do so; just don't try to include CONTAINS() constraints in any queries.  
You can add the spatial capabilities later (though it will require reloading the tables).


Augmenting Your Tables with the Necessary Columns
-------------------------------------------------
There are four columns you will need to add if you are going to support our built-in 
spatial indexing: x, y, z and spt_ind.  The first three are the unit sphere geometry
three-vector of the point and are calculated simply as::

   x = sin(RA) * cos(Dec)
   y = cos(RA) * cos(Dec)
   z = sin(Dec)

It is best to maintain these as full double precision numbers.

The fourth is the spatial index value (an integer).  If you need to generate this, use
our SpatialIndex package.  If you are working in Python, you already have this as it
is a dependency of nexsciTAP.  If you are working in C, download the source
(https://github.com/Caltech-IPAC/SpatialIndex.git) from GitHub and build it (run "make").


Computing Values for the New Columns
------------------------------------
There is code for computing all this in the SpatialIndex package.  The underlying
libraries there are written in C and there is an example program (SpatialIndex/src/sptIndx.c)
that adds (x,y,z) and both HTM and HPX spatial index ID columns to a CSV file (you specify
the tesselation level).
