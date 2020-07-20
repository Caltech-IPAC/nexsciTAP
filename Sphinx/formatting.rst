Notes on Data Formatting
========================

DBMSs in general do a rather poor job with data precision.  The values that are output
are accurate but quite often what was input as "3.45" will be output as "3.4499999999999903"
or "3.4500000000000001E00": accurate as to value but precision information has been lost.

nexsciTAP tries to compensate for this by augmenting the TAP_SCHEMA metadata with formatting
information (*e.g.,* "f4.2" for the above) and this works well for many of the uniform
catalogs encountered in astronomy.  However, there are two situations where it is 
insufficient.

The first is where the precision of different records varies.  This is the case in, for
instance, tables from the NASA Exoplanet Archive where the data is culled from the literature
and where the precision in the same column for different records may be different.  This 
can be addressed at the database level by storing precision information on a cell by 
cell basis.

The second arises with columns that are constructed on the fly, like "B-V as BmV" or
"count(*) as nrec".  We can't know up front how best to format these, though here are 
a variety of approaches we can take.

The simplest approach is to use a format guaranteed to fit, like "e22.17" for floating
point numbers.  However, this is hard to read and takes up a lot of space.

At the other extreme, we can use one form or another of a double pass through the data;
scanning once to determine reasonable formats and then a second time to output the
data using those formats.

A compromise, which is what we implement now, scans the first N (currently 10,000) records
(in memory) to deduce formats for any constructed columns, then outputs these and any
subsequent records using these formats.  Quite often, the number of records is less than
N and this is completely accurate.  We allow N to be configurable at run time.

None of this is absolutely foolproof; we will try to accomodate special cases as they
arise.
