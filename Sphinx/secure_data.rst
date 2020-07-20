Keeping Your Data Secure
========================

The whole point of TAP is to allow general relational queries against your tables.  
But what if you have tables that contain sensitive information?  The short answer 
is that you shouldn't make them visible to TAP.  

Similarly, while nexsciTAP limits input SQL to performing SELECT statements, the
tables visible to TAP should be read-only.

In all the DBMSs with which we are familiar, it is possible to create read-only
views of the tables you want to expose in a separate schema.  This schema is 
frequently nothing but views of tables in other schema.  This completely 
protects the data from being corrupted.

Similarly, any sensitive data can be kept out of the directly-readable space.
This doesn't mean you have no access to it.  For instance, some of our implementations
use user and project databases for determining which records in the searchable data
table a user is allowed to access.  This "proprietary filtering" is done in a 
custom module, modifying the query the user entered to include the right joins and
filtering to get the right records.  This code is allowed to see the schema containing
the secure data.

But the user's original query, which gets checked against the TAP_SCHEMA list of 
accessible tables, doesn't mention these sensitive tables and would not be allowed
to query them directly.
