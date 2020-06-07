Operations Issues
=================

NExScI TAP requires very little babysitting.  We don't manage the
the input jobs (*i.e.* they are started right away), so there is 
nothing to do there.

Results do pile up in the workspace.  To properly support people 
who submit background (*i.e.* "asynchronous") jobs, we leave results
in the workspace for four days after the job has completed.  
NExScI TAP does not delete the files; we have a cron job that
checks the workspace for files older than that and deletes them.
We could formalize this as a distributed tool but it is awfully
simple and you may want to manage this space alongside other 
services you have working or use different rules.

Contact us if you are interested in our code.
