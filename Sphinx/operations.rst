Operations Issues
=================

NExScI TAP requires very little babysitting.  We don't queue the
the input jobs (*i.e.* they are started right away), so there is 
nothing to do there.

Results do pile up in the workspace.  To properly support people 
who submit background (*i.e.* "asynchronous") jobs, our sites leave results
in the workspace for four days after the job has completed (four days
means that results for jobs that complete on Friday are still available 
on the following Monday).  

NExScI TAP does not delete the files; our sites have cron jobs that
check the workspace for files older than four days and deletes them.
We could formalize and distribute this as a formal tool but it is awfully
simple and you may want to manage this space alongside other 
services you have working or to use different rules.

Contact us if you are interested in seeing our code.
