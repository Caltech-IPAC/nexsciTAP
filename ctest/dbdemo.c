#include <stdio.h>
#include <Python.h>

PyObject *module;

char dbStatus[1024];
char dbMsg   [1024];

int  dbNrec;


//  I don't expect this capability is going to be very widely used.  Most current 
//  database access from C programs use the ISISQL service program and the TAP local
//  service example above can be used as a fairly direct replacement.  It is only when
//  we want to do something more unusual, like running additional SQL directives like
//  "create temporary table ..." or run a bunch of TAP-like queries in a loop that 
//  the local service mode becomes inefficient.  That is likely to involve a very small
//  handful of applications so rather than going through the effort of setting up yet
//  another library, we just give this example.  The developer (probably me) can
//  just include the two functions in their code and use the "main" method as an
//  example of how to use them.


int main(int argc, char **argv)
{
    int i, status, nrec;

    char sql  [1024];
    char fname[1024];

    strcpy(dbStatus, "");
    strcpy(dbMsg,    "");
    dbNrec = 0;

    status = dbInit();

    if(status == 0)
        printf("ERROR: dbInit() failed.\n");

    for(i=1; i<6; ++i)
    {
        sprintf(sql, "select ra, dec from PS where dec between %d and %d",
            -i, i);

        sprintf(fname, "ps_search_%d.tbl", i);

        printf("\nDEBUG> Parameters:\n");
        printf("DEBUG> [%s]\n", sql);
        printf("DEBUG> [%s]\n", fname);
        fflush(stdout);

        status = dbRunQuery(sql, fname);

        if(status == 0)
            printf("ERROR: %s\n", dbMsg);
        else
            printf("SUCCESS: %d records.\n", dbNrec);
    }
    
    dbClose();

    exit(0);
}

 

int dbInit()
{
    PyObject *filename, *init_func, *args, *rtn;

    int status;

   
    // Initialize an instance of embedded Python
    
    Py_Initialize();


    // When used from C, Python can't know when it can when
    // variables created by C can be garbage collected.  So
    // best practice is to explicitly tell it when we are done
    // with a variable using Py_DECREF().

    // Create a filename object.
    
    filename = PyUnicode_FromString("dbtap");

    if(filename == NULL)
    {
        printf("Failed to create 'filename' object.\n");
        fflush(stdout);
        exit(0);
    }


    // Import the Python module from 'dbtap.py'

    module = PyImport_Import(filename);

    if(module == NULL)
    {
        Py_DECREF(filename);

        printf("Failed to import module 'dbtap'.\n");
        fflush(stdout);
        exit(0);
    }

    Py_DECREF(filename);


    // Find the dbinit function 
    
    init_func = PyObject_GetAttrString(module, "dbinit");

    if(init_func == NULL)
    {
        Py_DECREF(filename);

        printf("Failed to get 'dbinit' function'.\n");
        fflush(stdout);
        exit(0);
    }


    // Run the dbinit() function

    rtn = PyObject_CallObject(init_func, NULL);

    Py_DECREF(init_func);

    if(rtn == NULL)
    {
        printf("Failed to run 'dbinit()'\n");
        fflush(stdout);
        exit(0);
    }

    status = PyLong_AsLong(PySequence_GetItem(rtn, 0));

    Py_DECREF(rtn);
}



// This is where we actually run the TAP ADQL query

int dbRunQuery(char *sql, char *filename)
{
    char cmd[32768];

    int nrec;

    PyObject *dbQuery, *args, *rtn;

    strcpy(dbStatus, "ERROR");


    // Find the 'query' function 

    dbQuery = PyObject_GetAttrString(module, "dbquery");

    if(dbQuery == NULL)
    {
        Py_DECREF(module);
        Py_DECREF(module);

        strcpy(dbMsg, "Failed to find the 'dbquery' function.\n");
        return 0;
    }

    
    // Compose args
    
    args = PyTuple_Pack(2, PyUnicode_FromString(sql), 
                           PyUnicode_FromString(filename));

    if(args == NULL)
    {
        strcpy(dbMsg, "Failed to compose arguments.\n");
        return 0;
    }


    // Run dbQuery function
    
    rtn = PyObject_CallObject(dbQuery, args);

    Py_DECREF(dbQuery);
    Py_DECREF(args);

    if(rtn == NULL)
    {
        strcpy(dbMsg, "Failed to run dbquery() function.\n");
        return 0;
    }


    // Return values

    strcpy(dbStatus, PyUnicode_AsUTF8(PySequence_GetItem(rtn, 0)));
    strcpy(dbMsg,    PyUnicode_AsUTF8(PySequence_GetItem(rtn, 1)));
    
    dbNrec = PyLong_AsLong(PySequence_GetItem(rtn, 2));
    
    Py_DECREF(rtn);

    if(strcmp(dbStatus, "OK") == 0)
        return(1);
    else
        return(0);
}



// Best not to use this unless we are sure we are done
// with the Python instance and have a lot more code to
// run.

int dbClose()
{
    Py_DECREF(module);

    Py_Finalize();
}
