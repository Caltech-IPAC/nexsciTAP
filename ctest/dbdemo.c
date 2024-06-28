#include <stdio.h>
#include <Python.h>

PyObject *module;

char dbStatus[1024];
char dbMsg   [1024];

int  dbNrec;


int main(int argc, char **argv)
{
    int i, status, nrec;

    char sql  [1024];
    char fname[1024];

    int debug = 0;

    strcpy(dbStatus, "");
    strcpy(dbMsg,    "");
    dbNrec = 0;

    dbInit(debug);

    for(i=1; i<6; ++i)
    {
        sprintf(sql, "select ra, dec from PS where dec between %d and %d",
            -i, i);

        sprintf(fname, "ps_search_%d.tbl", i);

        printf("\nDEBUG> Parameters:\n");
        printf("DEBUG> [%s]\n", sql);
        printf("DEBUG> [%s]\n", fname);
        fflush(stdout);

        status = dbRunQuery(sql, fname, debug);

        if(status == 0)
            printf("ERROR: %s\n", dbMsg);
        else
            printf("SUCCESS: %d records.\n", dbNrec);
    }
    
    dbClose();

    exit(0);
}

 

int dbInit(int debug)
{
    PyObject *filename, *init_func, *args, *call;

   
    // Initialize an instance of embedded Python
    
    Py_Initialize();

    if(debug)
    {
        printf("DEBUG> Embedded Python initialized.\n");
        fflush(stdout);
    }


    // Create a filename object.
    
    filename = PyUnicode_FromString("dbtap");

    if(filename == NULL)
    {
        if(debug)
        {
            printf("Failed to create 'filename' object.\n");
            fflush(stdout);
        }
        
        exit(0);
    }

    if(debug)
    {
        printf("DEBUG> Made filename.\n");
        fflush(stdout);
    }


    // Import the Python module from 'dbtap.py'

    module = PyImport_Import(filename);

    if(module == NULL)
    {
        Py_DECREF(filename);

        if(debug)
        {
            printf("Failed to import module 'dbtap'.\n");
            fflush(stdout);
        }
        
        exit(0);
    }

    if(debug)
    {
        printf("DEBUG> Imported module.\n");
        fflush(stdout);
    }

    Py_DECREF(filename);


    // Find the dbinit function 
    
    init_func = PyObject_GetAttrString(module, "dbinit");

    if(init_func == NULL)
    {
        Py_DECREF(filename);

        if(debug)
        {
            printf("Failed to get 'dbinit' function'.\n");
            fflush(stdout);
        }
        
        exit(0);
    }

    if(debug)
    {
        printf("DEBUG> Found dbinit() function.\n");
        fflush(stdout);
    }


    // Run dbinit() function

    args = PyTuple_Pack(1, PyLong_FromLong(debug));

    if(args == NULL)
    {
        if(debug)
        {
            printf("Failed to compose arguments.\n");
            fflush(stdout);
        }
        
        exit(0);
    }
    
    call = PyObject_CallObject(init_func, args);

    Py_DECREF(init_func);

    if(call == NULL)
    {
        if(debug)
        {
            printf("Failed to run 'dbinit()'\n");
            fflush(stdout);
        }
        
        exit(0);
    }

    if(debug)
    {
        printf("DEBUG> Ran dbinit() function.\n");
        fflush(stdout);
    }

    Py_DECREF(call);
}


int dbRunQuery(char *sql, char *filename, int debug)
{
    char cmd[32768];

    int nrec;

    PyObject *dbQuery, *args, *rtn;

    if(debug)
    {
        printf("\nDEBUG> In dbRunQuery().\n");
        fflush(stdout);
    }


    // Get a handle to the 'query' function in class 'DB'

    dbQuery = PyObject_GetAttrString(module, "dbquery");

    if(dbQuery == NULL)
    {
        Py_DECREF(module);
        Py_DECREF(module);

        if(debug)
        {
            printf("Failed to get 'dbquery' function.\n");
            fflush(stdout);
        }
        
        exit(0);
    }

    if(debug)
    {
        printf("DEBUG> Found dbquery() function.\n");
        fflush(stdout);
    }
    
    
    // Compose args
    
    args = PyTuple_Pack(2, PyUnicode_FromString(sql), 
                           PyUnicode_FromString(filename));

    if(args == NULL)
    {
        if(debug)
        {
            printf("Failed to compose arguments.\n");
            fflush(stdout);
        }
        
        exit(0);
    }

    if(debug)
    {
        printf("DEBUG> Created args.\n");
        fflush(stdout);
    }


    // Run dbQuery function
    
    rtn = PyObject_CallObject(dbQuery, args);

    Py_DECREF(dbQuery);
    Py_DECREF(args);

    if(rtn == NULL)
    {
        if(debug)
        {
            printf("Failed to run dbquery() function.\n");
            fflush(stdout);
        }
        
        exit(0);
    }

    if(debug)
    {
        printf("DEBUG> Ran dbquery() function.\n");
        fflush(stdout);
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


int dbClose()
{
    Py_DECREF(module);

    Py_Finalize();
}
