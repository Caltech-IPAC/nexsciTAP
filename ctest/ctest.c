#include <stdio.h>
#include <Python.h>

int main(int argc, char **argv)
{
    PyObject *filename, *module, *func, *rtn, *args;

    int rowcount, nrec;

    char status[1024];
    char msg   [1024];

    FILE     *fd;

    // Initialize an instance of embedded Pyton
    
    Py_Initialize();

    
    // When used from C, Python can't know when it can when
    // variables created by C can be garbage collected.  So
    // best practice is to explicitly tell it when we are done
    // with a variable using Py_DECREF().  The Py_Finalize call
    // at the end of this code makes this unnecessary but it is
    // better to stay in the habit.


    // Import the Python code from 'ctap.py'

    filename = PyUnicode_FromString("ctap");

    module = PyImport_Import(filename);

    if(module == NULL)
    {
        printf("Failed to import load module 'ctap'\n");
        exit(0);
    }

    Py_DECREF(filename);


    // Get a handle to the 'tapc' function in 'ctap.py'

    func = PyObject_GetAttrString(module, "tapc");

    if(func == NULL)
    {
        printf("Failed to extract function\n");
        exit(0);
    }

    Py_DECREF(module);


    // Put together the arguments tuple for a call to 'tapc',
    // in this case three strings: the name of a DBMS catalog
    // and a select query against that table, and an output
    // table file name.

    args = PyTuple_Pack(3, 
                        PyUnicode_FromString("PS"),
                        PyUnicode_FromString("select ra, dec from PS where ra between -3 and 3"),
                        PyUnicode_FromString("ctap.tbl"));


    // Call the function with the arguments
    
    rtn = PyObject_CallObject(func, args);

    if(rtn == NULL)
    {
        printf("Failed to run function\n");
        exit(0);
    }

    Py_DECREF(args);
    Py_DECREF(func);


    // Extract the return value from the return structure.
    // The tapc() function in our Python returns a tuple of 
    // four values: two strings and two integers.  As with all
    // Python library objects, these are all some form of PyObject*
    // and rtn itself is a PyObject* representing a sequence of
    // the four.  So first we pull the value out by sequence number
    // and then convert it to C variables based on it's type (utf-8
    // characters or long integer).

    strcpy(status, PyUnicode_AsUTF8(PySequence_GetItem(rtn, 0)));
    strcpy(msg,    PyUnicode_AsUTF8(PySequence_GetItem(rtn, 1)));
    
    rowcount = PyLong_AsLong(PySequence_GetItem(rtn, 2));
    nrec     = PyLong_AsLong(PySequence_GetItem(rtn, 3));

    printf("\n");
    printf("status:    %s\n", status);
    printf("msg:       %s\n", msg);
    printf("rowcount:  %d\n", rowcount);
    printf("nrec:      %d\n", nrec);
    printf("\n");


    // Close the embedded Python instance

    Py_DECREF(rtn);

    Py_Finalize();
}
