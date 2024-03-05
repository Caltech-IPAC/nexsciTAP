/*
  Copyright (c) 2020, Caltech IPAC.
  This code is released with a BSD 3-clause license. License information is at
    https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE
*/


#include <Python.h>
#include <sys/types.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>

static PyObject *method_writerecs(PyObject *self, PyObject *args) {

    PyObject *ddlist = NULL;
    PyObject *ddarr = NULL;
    
    PyObject *datalist = NULL;
    PyObject *dataarr = NULL;

    PyObject *item = NULL;
    PyObject *item_bytes = NULL;

    char filepath[1024];

    char **namearr;
    char **typearr;
    char **dbtypearr;
    char **fmtarr;
    char **descarr;
    char **unitsarr;

    char *cptr1;

    char charval;

    int  ishdr;
    int  coldesc;
    int  overflow;
    int  istail;

    int  *widtharr;
    
    int  ncols;
    int  nrows_data;
    int  nrows_dd;

    char    outfmt[40];
    char    msg[1024];
    char    fmt[40];
    char    nullval[20];
    char    nullval_ipac[20];
    char    strval[1024];
    char    jsonstr[4096];
    char    substr[40];
    
    double  dblval = 0.0;
    int     intval = 0;
    int     istatus;

    const char *cptr_outpath = NULL;
    const char *cptr_format = NULL;
    const char *cptr = NULL;

    int i, j, k, l, m;

    int inlen;

    char debugfname[1024];
    int  debug  = 1;
    int  debug1 = 1;

    FILE *fp;
    FILE *fp_debug = (FILE *)NULL;
    
    sprintf(debugfname, "/tmp/writerec_%d.debug", getpid());


    if (debug) {
        fp_debug = (FILE *)NULL;
        fp_debug = fopen (debugfname, "w+");

        if (fp_debug == (FILE *)NULL) {
            printf ("From writerecs: Failed to open debug file\n");
            fflush (stdout);
        }
    }

/* Parse arguments */

    if(!PyArg_ParseTuple(args, "ssOOiiii", &cptr_outpath, &cptr_format, 
        &ddlist, &datalist, &ishdr, &coldesc, &overflow, &istail)) {

        if ((debug) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "PyArg_ParseTuple error\n");
            fflush (fp_debug);
        }

        PyErr_SetString (PyExc_Exception, "parseTuple error");
        return NULL;
    }

    if (cptr_outpath == (char *)NULL) {
        PyErr_SetString (PyExc_Exception, "Input outpath string empty");
        return NULL;
    }
    strcpy (filepath, cptr_outpath);

    if (cptr_format == (char *)NULL) {
        PyErr_SetString (PyExc_Exception, "Input format string empty");
        return NULL;
    }
    strcpy (outfmt, cptr_format);

    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "filepath= %s\n", filepath);
        fprintf (fp_debug, "outfmt= %s\n", outfmt);
        fprintf (fp_debug, "ishdr= %d\n", ishdr);
        fprintf (fp_debug, "coldesc= %d\n", coldesc);
        fprintf (fp_debug, "overflow= %d\n", overflow);
        fprintf (fp_debug, "istail= %d\n", istail);
        fflush (fp_debug);
    } 


/*
    retrieve datalist
*/
    if(!PyList_Check (datalist)) {

        if ((debug) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "PyList_Check (datalist) failed: return NULL\n");
            fflush (fp_debug);
        }

        PyErr_SetString (PyExc_Exception, "PyList_Check (datalist) failed.");
        return NULL;
    }

    nrows_data = PyObject_Length (datalist);

    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "nrows_data= %d\n", nrows_data);
        fflush (fp_debug);
    } 
    
    
    if(!PyList_Check (ddlist)) {

        if ((debug) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "PyList_Check failed: return NULL\n");
            fflush (fp_debug);
        }
        PyErr_SetString (PyExc_Exception, "PyList_Check (ddlist) failed.");
        return NULL;
    }

    nrows_dd = PyObject_Length (ddlist);

    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "nrows_dd= %d\n", nrows_dd);
        fflush (fp_debug);
    } 
    
    if (nrows_dd <= 0) {
        PyErr_SetString (PyExc_Exception, "ddlist empty.");
        return NULL;
    }
    
    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "here0\n");
        fflush (fp_debug);
    } 

/*
    retrieve the first tuple to count ncols
*/
    ddarr = PyList_GetItem (ddlist, 0); 
       
    if (!PySequence_Check (ddarr)) {

        if ((debug) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "PySequence_Check failed: return NULL\n");
            fflush (fp_debug);
        }
        PyErr_SetString (PyExc_Exception, "Failed PySequence_Check");
        return NULL;
    }
       
    ncols = PyObject_Length (ddarr);
        
    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "ncols= %d\n", ncols);
        fflush (fp_debug);
    }         

/*
    malloc arrays
*/
    namearr = (char **)NULL;
    namearr = (char **)malloc (ncols*sizeof(char *));
    if (namearr == (char **)NULL) {
        PyErr_SetString (PyExc_Exception, "Failed to malloc namearr");
        return NULL;
    }
    for (i=0; i<ncols; i++) {
        namearr[i] = (char *)NULL;
        namearr[i] = (char *)malloc (40*sizeof(char));
        if (namearr[i] == (char *)NULL) {
            PyErr_SetString (PyExc_Exception, "Failed to malloc namearr");
            return NULL;
        }
    }

    typearr = (char **)NULL;
    typearr = (char **)malloc (ncols*sizeof(char *));
    if (typearr == (char **)NULL) {
        PyErr_SetString (PyExc_Exception, "Failed to malloc typearr");
        return NULL;
    }
    for (i=0; i<ncols; i++) {
        typearr[i] = (char *)NULL;
        typearr[i] = (char *)malloc (20*sizeof(char));
        if (typearr[i] == (char *)NULL) {
            PyErr_SetString (PyExc_Exception, "Failed to malloc typearr");
            return NULL;
        }
    }

    dbtypearr = (char **)NULL;
    dbtypearr = (char **)malloc (ncols*sizeof(char *));
    if (dbtypearr == (char **)NULL) {
        PyErr_SetString (PyExc_Exception, "Failed to malloc dbtypearr");
        return NULL;
    }
    for (i=0; i<ncols; i++) {
        dbtypearr[i] = (char *)NULL;
        dbtypearr[i] = (char *)malloc (40*sizeof(char));
        if (dbtypearr[i] == (char *)NULL) {
            PyErr_SetString (PyExc_Exception, "Failed to malloc dbtypearr");
            return NULL;
        }
    }

    fmtarr = (char **)NULL;
    fmtarr = (char **)malloc (ncols*sizeof(char *));
    if (fmtarr == (char **)NULL) {
        PyErr_SetString (PyExc_Exception, "Failed to malloc fmtarr array");
        return NULL;
    }
    for (i=0; i<ncols; i++) {
        fmtarr[i] = (char *)NULL;
        fmtarr[i] = (char *)malloc (40*sizeof(char));
        if (fmtarr[i] == (char *)NULL) {
            PyErr_SetString (PyExc_Exception, "Failed to malloc fmtarr");
            return NULL;
        }
    }

    descarr = (char **)NULL;
    descarr = (char **)malloc (ncols*sizeof(char *));
    if (descarr == (char **)NULL) {
        PyErr_SetString (PyExc_Exception, "Failed to malloc descarr array");
        return NULL;
    }
    for (i=0; i<ncols; i++) {
        descarr[i] = (char *)NULL;
        descarr[i] = (char *)malloc (1024*sizeof(char));
        if (descarr[i] == (char *)NULL) {
            PyErr_SetString (PyExc_Exception, "Failed to malloc descarr");
            return NULL;
        }
    }

    unitsarr = (char **)NULL;
    unitsarr = (char **)malloc (ncols*sizeof(char *));
    if (unitsarr == (char **)NULL) {
        PyErr_SetString (PyExc_Exception, "Failed to malloc unitsarr array");
        return NULL;
    }
    for (i=0; i<ncols; i++) {
        unitsarr[i] = (char *)NULL;
        unitsarr[i] = (char *)malloc (40*sizeof(char));
        if (unitsarr[i] == (char *)NULL) {
            PyErr_SetString (PyExc_Exception, "Failed to malloc unitsarr");
            return NULL;
        }
    }

    widtharr = (int *)NULL;
    widtharr = (int *)malloc (ncols*sizeof(int));
    if (widtharr == (int *)NULL) {
        PyErr_SetString (PyExc_Exception, "Failed to malloc widtharr");
        return NULL;
    }

    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "\nDone malloc dd arrays\n");
        fflush (fp_debug);
    }

/*
    retrieve namearr, typearr, dbtypearr, fmtarr, unitsarr, descarr, widtharr
*/
    for (l=0; l<nrows_dd; l++) {

        if ((debug) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "\nl= [%d]\n", l );
            fflush (fp_debug);
        }
        
        ddarr = PyList_GetItem (ddlist, l); 

        if (!PySequence_Check (ddarr)) {

            if ((debug1) && (fp_debug != (FILE *)NULL)) {
                fprintf (fp_debug, "PySequence_Check failed: return NULL\n");
                fflush (fp_debug);
            }
            PyErr_SetString (PyExc_Exception, "Failed PySequence_Check");
            return NULL;
        }
       
        if (l < nrows_dd-1) {
        
            for (i=0; i<ncols; i++) {
       
                item = PySequence_GetItem (ddarr, i); 

                strcpy (strval, "");
                if (!PyUnicode_Check (item)) {
                
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "here1-2\n");
                        fprintf (fp_debug, "strval= %s\n", strval);
                        fflush (fp_debug);
                    }
                }
                else {
                    item_bytes 
                        = PyUnicode_AsEncodedString (item, "UTF-8", "strict");

                    if (item_bytes != NULL) {
                        cptr = PyBytes_AS_STRING (item_bytes);
                        strcpy (strval, cptr);
                    }
                }

                if (l == 0) {
                    strcpy (namearr[i], strval);

                    for(j=0; j<(int)strlen(namearr[i]); ++j)
                       namearr[i][j] = tolower(namearr[i][j]);
                
                    if ((debug) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "namearr[%d]= [%s]\n", 
                            i, namearr[i]);
                        fflush (fp_debug);
                    }
                }
                else if (l == 1) {
/*
    typearr: date and timestamp output file type is char
*/  
                    if ((strcasecmp (strval, "date") == 0) ||
                        (strcasecmp (strval, "timestamp") == 0)) {
                        
                        strcpy (typearr[i], "char");
                    }
                    else {
                        strcpy (typearr[i], strval);
                    }

                    if ((debug) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "typearr[%d]= [%s]\n", 
                            i, typearr[i]);
                        fflush (fp_debug);
                    }
                }
                else if (l == 2) {
/*
    dbtypearr
*/  
                    strcpy (dbtypearr[i], strval);
                    if ((debug) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "dbtypearr[%d]= [%s]\n", 
                            i, dbtypearr[i]);
                        fflush (fp_debug);
                    }
                }
                else if (l == 3) {
                    strcpy (fmtarr[i], strval);
                    if ((debug) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "fmtarr[%d]= [%s]\n", 
                            i, fmtarr[i]);
                        fflush (fp_debug);
                    }
                }
                else if (l == 4) {
                    strcpy (unitsarr[i], strval);
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "unitsarr[%d]= [%s]\n", 
                            i, unitsarr[i]);
                        fflush (fp_debug);
                    }
                }
                else if (l == 5) {
                    strcpy (descarr[i], strval);
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "descarr[%d]= [%s]\n", 
                            i, descarr[i]);
                        fflush (fp_debug);
                    }
                }
            }
                    
        }
        else {
/*
    the last dd row is widtharr: integer type
*/
            for (i=0; i<ncols; i++) {
       
                item = PySequence_GetItem (ddarr, i); 
              
                intval = 0;
                if (PyLong_Check (item)) {
                    intval = PyLong_AsLong (item);
                }
                
                widtharr[i] = intval;

                if ((debug) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "widtharr[%d]= [%d]\n", 
                        i, widtharr[i]);
                    fflush (fp_debug);
                }
            }
        }
    }
            
    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "\nDone with ddlist retrieval\n");
        fflush (fp_debug);
    }

/*
    open filepath to write header
*/
    fp = (FILE *)NULL;
    if (ishdr) {
        fp = fopen (filepath, "w+");
        chmod(filepath, 0664);
    }
    else {
        fp = fopen (filepath, "a+");
    }

    if (fp == (FILE *)NULL) {
        sprintf (msg, "Failed to open filepath: [%s](errno:%d)", filepath, errno);
        
        if ((debug) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "msg= [%s]\n", msg);
            fflush (fp_debug);
        }

        PyErr_SetString (PyExc_Exception, msg);
        return NULL;
    }

    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "filepath [%s] opened for write\n", filepath);
        fflush (fp_debug);
    }

/*
    write header
*/
    strcpy (nullval, "");
    strcpy (nullval_ipac, "null");

    if (ishdr) {
        
        if (strcasecmp (outfmt, "ipac") == 0) {
    

//            fprintf (fp, "\\\n");
//            fprintf (fp, "\\RowsRetrieved = %10d\n", nrows_data);

/*
    if coldesc =1: write column description -- currently not implemented
*/
            if (coldesc) {

            }

            fprintf (fp, "|");
            for (i=0; i<ncols; i++) {
                sprintf (fmt, "%%-%ds|", widtharr[i]);
                fprintf (fp, fmt, namearr[i]); 
            }
            fprintf (fp, "\n");
            fflush (fp);

            fprintf (fp, "|");
            for (i=0; i<ncols; i++) {
                sprintf (fmt, "%%-%ds|", widtharr[i]);
                fprintf (fp, fmt, typearr[i]); 
            }
            fprintf (fp, "\n");
            fflush (fp);

            fprintf (fp, "|");
            for (i=0; i<ncols; i++) {
                sprintf (fmt, "%%-%ds|", widtharr[i]);
                fprintf (fp, fmt, unitsarr[i]); 
            }
            fprintf (fp, "\n");
            fflush (fp);

            fprintf (fp, "|");
            for (i=0; i<ncols; i++) {
                sprintf (fmt, "%%-%ds|", widtharr[i]);
                fprintf (fp, fmt, nullval_ipac); 
            }
            fprintf (fp, "\n");
            fflush (fp);

        }
        else if (strcasecmp (outfmt, "votable") == 0) {
   
            fprintf (fp, "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
            fprintf (fp, "<VOTABLE version=\"1.3\" xmlns=\"http://www.ivoa.net/xml/VOTable/v1.3\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"http://www.ivoa.net/xml/VOTable/v1.3\">\n");
        
            fprintf (fp, "  <RESOURCE type=\"results\">\n");
        
            if (overflow) {
                fprintf (fp, 
                    "  <INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>\n");
            }
            else {
                fprintf (fp, "  <INFO name=\"QUERY_STATUS\" value=\"OK\"/>\n");
            }
    
            fprintf (fp, "  <TABLE>\n");
       
            for (i=0; i<ncols; i++) { 
    
                if ((debug1) && (fp_debug != (FILE *)NULL)) {
        
                    fprintf (fp_debug, "i= [%d] namearr= [%s]\n",
                        i, namearr[i]);

                    fprintf (fp_debug, "widtharr= [%d] typearr= [%s]\n",

                        widtharr[i], typearr[i]);
                    fprintf(fp, "descarr= [%s]\n", descarr[i]);

                    fflush (fp_debug);
                }
            

                // Char type
                
                if (strcasecmp (typearr[i], "char") == 0) {

                    if(strlen(descarr[i]) > 0)
                    {
                       if(strlen(unitsarr[i]) > 0)
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" arraysize=\"*\" datatype=\"%s\" "
                              "name=\"%s\" unit=\"%s\">\n", namearr[i], typearr[i],
                             namearr[i], unitsarr[i]);
                          fprintf (fp, "  <DESCRIPTION><![CDATA[ %s ]]></DESCRIPTION>\n", descarr[i]);
                          fprintf (fp, "</FIELD>\n");
                       }
                       else
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" arraysize=\"*\" datatype=\"%s\" "
                              "name=\"%s\">\n", namearr[i], typearr[i], namearr[i]);
                          fprintf (fp, "  <DESCRIPTION><![CDATA[ %s ]]></DESCRIPTION>\n", descarr[i]);
                          fprintf (fp, "</FIELD>\n");
                       }
                    }

                    else
                    {
                       if(strlen(unitsarr[i]) > 0)
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" arraysize=\"*\" datatype=\"%s\" "
                              "name=\"%s\" unit=\"%s\"/>\n", namearr[i], typearr[i],
                             namearr[i], unitsarr[i]);
                       }
                       else
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" arraysize=\"*\" datatype=\"%s\" "
                              "name=\"%s\"/>\n", namearr[i], typearr[i], namearr[i]);
                       }
                    }
                }


                // Non-char columns
                
                else {
                    if(strlen(descarr[i]) > 0)
                    {
                       if(strlen(unitsarr[i]) > 0)
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" datatype=\"%s\" name=\"%s\" unit=\"%s\">\n", 
                              namearr[i], typearr[i], namearr[i], unitsarr[i]);
                       }
                       else
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" datatype=\"%s\" name=\"%s\">\n", 
                              namearr[i], typearr[i], namearr[i]);
                       }

                       fprintf (fp, "  <DESCRIPTION><![CDATA[ %s ]]></DESCRIPTION>\n", descarr[i]);
                       fprintf (fp, "    </FIELD>\n>");
                    }
                    else
                    {
                       if(strlen(unitsarr[i]) > 0)
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" datatype=\"%s\" name=\"%s\" unit=\"%s\"/>\n", 
                              namearr[i], typearr[i], namearr[i], unitsarr[i]);
                       }
                       else
                       {
                          fprintf (fp, 
                              "    <FIELD ID=\"%s\" datatype=\"%s\" name=\"%s\"/>\n", 
                              namearr[i], typearr[i], namearr[i]);
                       }
                    }
                }
            }
        }
        else if (strcasecmp (outfmt, "csv") == 0) {
   
            for (i=0; i<ncols; i++) { 
            
                if (i == ncols-1) { 
                    fprintf (fp, "%s", namearr[i]);
                }
                else {
                    fprintf (fp, "%s,", namearr[i]);
                }
            }
            fprintf (fp, "\n");
        }
        else if (strcasecmp (outfmt, "tsv") == 0) {
    
            for (i=0; i<ncols; i++) { 
            
                if (i == ncols-1) { 
                    fprintf (fp, "%s", namearr[i]);
                }
                else {
                    fprintf (fp, "%s\t", namearr[i]);
                }
            }
            fprintf (fp, "\n");
        }
        else if (strcasecmp (outfmt, "json") == 0) {
           fprintf (fp, "[\n");
        }
        fflush (fp);
   
        if ((debug) && (fp_debug != (FILE *)NULL)) {
           fprintf (fp_debug, "Done write header\n");
           fflush (fp_debug);
        }
    }


/*
    retrieve data rows
*/
    if (istail == 1) {
       if ((nrows_data == 0) && (ishdr == 1)) {

           if ((debug) && (fp_debug != (FILE *)NULL)) {
               fprintf (fp_debug, "here0-1: write tail for empty table\n");
               fflush (fp_debug);
           } 

           if (strcasecmp (outfmt, "votable") == 0) {
     
               fprintf (fp, "  </TABLE>\n");
               fprintf (fp, "  </RESOURCE>\n");
               fprintf (fp, "</VOTABLE>\n");
           }
           else if (strcasecmp (outfmt, "json") == 0) {
               fprintf (fp, "]\n");
           }
           fflush (fp);
           fclose (fp);

           istatus = 0;    
           return PyLong_FromLong (istatus);
       }
    }
    
    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "here0-2\n");
        fflush (fp_debug);
    } 


/*
    retrieve each row and format output line 
*/
    if ((strcasecmp (outfmt, "votable") == 0) && (ishdr == 1)) {
  
        fprintf (fp, "    <DATA>\n");
        fprintf (fp, "      <TABLEDATA>\n");
    }

    for (l=0; l<nrows_data; l++) {

        if ((debug1) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "\nxxxl= [%d] (row counter)\n", l );
            fflush (fp_debug);
        }
        
        dataarr = PyList_GetItem (datalist, l); 

        if (!PyList_Check (dataarr)) {

            if ((debug1) && (fp_debug != (FILE *)NULL)) {
                fprintf (fp_debug, 
                    "PyList_Check (dataarr) failed: return NULL\n");
                fflush (fp_debug);
            }
            PyErr_SetString (PyExc_Exception, "Failed PyList_Check (dataarr)");
            return NULL;
        }
       
        if ((debug1) && (fp_debug != (FILE *)NULL)) {
            fprintf (fp_debug, "here0-3\n");
            fflush (fp_debug);
        }     

        if (strcasecmp (outfmt, "ipac") == 0) {
        
            fprintf (fp, " ");
        }
        else if (strcasecmp (outfmt, "votable") == 0) {
  
            fprintf (fp, "        <TR>\n");
        }

        for (i=0; i<ncols; i++) {
       
            item = PyList_GetItem (dataarr, i); 
            
            const char* coltype = Py_TYPE(item)->tp_name;

            if ((debug1) && (fp_debug != (FILE *)NULL)) {
                fprintf (fp_debug, "i= [%d] namearr= [%s]\n", i, namearr[i]);
                fflush (fp_debug);
                
                fprintf (fp_debug, "PyObject type:[%s], typearr[i]: [%s]\n", coltype, typearr[i]);
                fflush (fp_debug);
            }

            if (item == Py_None) {
                
                sprintf (fmt, "%%-%ds ", widtharr[i]);
            
                if ((debug1) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "i= [%d] PyNone\n", i);
                    fprintf (fp_debug, "fmt= [%s]\n", fmt);
                    fflush (fp_debug);
                }
                
                if (strcasecmp (outfmt, "ipac") == 0) {
        
                    fprintf (fp, fmt, nullval_ipac);
                    if (i == ncols-1) {
                        fprintf (fp, "\n");
                        if ((debug1) && (fp_debug != (FILE *)NULL)) {
                            fprintf (fp_debug, 
                                "new line written: i= [%d] ncols= [%d]\n", 
                                i, ncols);
                            fflush (fp_debug);
                        }
                    }
                }
                else if (strcasecmp (outfmt, "votable") == 0) {
                        
                    fprintf (fp, "        <TD>%s</TD>\n", nullval);
                }
                else if (strcasecmp (outfmt, "csv") == 0) {

                    if (i == ncols-1) {
                        fprintf (fp, "%s\n", nullval);
                    }
                    else {
                        fprintf (fp, "%s,", nullval);
                    }
                }
                else if (strcasecmp (outfmt, "tsv") == 0) {
            
                    if (i == ncols-1) {
                        fprintf (fp, "%s\n", nullval);
                    }
                    else {
                        fprintf (fp, "%s\t", nullval);
                    }
                }     
                else if (strcasecmp (outfmt, "json") == 0) {
            
                   if (i == 0)  {
                        fprintf (fp, "{");
                   }

                   fprintf (fp, "\"%s\": null", namearr[i]);

                   if (i < ncols-1)
                        fprintf (fp, ",\n");

                   if (i == ncols-1)
                        fprintf (fp, "}");
                }     
            }
            else if ((strcasecmp (typearr[i],   "char") == 0) || 
                     (strcasecmp (typearr[i],   "date") == 0) ||
                     (strcasecmp (dbtypearr[i], "timestamp") == 0)) {

                strcpy (strval, "");
                if (PyUnicode_Check (item)) {
                
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "pass Unicode check\n");
                        fflush (fp_debug);
                    }

                    item_bytes = PyUnicode_AsEncodedString (item, 
                        "UTF-8", "strict");

                    if (item_bytes != NULL) {
                        cptr = PyBytes_AS_STRING (item_bytes);
                        strcpy (strval, cptr);
                    }
                }
                
                
                if ((debug1) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "i= [%d] strval= [%s]\n", i, strval);
                    fflush (fp_debug);
                }

                if (strcasecmp (outfmt, "ipac") == 0) {
                    
                    sprintf (fmt, "%%-%s ", fmtarr[i]);
                    
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "fmt= [%s]\n", fmt);
                        fflush (fp_debug);
                    }
                    
                    fprintf (fp, fmt, strval);
                
                    if (i == ncols-1) {
                        fprintf (fp, "\n");
                        if ((debug1) && (fp_debug != (FILE *)NULL)) {
                            fprintf (fp_debug, 
                                "new line written: i= [%d] ncols= [%d]\n", 
                                i, ncols);
                            fflush (fp_debug);
                        }
                    }
                
                }
                else if (strcasecmp (outfmt, "votable") == 0) {
       
                    fprintf (fp, "        <TD><![CDATA[%s]]></TD>\n", strval);
                }
                else if (strcasecmp (outfmt, "csv") == 0) {

                    if (i == ncols-1) {
                        fprintf (fp, "\"%s\"\n", strval);
                    }
                    else {
                        fprintf (fp, "\"%s\",", strval);
                    }
                }
                else if (strcasecmp (outfmt, "tsv") == 0) {
            
                    if (i == ncols-1) {
                        fprintf (fp, "%s\n", strval);
                    }
                    else {
                        fprintf (fp, "%s\t", strval);
                    }
                } 
                else if (strcasecmp (outfmt, "json") == 0) {
                   if (i == 0) {
                        fprintf (fp, "{");
                   }

                    // JSON strings have to be encoded to escape some
                    // stuff (mostly the double quote character)

                    inlen = strlen(strval);

                    m=0;
                    for (k=0; k<inlen; ++k) {

                       charval = strval[k];

                       if(charval == '\b') {
                          jsonstr[m] = '\\';
                          jsonstr[m+1] = 'b';
                          m += 2;
                       }

                       else if(charval == '\f') {
                          jsonstr[m] = '\\';
                          jsonstr[m+1] = 'f';
                          m += 2;
                       }

                       else if(charval == '\n') {
                          jsonstr[m] = '\\';
                          jsonstr[m+1] = 'n';
                          m += 2;
                       }

                       else if(charval == '\r') {
                          jsonstr[m] = '\\';
                          jsonstr[m+1] = 'r';
                          m += 2;
                       }

                       else if(charval == '\t') {
                          jsonstr[m] = '\\';
                          jsonstr[m+1] = 't';
                          m += 2;
                       }

                       else if(charval == '"') {
                          jsonstr[m] = '\\';
                          jsonstr[m+1] = '"';
                          m += 2;
                       }

                       else if(charval == '\\') {
                          jsonstr[m] = '\\';
                          jsonstr[m+1] = '\\';
                          m += 2;
                       }

                       else {
                          jsonstr[m] = charval;
                          ++m;
                       }
                    }

                    jsonstr[m] = '\0';

                    fprintf (fp, "\"%s\": \"%s\"", namearr[i], jsonstr);

                   if (i < ncols-1)
                        fprintf (fp, ",\n");

                   if (i == ncols-1) {
                        fprintf (fp, "}");
                   }
                }     
            
            }
            else if ((strcasecmp (typearr[i], "int"    ) == 0) || 
                     (strcasecmp (typearr[i], "long"   ) == 0) ||
                     (strcasecmp (typearr[i], "integer") == 0)) {

                sprintf (fmt, "%%-%s", fmtarr[i]);
                        
                if ((debug1) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "fmt= [%s]\n", fmt);
                    fflush (fp_debug);
                }
                    
                strcpy (strval, "");
                if (PyLong_Check (item)) {
                    intval = PyLong_AsLong (item);
                    sprintf (strval, fmt, intval);
                }

                if ((debug1) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "i= [%d] intval= [%d]\n", i, intval);
                    fprintf (fp_debug, "strval= [%s]\n", strval);
                    fflush (fp_debug);
                }
                    
                if (strcasecmp (outfmt, "ipac") == 0) {
                    
                    fprintf (fp, "%s ", strval);
                    
                    if (i == ncols-1) {
                        fprintf (fp, "\n");
                        if ((debug1) && (fp_debug != (FILE *)NULL)) {
                            fprintf (fp_debug, 
                                "new line written: i= [%d] ncols= [%d]\n", 
                                i, ncols);
                            fflush (fp_debug);
                        }
                    }
                }
                else if (strcasecmp (outfmt, "votable") == 0) {
                        
                    sprintf (strval, "%d", intval);
                
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "votable:\n");
                        fprintf (fp_debug, "intval= [%d]\n", intval);
                        fprintf (fp_debug, "strval= [%s]\n", strval);
                        fflush (fp_debug);
                    }
                    
                    fprintf (fp, "        <TD>%s</TD>\n", strval);
                }
                else if (strcasecmp (outfmt, "csv") == 0) {

                    sprintf (strval, "%d", intval);
                    
                    if (i == ncols-1) {
                        fprintf (fp, "%s\n", strval);
                    }
                    else {
                        fprintf (fp, "%s,", strval);
                    }
                }
                else if (strcasecmp (outfmt, "tsv") == 0) {
            
                    sprintf (strval, "%d", intval);
                    
                    if (i == ncols-1) {
                        fprintf (fp, "%s\n", strval);
                    }
                    else {
                        fprintf (fp, "%s\t", strval);
                    }
                }
                else if (strcasecmp (outfmt, "json") == 0) {
            
                   sprintf (strval, "%d", intval);

                   if (i == 0) 
                        fprintf (fp, "{");

                    fprintf (fp, "\"%s\": %s", namearr[i], strval);

                   if (i < ncols-1)
                        fprintf (fp, ",\n");

                   if (i == ncols-1) {
                        fprintf (fp, "}");
                   }
                }     
            }
            else if ((strcasecmp (typearr[i], "float" ) == 0) || 
                     (strcasecmp (typearr[i], "double") == 0)) {

                if ((debug1) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "typearr= [%s]\n", typearr[i]);
                    fprintf (fp_debug, "fmtarr= [%s]\n", fmtarr[i]);
                    fflush (fp_debug);
                }

                sprintf (fmt, "%%-%s", fmtarr[i]);
                
                if ((debug1) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "fmt= [%s]\n", fmt);
                    fflush (fp_debug);
                }

                if (strcasecmp (outfmt, "ipac") != 0) {
                    
/*
    Non-ipac tables: strip width element from double format
*/
                    cptr1 = strchr (fmt, '.');
                    if (cptr1 != (char *)NULL) {
                        strcpy (substr, cptr1);
                        sprintf (fmt, "%%%s", substr);
                    }
                }
                
                if ((debug1) && (fp_debug != (FILE *)NULL)) {
                    fprintf (fp_debug, "finale: fmt= [%s]\n", fmt);
                    fflush (fp_debug);
                }

                strcpy (strval, "");
                if (PyFloat_Check (item)) {
                    
                    dblval = PyFloat_AsDouble (item);
                    sprintf (strval, fmt, dblval);
                    
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, 
                            "i= [%d] dblval= [%lf] strval= [%s]\n", 
                            i, dblval, strval);
                        fflush (fp_debug);
                    }
                }
                else if (PyLong_Check (item)) {
               
                    intval = PyLong_AsLong (item);
                    dblval = (float)intval;
                    sprintf (strval, fmt, dblval);
                
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "i= [%d] intval= [%d]\n", i, intval);
                        fprintf (fp_debug, "dblval= [%lf] strval= [%s]\n", 
                            dblval, strval);
                        fflush (fp_debug);
                    }
                }

                if (strcasecmp (outfmt, "ipac") == 0) {
        
                    if ((debug1) && (fp_debug != (FILE *)NULL)) {
                        fprintf (fp_debug, "ipac outfmt\n");
                        fflush (fp_debug);
                    }

                    fprintf (fp, "%s ", strval);
                        
                    if (i == ncols-1) {
                        fprintf (fp, "\n");
                        if ((debug1) && (fp_debug != (FILE *)NULL)) {
                            fprintf (fp_debug, 
                                "new line written: i= [%d] ncols= [%d]\n", 
                                i, ncols);
                            fflush (fp_debug);
                        }
                    }
                }
                else if (strcasecmp (outfmt, "votable") == 0) {
                        
                    fprintf (fp, "        <TD>%s</TD>\n", strval);
                }
                else if (strcasecmp (outfmt, "csv") == 0) {

                    if (i == ncols-1) {
                        fprintf (fp, "%s\n", strval);
                    }
                    else {
                        fprintf (fp, "%s,", strval);
                    }
                }
                else if (strcasecmp (outfmt, "tsv") == 0) {
            
                    if (i == ncols-1) {
                        fprintf (fp, "%s\n", strval);
                    }
                    else {
                        fprintf (fp, "%s\t", strval);
                    }
                }
                else if (strcasecmp (outfmt, "json") == 0) {
            
                   if (i == 0) 
                        fprintf (fp, "{");

                    fprintf (fp, "\"%s\": %s", namearr[i], strval);

                   if (i < ncols-1)
                        fprintf (fp, ",\n");

                   if (i == ncols-1) {
                        fprintf (fp, "}");
                   }
                }     
            }
        }

        if (strcasecmp (outfmt, "votable") == 0) {
            fprintf (fp, "        </TR>\n");
            fflush (fp);
        }
        else if (strcasecmp(outfmt, "json") == 0) {
            if (l == nrows_data-1 && istail == 1) {
                fprintf (fp, "\n");
            }
            else {
                fprintf (fp, ",\n");
            }
        }
    }

    
    if ((debug) && (fp_debug != (FILE *)NULL)) {
        fprintf (fp_debug, "outfmt= %s\n", outfmt);
        fprintf (fp_debug, "istail= %d\n", istail);
        fflush (fp_debug);
    } 

    if (istail == 1) {
       if (strcasecmp (outfmt, "votable") == 0) {
           fprintf (fp, "      </TABLEDATA>\n");
           fprintf (fp, "    </DATA>\n");
           fprintf (fp, "  </TABLE>\n");
           fprintf (fp, "  </RESOURCE>\n");
           fprintf (fp, "</VOTABLE>\n");
       
           if ((debug) && (fp_debug != (FILE *)NULL)) {
               fprintf (fp_debug, "votable close brackets written\n");
               fflush (fp_debug);
           }     
       }
       else if (strcasecmp (outfmt, "json") == 0) {
          fprintf (fp, "]\n");
       }
    }

    fflush (fp);
    fclose (fp);
    

/*
    free memory
*/
    for (i=0; i<ncols; i++) {
        
        free (namearr[i]);
        free (typearr[i]);
        free (dbtypearr[i]);
        free (fmtarr[i]);
        free (descarr[i]);
        free (unitsarr[i]);
    } 
    free (namearr);
    free (typearr);
    free (dbtypearr);
    free (fmtarr);
    free (descarr);
    free (unitsarr);
    free (widtharr);

    istatus = 0;    
    return PyLong_FromLong (istatus);
}

static PyMethodDef FputsMethods[] = {
    
    {"writerecs", method_writerecs, METH_VARARGS, 
    "Python interface for writerec C library function"},
    
    {NULL, NULL, 0, NULL}
};


static struct PyModuleDef writerecsmodule = {
    
    PyModuleDef_HEAD_INIT,
    "writerecs",
    "Python interface for the writerecs C library function",
    -1,
    FputsMethods
};


PyMODINIT_FUNC PyInit_writerecs(void) {
    return PyModule_Create(&writerecsmodule);
}



