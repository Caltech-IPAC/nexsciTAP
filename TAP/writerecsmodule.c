/*
  Copyright (c) 2020, Caltech IPAC.
  This code is released with a BSD 3-clause license. License information is at
    https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE
*/

/*
  writerecs: write a block of query result rows to a file in IPAC, VOTable,
  CSV, TSV or JSON format.

      writerecs(outpath, format, ddlist, datalist,
                ishdr, coldesc, overflow, istail)

  ddlist holds one list per column property, in this order: names, types,
  dbtypes, formats, units, descriptions, and (last) integer widths.
  datalist holds the rows, each a list with one value per column.  ishdr
  starts a new file and writes the header; istail writes the closing
  elements.  A result is written in one or more calls.

  Names, units, descriptions and values can be any length: every string is
  held in memory sized to it, and every formatted write is bounded.  The
  formats from the data dictionary are checked before use, and integers are
  written at full width (64 bits, or exactly from Python when larger).
*/

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>


/* Number of string rows in ddlist before the width row. */

#define NDDSTR 6

enum { FMT_IPAC, FMT_VOTABLE, FMT_CSV, FMT_TSV, FMT_JSON, FMT_OTHER };

enum { KIND_NULL, KIND_STR, KIND_NUM, KIND_SKIP };

typedef struct {
    char *name;
    char *type;
    char *dbtype;
    char *fmt;
    char *units;
    char *desc;
    int   width;
} column_t;


/* ---------------------------------------------------------------------
   Small helpers
   --------------------------------------------------------------------- */

static char *xstrdup (const char *s)
{
    size_t n = strlen (s);
    char  *d = (char *)malloc (n + 1);

    if (d != NULL)
        memcpy (d, s, n + 1);

    return d;
}


/* printf into a newly allocated string of the right size. */

static char *xprintf (const char *fmt, ...)
{
    va_list ap;
    int     n;
    char   *s;

    va_start (ap, fmt);
    n = vsnprintf (NULL, 0, fmt, ap);
    va_end (ap);

    if (n < 0)
        return NULL;

    s = (char *)malloc ((size_t)n + 1);
    if (s == NULL)
        return NULL;

    va_start (ap, fmt);
    vsnprintf (s, (size_t)n + 1, fmt, ap);
    va_end (ap);

    return s;
}


/*
   Return the UTF-8 text of a Python str, or "" for anything else (or a str
   that cannot be encoded).  The pointer belongs to the Python object.
*/

static const char *utf8_or_empty (PyObject *obj)
{
    const char *s;

    if (obj == NULL || !PyUnicode_Check (obj))
        return "";

    s = PyUnicode_AsUTF8 (obj);
    if (s == NULL) {
        PyErr_Clear ();
        return "";
    }
    return s;
}


/*
   Split a printf conversion spec from the data dictionary, such as "14.6f",
   "24ld" or "+12s", into its flags/width/precision part and its conversion
   character.  Length modifiers are dropped; the caller supplies the right
   one.  Returns 0 if the spec is not a single, simple conversion.
*/

static int parse_spec (const char *spec, char *body, size_t bodysz,
                       char *conv)
{
    const char *p = spec;
    size_t      n;

    while (*p && strchr ("-+ #0", *p))
        ++p;
    while (isdigit ((unsigned char)*p))
        ++p;
    if (*p == '.') {
        ++p;
        while (isdigit ((unsigned char)*p))
            ++p;
    }

    n = (size_t)(p - spec);
    if (n + 1 > bodysz)
        return 0;

    while (*p == 'l' || *p == 'h')
        ++p;

    if (*p == '\0' || p[1] != '\0' || !isalpha ((unsigned char)*p))
        return 0;

    memcpy (body, spec, n);
    body[n] = '\0';
    *conv = *p;
    return 1;
}


/* XML attribute value: escape the five special characters. */

static void write_xml_attr (FILE *fp, const char *s)
{
    for (; *s; ++s) {
        switch (*s) {
            case '&':  fputs ("&amp;",  fp); break;
            case '<':  fputs ("&lt;",   fp); break;
            case '>':  fputs ("&gt;",   fp); break;
            case '"':  fputs ("&quot;", fp); break;
            case '\'': fputs ("&apos;", fp); break;
            default:
                if ((unsigned char)*s < 0x20 &&
                    *s != '\t' && *s != '\n' && *s != '\r')
                    fputs ("\xEF\xBF\xBD", fp);      /* U+FFFD */
                else
                    fputc (*s, fp);
        }
    }
}


/*
   Body of a CDATA section.  "]]>" would end the section early, so it is
   split across two sections; control characters that XML does not allow
   at all become U+FFFD.
*/

static void write_cdata_body (FILE *fp, const char *s)
{
    for (; *s; ++s) {
        if (s[0] == ']' && s[1] == ']' && s[2] == '>') {
            fputs ("]]]]><![CDATA[>", fp);
            s += 2;
        }
        else if ((unsigned char)*s < 0x20 &&
                 *s != '\t' && *s != '\n' && *s != '\r')
            fputs ("\xEF\xBF\xBD", fp);
        else
            fputc (*s, fp);
    }
}


/* JSON string, with its quotes. */

static void write_json_string (FILE *fp, const char *s)
{
    fputc ('"', fp);

    for (; *s; ++s) {
        unsigned char c = (unsigned char)*s;

        switch (c) {
            case '"':  fputs ("\\\"", fp); break;
            case '\\': fputs ("\\\\", fp); break;
            case '\b': fputs ("\\b",  fp); break;
            case '\f': fputs ("\\f",  fp); break;
            case '\n': fputs ("\\n",  fp); break;
            case '\r': fputs ("\\r",  fp); break;
            case '\t': fputs ("\\t",  fp); break;
            default:
                if (c < 0x20)
                    fprintf (fp, "\\u%04x", c);
                else
                    fputc (c, fp);
        }
    }

    fputc ('"', fp);
}


/* CSV field, quoted, with quotes inside it doubled (RFC 4180). */

static void write_csv_quoted (FILE *fp, const char *s)
{
    fputc ('"', fp);

    for (; *s; ++s) {
        if (*s == '"')
            fputc ('"', fp);
        fputc (*s, fp);
    }

    fputc ('"', fp);
}


/* CSV header name: quoted only when it has to be, so plain names are bare. */

static void write_csv_name (FILE *fp, const char *s)
{
    if (strpbrk (s, ",\"\r\n") != NULL)
        write_csv_quoted (fp, s);
    else
        fputs (s, fp);
}


/* VOTable datatype for an internal type name. */

static const char *votable_datatype (const char *type)
{
    if (strcasecmp (type, "longlong") == 0 || strcasecmp (type, "bigint") == 0)
        return "long";

    if (strcasecmp (type, "integer") == 0)
        return "int";

    return type;
}


static int is_string_col (const column_t *c)
{
    return (strcasecmp (c->type,   "char")      == 0) ||
           (strcasecmp (c->type,   "date")      == 0) ||
           (strcasecmp (c->dbtype, "timestamp") == 0);
}


static int is_int_col (const column_t *c)
{
    return (strcasecmp (c->type, "int")      == 0) ||
           (strcasecmp (c->type, "long")     == 0) ||
           (strcasecmp (c->type, "longlong") == 0) ||
           (strcasecmp (c->type, "bigint")   == 0) ||
           (strcasecmp (c->type, "integer")  == 0);
}


static int is_double_col (const column_t *c)
{
    return (strcasecmp (c->type, "float")  == 0) ||
           (strcasecmp (c->type, "double") == 0);
}


/* ---------------------------------------------------------------------
   Column metadata
   --------------------------------------------------------------------- */

static void free_columns (column_t *cols, Py_ssize_t ncols)
{
    Py_ssize_t i;

    if (cols == NULL)
        return;

    for (i = 0; i < ncols; i++) {
        free (cols[i].name);
        free (cols[i].type);
        free (cols[i].dbtype);
        free (cols[i].fmt);
        free (cols[i].units);
        free (cols[i].desc);
    }
    free (cols);
}


/* Read ddlist into an array of columns.  Returns NULL with an exception set. */

static column_t *read_columns (PyObject *ddlist, Py_ssize_t *ncols_out)
{
    Py_ssize_t  nrows_dd, ncols, i, l;
    PyObject   *ddarr;
    column_t   *cols;

    if (!PyList_Check (ddlist)) {
        PyErr_SetString (PyExc_Exception, "PyList_Check (ddlist) failed.");
        return NULL;
    }

    nrows_dd = PyList_GET_SIZE (ddlist);
    if (nrows_dd <= 0) {
        PyErr_SetString (PyExc_Exception, "ddlist empty.");
        return NULL;
    }

    ddarr = PyList_GET_ITEM (ddlist, 0);
    if (!PySequence_Check (ddarr)) {
        PyErr_SetString (PyExc_Exception, "Failed PySequence_Check");
        return NULL;
    }

    ncols = PySequence_Length (ddarr);
    if (ncols < 0)
        return NULL;

    cols = (column_t *)calloc (ncols > 0 ? (size_t)ncols : 1, sizeof (column_t));
    if (cols == NULL) {
        PyErr_NoMemory ();
        return NULL;
    }

    /* Rows missing from ddlist leave their property empty. */

    for (l = 0; l < nrows_dd; l++) {

        ddarr = PyList_GET_ITEM (ddlist, l);

        if (!PySequence_Check (ddarr)) {
            PyErr_SetString (PyExc_Exception, "Failed PySequence_Check");
            free_columns (cols, ncols);
            return NULL;
        }

        if (PySequence_Length (ddarr) < ncols) {
            PyErr_SetString (PyExc_Exception,
                "ddlist row has fewer entries than there are columns.");
            free_columns (cols, ncols);
            return NULL;
        }

        for (i = 0; i < ncols; i++) {

            PyObject *item = PySequence_GetItem (ddarr, i);
            column_t *c    = &cols[i];

            if (item == NULL) {
                free_columns (cols, ncols);
                return NULL;
            }

            if (l == nrows_dd - 1) {

                /* The last row is the widths. */

                long w = 0;

                if (PyLong_Check (item)) {
                    w = PyLong_AsLong (item);
                    if (w == -1 && PyErr_Occurred ()) {
                        PyErr_Clear ();
                        w = 0;
                    }
                }
                if (w < 0 || w > 1000000)
                    w = 0;

                c->width = (int)w;
            }
            else if (l < NDDSTR) {

                const char *s = utf8_or_empty (item);
                char      **dst = NULL;

                switch (l) {
                    case 0: dst = &c->name;   break;
                    case 1: dst = &c->type;   break;
                    case 2: dst = &c->dbtype; break;
                    case 3: dst = &c->fmt;    break;
                    case 4: dst = &c->units;  break;
                    case 5: dst = &c->desc;   break;
                }

                /* Date and timestamp values are written as text. */

                if (l == 1 && ((strcasecmp (s, "date") == 0) ||
                               (strcasecmp (s, "timestamp") == 0)))
                    s = "char";

                free (*dst);
                *dst = xstrdup (s);

                if (*dst == NULL) {
                    Py_DECREF (item);
                    free_columns (cols, ncols);
                    PyErr_NoMemory ();
                    return NULL;
                }

                if (l == 0) {
                    char *p;
                    for (p = c->name; *p; ++p)
                        if (*p >= 'A' && *p <= 'Z')
                            *p = (char)(*p - 'A' + 'a');
                }
            }

            Py_DECREF (item);
        }
    }

    for (i = 0; i < ncols; i++) {
        char **f[NDDSTR] = { &cols[i].name, &cols[i].type, &cols[i].dbtype,
                             &cols[i].fmt,  &cols[i].units, &cols[i].desc };
        int    k;

        for (k = 0; k < NDDSTR; k++) {
            if (*f[k] == NULL && (*f[k] = xstrdup ("")) == NULL) {
                free_columns (cols, ncols);
                PyErr_NoMemory ();
                return NULL;
            }
        }
    }

    *ncols_out = ncols;
    return cols;
}


/* ---------------------------------------------------------------------
   Value formatting
   --------------------------------------------------------------------- */

/*
   Text of an integer value.  For IPAC the data dictionary format is used,
   with its flags, width and precision kept and the 64-bit length modifier
   supplied; other formats are written plainly.  Integers too large for 64
   bits are written exactly, as Python prints them.
*/

static char *format_int (PyObject *item, const column_t *c, int ipac)
{
    long long v;
    int       overflow = 0;
    char      body[64], conv, fmt[96];

    if (!PyLong_Check (item))
        return xstrdup ("");

    v = PyLong_AsLongLongAndOverflow (item, &overflow);

    if (v == -1 && PyErr_Occurred ()) {
        PyErr_Clear ();
        return xstrdup ("");
    }

    if (overflow) {
        PyObject   *str = PyObject_Str (item);
        const char *s   = utf8_or_empty (str);
        char       *out;

        if (str == NULL)
            PyErr_Clear ();

        out = xstrdup (s);
        Py_XDECREF (str);
        return out;
    }

    if (ipac && parse_spec (c->fmt, body, sizeof body, &conv)) {

        if (conv == 'd' || conv == 'i') {
            snprintf (fmt, sizeof fmt, "%%-%sll%c", body, conv);
            return xprintf (fmt, v);
        }
        if (conv == 'u' || conv == 'x' || conv == 'X' || conv == 'o') {
            snprintf (fmt, sizeof fmt, "%%-%sll%c", body, conv);
            return xprintf (fmt, (unsigned long long)v);
        }
    }

    if (ipac)
        return xprintf ("%-lld", v);

    return xprintf ("%lld", v);
}


/*
   Text of a floating-point value, in the data dictionary format.  Outside
   IPAC, a format with a precision loses its flags and width (only the part
   from the '.' on is kept); one without a precision is used as it is.
*/

static char *format_double (PyObject *item, const column_t *c, int ipac)
{
    double      d;
    char        body[64], conv, fmt[96];
    const char *dot;

    if (PyFloat_Check (item))
        d = PyFloat_AsDouble (item);

    else if (PyLong_Check (item)) {
        d = PyLong_AsDouble (item);
        if (d == -1.0 && PyErr_Occurred ()) {
            PyErr_Clear ();
            return xstrdup ("");
        }
    }
    else
        return xstrdup ("");

    if (parse_spec (c->fmt, body, sizeof body, &conv) &&
        strchr ("eEfFgGaA", conv) != NULL) {

        if (!ipac && (dot = strchr (body, '.')) != NULL)
            snprintf (fmt, sizeof fmt, "%%%s%c", dot, conv);
        else
            snprintf (fmt, sizeof fmt, "%%-%s%c", body, conv);
    }
    else
        snprintf (fmt, sizeof fmt, "%s", "%.17g");

    return xprintf (fmt, d);
}


/* ---------------------------------------------------------------------
   Header and trailer
   --------------------------------------------------------------------- */

static void write_votable_field (FILE *fp, const column_t *c)
{
    int ischar = (strcasecmp (c->type, "char") == 0);
    int hasdesc  = (c->desc[0]  != '\0');
    int hasunits = (c->units[0] != '\0');

    fputs ("    <FIELD ID=\"", fp);
    write_xml_attr (fp, c->name);
    fputs (ischar ? "\" arraysize=\"*\" datatype=\"" : "\" datatype=\"", fp);
    write_xml_attr (fp, votable_datatype (c->type));
    fputs ("\" name=\"", fp);
    write_xml_attr (fp, c->name);
    fputs ("\"", fp);

    if (hasunits) {
        fputs (" unit=\"", fp);
        write_xml_attr (fp, c->units);
        fputs ("\"", fp);
    }

    if (!hasdesc) {
        fputs ("/>\n", fp);
        return;
    }

    fputs (">\n", fp);
    fputs ("  <DESCRIPTION><![CDATA[ ", fp);
    write_cdata_body (fp, c->desc);
    fputs (" ]]></DESCRIPTION>\n", fp);
    fputs (ischar ? "</FIELD>\n" : "    </FIELD>\n", fp);
}


static void write_header (FILE *fp, int ofmt, const column_t *cols,
                          Py_ssize_t ncols, int overflow)
{
    Py_ssize_t i;

    if (ofmt == FMT_IPAC) {

        fputs ("|", fp);
        for (i = 0; i < ncols; i++)
            fprintf (fp, "%-*s|", cols[i].width, cols[i].name);
        fputs ("\n", fp);

        fputs ("|", fp);
        for (i = 0; i < ncols; i++)
            fprintf (fp, "%-*s|", cols[i].width, cols[i].type);
        fputs ("\n", fp);

        fputs ("|", fp);
        for (i = 0; i < ncols; i++)
            fprintf (fp, "%-*s|", cols[i].width, cols[i].units);
        fputs ("\n", fp);

        fputs ("|", fp);
        for (i = 0; i < ncols; i++)
            fprintf (fp, "%-*s|", cols[i].width, "null");
        fputs ("\n", fp);
    }
    else if (ofmt == FMT_VOTABLE) {

        fputs ("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n", fp);
        fputs ("<VOTABLE version=\"1.3\" "
               "xmlns=\"http://www.ivoa.net/xml/VOTable/v1.3\" "
               "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
               "xsi:noNamespaceSchemaLocation="
               "\"http://www.ivoa.net/xml/VOTable/v1.3\">\n", fp);
        fputs ("  <RESOURCE type=\"results\">\n", fp);

        if (overflow)
            fputs ("  <INFO name=\"QUERY_STATUS\" value=\"OVERFLOW\"/>\n", fp);
        else
            fputs ("  <INFO name=\"QUERY_STATUS\" value=\"OK\"/>\n", fp);

        fputs ("  <TABLE>\n", fp);

        for (i = 0; i < ncols; i++)
            write_votable_field (fp, &cols[i]);
    }
    else if (ofmt == FMT_CSV || ofmt == FMT_TSV) {

        for (i = 0; i < ncols; i++) {
            if (ofmt == FMT_CSV)
                write_csv_name (fp, cols[i].name);
            else
                fputs (cols[i].name, fp);

            if (i < ncols - 1)
                fputc (ofmt == FMT_CSV ? ',' : '\t', fp);
        }
        fputs ("\n", fp);
    }
    else if (ofmt == FMT_JSON) {
        fputs ("[\n", fp);
    }

    fflush (fp);
}


/* ---------------------------------------------------------------------
   One cell
   --------------------------------------------------------------------- */

/*
   Write one cell.  For IPAC, a value wider than its column is an error:
   returns -1 with an exception set (the message names the column, row,
   value and format).
*/

static int write_cell (FILE *fp, int ofmt, const column_t *c,
                       Py_ssize_t i, Py_ssize_t ncols, Py_ssize_t row,
                       int kind, const char *text)
{
    int last = (i == ncols - 1);

    if (kind == KIND_SKIP)
        return 0;

    if (ofmt == FMT_IPAC) {

        if (kind == KIND_NULL) {
            fprintf (fp, "%-*s ", c->width, "null");
        }
        else {
            char *cell = NULL;
            char  body[64], conv, fmt[96];

            /* Strings use the dd format; numbers are already formatted. */

            if (kind == KIND_STR &&
                parse_spec (c->fmt, body, sizeof body, &conv) &&
                conv == 's') {
                snprintf (fmt, sizeof fmt, "%%-%ss ", body);
                cell = xprintf (fmt, text);
            }
            else
                cell = xprintf ("%-*s ", c->width, text);

            if (cell == NULL) {
                PyErr_NoMemory ();
                return -1;
            }

            if (strlen (cell) > (size_t)c->width + 1) {
                PyErr_Format (PyExc_Exception,
                    "Column %zd [%s], row %zd: Formatted value [%s](%s) -> "
                    "%zd characters,  overflows column width [%d char].",
                    i, c->name, row, text, c->fmt,
                    (Py_ssize_t)strlen (cell), c->width);
                free (cell);
                return -1;
            }

            fputs (cell, fp);
            free (cell);
        }

        if (last)
            fputs ("\n", fp);
    }
    else if (ofmt == FMT_VOTABLE) {

        if (kind == KIND_NULL)
            fputs ("        <TD></TD>\n", fp);

        else if (kind == KIND_STR) {
            fputs ("        <TD><![CDATA[", fp);
            write_cdata_body (fp, text);
            fputs ("]]></TD>\n", fp);
        }
        else
            fprintf (fp, "        <TD>%s</TD>\n", text);
    }
    else if (ofmt == FMT_CSV || ofmt == FMT_TSV) {

        if (kind == KIND_STR && ofmt == FMT_CSV)
            write_csv_quoted (fp, text);

        else if (kind != KIND_NULL)
            fputs (text, fp);

        if (last)
            fputs ("\n", fp);
        else
            fputc (ofmt == FMT_CSV ? ',' : '\t', fp);
    }
    else if (ofmt == FMT_JSON) {

        if (i == 0)
            fputs ("{", fp);

        write_json_string (fp, c->name);
        fputs (": ", fp);

        if (kind == KIND_STR)
            write_json_string (fp, text);

        else if (kind == KIND_NUM && text[0] != '\0')
            fputs (text, fp);

        else
            fputs ("null", fp);

        if (!last)
            fputs (",\n", fp);
        else
            fputs ("}", fp);
    }

    return 0;
}


/* ---------------------------------------------------------------------
   Entry point
   --------------------------------------------------------------------- */

static PyObject *method_writerecs (PyObject *self, PyObject *args)
{
    PyObject   *ddlist   = NULL;
    PyObject   *datalist = NULL;

    const char *outpath  = NULL;
    const char *outfmt   = NULL;

    int ishdr, coldesc, overflow, istail;
    int ofmt;

    column_t   *cols = NULL;
    Py_ssize_t  ncols = 0, nrows_data, i, l;

    FILE       *fp = NULL;
    PyObject   *result = NULL;

    (void)self;

    if (!PyArg_ParseTuple (args, "ssOOiiii", &outpath, &outfmt,
            &ddlist, &datalist, &ishdr, &coldesc, &overflow, &istail)) {

        PyErr_SetString (PyExc_Exception, "parseTuple error");
        return NULL;
    }

    (void)coldesc;    /* column descriptions in IPAC: not implemented */

    if      (strcasecmp (outfmt, "ipac")    == 0) ofmt = FMT_IPAC;
    else if (strcasecmp (outfmt, "votable") == 0) ofmt = FMT_VOTABLE;
    else if (strcasecmp (outfmt, "csv")     == 0) ofmt = FMT_CSV;
    else if (strcasecmp (outfmt, "tsv")     == 0) ofmt = FMT_TSV;
    else if (strcasecmp (outfmt, "json")    == 0) ofmt = FMT_JSON;
    else                                          ofmt = FMT_OTHER;

    if (!PyList_Check (datalist)) {
        PyErr_SetString (PyExc_Exception, "PyList_Check (datalist) failed.");
        return NULL;
    }
    nrows_data = PyList_GET_SIZE (datalist);

    cols = read_columns (ddlist, &ncols);
    if (cols == NULL)
        return NULL;

    if (ishdr) {
        fp = fopen (outpath, "w+");
        chmod (outpath, 0664);
    }
    else
        fp = fopen (outpath, "a+");

    if (fp == NULL) {
        PyErr_Format (PyExc_Exception,
            "Failed to open filepath: [%s](errno:%d)", outpath, errno);
        goto done;
    }

    if (ishdr)
        write_header (fp, ofmt, cols, ncols, overflow);


    /* An empty result: close the document and stop. */

    if (istail == 1 && nrows_data == 0 && ishdr == 1) {

        if (ofmt == FMT_VOTABLE) {
            fputs ("  </TABLE>\n", fp);
            fputs ("  </RESOURCE>\n", fp);
            fputs ("</VOTABLE>\n", fp);
        }
        else if (ofmt == FMT_JSON)
            fputs ("]\n", fp);

        result = PyLong_FromLong (0);
        goto done;
    }

    if (ofmt == FMT_VOTABLE && ishdr == 1) {
        fputs ("    <DATA>\n", fp);
        fputs ("      <TABLEDATA>\n", fp);
    }


    /* Rows */

    for (l = 0; l < nrows_data; l++) {

        PyObject *dataarr = PyList_GET_ITEM (datalist, l);

        if (!PyList_Check (dataarr)) {
            PyErr_SetString (PyExc_Exception, "Failed PyList_Check (dataarr)");
            goto done;
        }

        if (PyList_GET_SIZE (dataarr) < ncols) {
            PyErr_Format (PyExc_Exception,
                "Row %zd has %zd values for %zd columns.",
                l, PyList_GET_SIZE (dataarr), ncols);
            goto done;
        }

        /*
           JSON rows are separated by ",\n" written before each row but the
           first, and the trailer starts with "\n".  The separator does not
           depend on whether more rows follow, so a call that only closes
           the document (no rows, istail) still leaves valid JSON.
        */

        if (ofmt == FMT_IPAC)
            fputs (" ", fp);
        else if (ofmt == FMT_VOTABLE)
            fputs ("        <TR>\n", fp);
        else if (ofmt == FMT_JSON && !(ishdr && l == 0))
            fputs (",\n", fp);

        for (i = 0; i < ncols; i++) {

            PyObject       *item = PyList_GET_ITEM (dataarr, i);
            const column_t *c    = &cols[i];
            char           *owned = NULL;
            const char     *text  = "";
            int             kind, rc;

            if (item == Py_None)
                kind = KIND_NULL;

            else if (is_string_col (c)) {
                kind = KIND_STR;
                text = utf8_or_empty (item);
            }
            else if (is_int_col (c)) {
                kind  = KIND_NUM;
                owned = format_int (item, c, ofmt == FMT_IPAC);
            }
            else if (is_double_col (c)) {
                kind  = KIND_NUM;
                owned = format_double (item, c, ofmt == FMT_IPAC);
            }
            else
                kind = KIND_SKIP;

            if (kind == KIND_NUM) {
                if (owned == NULL) {
                    PyErr_NoMemory ();
                    goto done;
                }
                text = owned;
            }

            rc = write_cell (fp, ofmt, c, i, ncols, l, kind, text);
            free (owned);

            if (rc != 0)
                goto done;
        }

        if (ofmt == FMT_VOTABLE) {
            fputs ("        </TR>\n", fp);
            fflush (fp);
        }
    }


    /* Trailer */

    if (istail == 1) {
        if (ofmt == FMT_VOTABLE) {
            fputs ("      </TABLEDATA>\n", fp);
            fputs ("    </DATA>\n", fp);
            fputs ("  </TABLE>\n", fp);
            fputs ("  </RESOURCE>\n", fp);
            fputs ("</VOTABLE>\n", fp);
        }
        else if (ofmt == FMT_JSON)
            fputs ("\n]\n", fp);     /* rows were written: see the row loop */
    }

    result = PyLong_FromLong (0);

done:
    if (fp != NULL) {
        fflush (fp);
        fclose (fp);
    }
    free_columns (cols, ncols);
    return result;
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
