#!/bin/bash


# TAP interaction functions

# We rely on two command-line programs: WGET to contact 
# remote HTTP services and XMLLINT to extract info from
# XML files. These can be replaced with any other tools
# that provide the same functionality.

# -------------------------------------------------------

# Submit a query to the TAP server.  This is actually
# two-step since the original submission returns a redirect
# but that is hidden from the user since wget automatically
# follows the redirect and returns the status.xml contents.

baseURL="https://exoplanetarchive.ipac.caltech.edu/TAP"

mode="/unknown"

submit()
{
    local query="$@"

    local url="$baseURL$query"

    wget -q -O status.xml "$url"

    if [ "$mode" = "/async" ]; then

        jobid=$(sed 's/uws://g' status.xml | \
               xmllint --xpath '/job/jobId' - | \
                sed 's/<[\/]*jobId>//g')
    else
        jobid="none"
    fi

    echo "$jobid"
}


# We normally submit the query without telling it to run
# immediately, so we must follow up the submission with 
# a "RUN" directive.

execute()
{
    local url="$baseURL$mode/$jobid/phase?PHASE=RUN"

    wget -q -O status.xml $url
}


# We don't need this for our processing here, but for completeness
# we include a method to retrieve the current status.xml.
# (or errored).

get_status()
{
    local url="$baseURL$mode/$jobid"

    wget -q  -O status.xml $url
}


# We can query the phase of the process at any time but usually
# use this for polling an executing job to see if it has completed
# (or errored).

get_phase()
{
    local url="$baseURL$mode/$jobid/phase"

    phase=$(wget -qO- $url)

    echo "$phase"
}


# If the job errors, we want to get the error message as the
# result.

get_errorMsg()
{
    get_status

    # local url="$baseURL$mode/$jobid/error"
    #
    # local message=$(wget -qO- $url)

    local message=$(sed 's/uws://g' status.xml | \
        xmllint --xpath '/job/errorSummary/message' - | \
        sed 's/<[\/]*message>//g')

    echo "$message" > results.dat

    echo "$message"
}


# If the job completes, we want the download the result data
# table.

get_dataUrl()
{
    local url="$baseURL$mode/$jobid/results/result"

    wget -q -O results.dat $url
}

# -------------------------------------------------------

# This is the main program.  It uses the above functions to submit 
# a TAP request, monitor its progress, and return either the data
# or the error status.

line="$@"

# Example: line="test01:  /sync?query=select ra,dec,pl_name from ps where ra<4.1&format=csv"


name=$(echo $line | cut -d':' -f 1)
query=$(echo $line | cut -d':' -f 2-)
mode=$(echo $query | cut -d'?' -f 1)

echo ""
echo "NAME:  $name"
echo "QUERY: $query"
echo "MODE:  $mode"

# Submit the query

rm -f status.xml

jobid=$(submit $query)


# Process the two modes differently

# /sync mode

if [ "$mode" = "/sync" ]; then  
    echo "mv status.xml results.dat"
    mv status.xml results.dat

# /async mode

else
    echo ""
    echo "JOB ID: $jobid"

    echo ""
    echo "INITIAL status.xml:"
    echo ""
    cat status.xml
    echo ""

    execute

    while true; do

        sleep 1

        phase=$(get_phase)
        echo "$phase"

        if [ "$phase" = "ERROR" ]; then
            echo ""
            echo "Error message:"
            echo ""
            err=$(get_errorMsg)
            echo "$err"
            break
        fi

        if [ "$phase" = "COMPLETED" ]; then
            get_dataUrl
            echo ""
            echo "Query completed. Data saved to results.dat"
            break
        fi
    done

    echo ""
    echo "FINAL status.xml:"
    echo ""
    get_status
    cat status.xml
    echo ""
fi


# For both /sync and /async

if [[ ! -f results.dat.orig ]]
then
    echo "Saving to results.dat.orig also."
    cp results.dat results.dat.orig
else
    echo "Checking against results.dat.orig."
    diffout=$(diff results.dat.orig results.dat)

    echo ""
    if [ "$diffout" = "" ]; then
        echo "TEST STATUS> $name:  No diff"
    else
        echo "TEST STATUS> $name:  New result differs from old result."
    fi

fi
