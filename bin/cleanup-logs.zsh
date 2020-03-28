#! /bin/zsh

DAYSTOKEEP=7
LOGDIR=/var/tmp
CDATE=`date '+%Y%m%d'`

cd $LOGDIR ;

# Zip old files
foreach i in `find . -name "mws_*.txt" 2>/dev/null | xargs`
do
    # Get date and age of log
    DATE=`echo $i | awk -F. '{print $3}'|awk -F_ '{print $1}'`

    D1=$(date -d "$CDATE" +%s)
    D2=$(date -d "$DATE" +%s)
    AGE=$(( ($D1 - $D2) / 86400 ))

    # if date > $DAYSTOKEEP, zip it, else purge it
    if [[ $AGE -lt $DAYSTOKEEP ]] ; then
        tar -cvzf $i.tar.gz $i > /dev/null
    fi
    rm -f $i
done

# Delete old zip files
foreach i in `find . -name "mws_*.tar.gz" 2>/dev/null | xargs`
do
    # Get date and age of log
    DATE=`echo $i | awk -F. '{print $3}'|awk -F_ '{print $1}'`

    D1=$(date -d "$CDATE" +%s)
    D2=$(date -d "$DATE" +%s)
    AGE=$(( ($D1 - $D2) / 86400 ))

    # if date > $DAYSTOKEEP, zip it, else purge it
    if [[ $AGE -gt $DAYSTOKEEP ]] ; then
        rm -f $i
    fi
done

