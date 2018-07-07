#! /usr/bin/zsh

DAYSTOKEEP=7
LOGDIR=/var/tmp
CDATE=`date '+%Y%m%d'`

cd $LOGDIR ;

foreach i in `ls mws_*.txt 2>/dev/null`
do
    # Get date and age of log
    DATE=`echo $i | awk -F. '{print $2}'|awk -F_ '{print $1}'`

    D1=$(date -d "$CDATE" +%s)
    D2=$(date -d "$DATE" +%s)
    AGE=$(( ($D1 - $D2) / 86400 ))
    # echo "file $DATE, age $AGE, $D1, $D2"

    # if date > $DAYSTOKEEP, zip it, else purge it
    if [[ $AGE -lt $DAYSTOKEEP ]] ; then
        # echo "Zipping $i"
        tar -cvzf $i.tar.gz $i > /dev/null
    fi
    rm -f $i
done

