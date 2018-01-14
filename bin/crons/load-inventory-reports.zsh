#!/usr/bin/zsh

REPORT_LOADER=/home/ericferg/mkp/bin/mkp_inventory_loader.pl
REPORT_DIR=/mkp/reports/inventory ;
BACKUP_DIR=/mkp/loaded/inventory ;

cd $REPORT_DIR ;

foreach i in `ls`
do
REPORT_DATE=`echo $i | perl -pe  's/^.*([0-9]{2})-([0-9]{2})-([0-9]{4}).*$/$3-$1-$2/'`
if [[ $REPORT_DATE = $i ]] ; then
    echo "Invalid filename: $i" ;
else
    $REPORT_LOADER --filename $i --report-date=$REPORT_DATE
fi
mv $i $BACKUP_DIR
done

