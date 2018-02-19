#!/usr/bin/zsh

REPORT_GET=/home/ericferg/mkp/bin/get-inventory-report.pl
TEMP_DIR=/mkp/reports/temp ;
REPORT_DIR=/mkp/reports/inventory ;

cd $TEMP_DIR ;
$REPORT_GET ;

foreach i in `ls | grep -v inv-report`
do
    mv $TEMP_DIR/$i $REPORT_DIR
done



