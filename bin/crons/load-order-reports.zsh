#!/usr/bin/zsh

REPORT_LOADER=/home/ericferg/mkp/bin/mkp_order_laoder.pl
REPORT_DIR=/mkp/reports/orders ;
BACKUP_DIR=/mkp/loaded/orders ;

cd $REPORT_DIR ;

foreach i in `ls`
do
$REPORT_LOADER --filename $i
mv $i $BACKUP_DIR
done

