#!/usr/bin/zsh

REPORT_LOADER=/home/ericferg/mkp/bin/mkp_sku_loader.pl
REPORT_DIR=/mkp/reports/skus ;
BACKUP_DIR=/mkp/loaded/skus ;

cd $REPORT_DIR ;

foreach i in `ls`
do
$REPORT_LOADER --filename $i && mv $i $BACKUP_DIR
done

