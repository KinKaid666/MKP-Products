#!/usr/bin/zsh

REPORT_LOADER=/home/ericferg/mkp/bin/mkp_sku_cost_laoder.pl
REPORT_DIR=/mkp/reports/sku_costs ;
BACKUP_DIR=/mkp/loaded/sku_costs ;

cd $REPORT_DIR ;

foreach i in `ls`
do
$REPORT_LOADER --filename $i
mv $i $BACKUP_DIR
done

