package MKPSKU ;

use strict ;
use warnings ;
use vars qw(@ISA @EXPORT) ;
require Exporter;


@ISA = qw (Exporter);
@EXPORT = qw ( validate_or_insert_sku );

use constant DEFAULT_VENDOR_NAME => "Unknown" ;
use constant DEFAULT_TITLE       => "Unknown" ;
use constant DEFAULT_DESCRIPTION => "Unknown" ;


use IO::Handle ;
use Data::Dumper ;
use POSIX ;

# AMZL Specific Libraries
use File::Basename qw(dirname basename) ;
use Cwd qw(abs_path) ;
use lib &dirname(&abs_path($0)) . "/lib" ;
use MKPTimer ;
use MKPDatabase ;

# mysql> desc skus;
# +---------------+--------------+------+-----+-------------------+-----------------------------+
# | Field         | Type         | Null | Key | Default           | Extra                       |
# +---------------+--------------+------+-----+-------------------+-----------------------------+
# | sku           | varchar(20)  | NO   | PRI | NULL              |                             |
# | vendor_name   | varchar(50)  | YES  | MUL | NULL              |                             |
# | title         | varchar(150) | YES  |     | NULL              |                             |
# | description   | varchar(500) | YES  |     | NULL              |                             |
# | latest_user   | varchar(30)  | YES  |     | NULL              |                             |
# | latest_update | timestamp    | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user | varchar(30)  | YES  |     | NULL              |                             |
# | creation_date | timestamp    | NO   |     | CURRENT_TIMESTAMP |                             |
# +---------------+--------------+------+-----+-------------------+-----------------------------+
use constant SKUS_SELECT_STATEMENT => qq( select sku from skus where sku = ? ) ;
use constant SKUS_INSERT_STATEMENT => qq( insert into skus ( sku, vendor_name, title, description ) value ( ?, ?, ?, ? ) ) ;

# mysql> desc sku_case_packs ;
# +---------------+------------------+------+-----+-------------------+-----------------------------+
# | Field         | Type             | Null | Key | Default           | Extra                       |
# +---------------+------------------+------+-----+-------------------+-----------------------------+
# | sku           | varchar(20)      | NO   | PRI | NULL              |                             |
# | vendor_sku    | varchar(20)      | NO   | PRI | NULL              |                             |
# | pack_size     | int(10) unsigned | NO   | PRI | NULL              |                             |
# | latest_user   | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +---------------+------------------+------+-----+-------------------+-----------------------------+
use constant SKU_CASE_PACKS_SELECT_STATEMENT => qq( select sku, vendor_sku, pack_size from sku_case_packs where sku = ? ) ;
use constant SKU_CASE_PACKS_UPDATE_STATEMENT => qq( update sku_case_packs set vendor_sku = ?, pack_size = ? where sku = ? ) ;
use constant SKU_CASE_PACKS_INSERT_STATEMENT => qq( insert into sku_case_packs ( sku, vendor_sku, pack_size ) value ( ?, ?, ? ) ) ;

#
# Validate a SKU exists or inert
sub validate_or_insert_sku
{
    my $sku         = shift ;
    my $vendor_name = shift || ${\DEFAULT_VENDOR_NAME} ;
    my $title       = shift || ${\DEFAULT_TITLE}       ;
    my $description = shift || ${\DEFAULT_DESCRIPTION} ;
    my $s_stmt = $mkpDB->prepare(${\SKUS_SELECT_STATEMENT}) ;

    # Check if it exists; else insert
    $s_stmt->execute( $sku ) or die $s_stmt->errstr ;
    if( $s_stmt->rows == 0 )
    {
        my $i_stmt = $mkpDB->prepare(${\SKUS_INSERT_STATEMENT}) ;
        if( not $i_stmt->execute( $sku, $vendor_name, $title, $description ) )
        {
            print STDERR "Failed to insert " . $sku->{sku} . ", with error: " . $i_stmt->errstr . "\n" ;
        }
    }
}

1 ;
