#!/usr/bin/perl -w

use strict;

use Try::Tiny ;
use Amazon::MWS::Client ;
use DateTime;
use Date::Manip ;
use Data::Dumper ;
use Getopt::Long ;
use DBI ;

# MKP Specific Libraries
use File::Basename qw(dirname basename) ;
use Cwd qw(abs_path) ;
use lib &dirname(&abs_path($0)) . "/lib" ;

use MKPFormatter ;
use MKPTimer ;

use constant SELECT_ORDER_CHANNEL_CREDENTIALS => qq(
    select credentials
      from order_channel_credentials
     where source_name = ?
) ;

# mysql> desc realtime_inventory ;
# +------------------+------------------+------+-----+-------------------+-----------------------------+
# | Field            | Type             | Null | Key | Default           | Extra                       |
# +------------------+------------------+------+-----+-------------------+-----------------------------+
# | sku              | varchar(20)      | NO   | PRI | NULL              |                             |
# | source_name      | varchar(50)      | NO   | PRI | NULL              |                             |
# | quantity_instock | int(10) unsigned | NO   |     | NULL              |                             |
# | quantity_total   | int(10) unsigned | NO   |     | NULL              |                             |
# | instock_date     | date             | YES  |     | NULL              |                             |
# | latest_user      | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update    | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user    | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date    | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +------------------+------------------+------+-----+-------------------+-----------------------------+
use constant SELECT_ONHAND_INVENTORY => qq(
    select sku
           , source_name
           , quantity_instock
           , quantity_total
           , instock_date
      from realtime_inventory ri
     where ri.sku = ?
) ;
use constant INSERT_ONHAND_INVENTORY => qq(
    insert into realtime_inventory ( sku, source_name, quantity_instock, quantity_total, instock_date )
     values ( ?, ?, ?, ?, ? )
) ;
use constant UPDATE_ONHAND_INVENTORY => qq(
    update realtime_inventory
       set source_name = ?
           , quantity_instock = ?
           , quantity_total = ?
           , instock_date = ?
     where sku = ?
) ;

# mysql> desc active_sources ;
# +---------------+-------------+------+-----+-------------------+-----------------------------+
# | Field         | Type        | Null | Key | Default           | Extra                       |
# +---------------+-------------+------+-----+-------------------+-----------------------------+
# | sku           | varchar(20) | NO   | PRI | NULL              |                             |
# | sku_source_id | varchar(50) | YES  |     | NULL              |                             |
# | source_name   | varchar(50) | YES  | MUL | NULL              |                             |
# | active        | tinyint(1)  | NO   |     | NULL              |                             |
# | latest_user   | varchar(30) | YES  |     | NULL              |                             |
# | latest_update | timestamp   | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user | varchar(30) | YES  |     | NULL              |                             |
# | creation_date | timestamp   | NO   |     | CURRENT_TIMESTAMP |                             |
# +---------------+-------------+------+-----+-------------------+-----------------------------+
use constant SELECT_ACTIVE_SOURCES => qq(
    select sku
           , sku_source_id
           , source_name
           , active
      from active_sources
     where sku = ?
) ;
use constant INSERT_ACTIVE_SOURCES => qq(
    insert into active_sources (sku, sku_source_id, source_name, active) values (?, ?, ?, ?)
) ;
use constant UPDATE_ACTIVE_SOURCES => qq(
    update active_sources
       set sku_source_id = ?
           , source_name = ?
           , active = ?
     where sku =?
) ;


#
# Parse options and set defaults
my %options ;
$options{username} = 'mkp_loader'      ;
$options{password} = 'mkp_loader_2018' ;
$options{database} = 'mkp_products'    ;
$options{hostname} = 'mkp.cjulnvkhabig.us-east-2.rds.amazonaws.com'       ;
$options{timing}   = 0 ;
$options{verbose}  = 0 ; # default

&GetOptions(
    "database=s"     => \$options{database},
    "username=s"     => \$options{username},
    "password=s"     => \$options{password},
    "from=s"         => \$options{from},
    "dumper"         => \$options{dumper},
    "timing|t+"      => \$options{timing},
    "verbose|v+"     => \$options{verbose},
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

if( defined $options{from} and not $options{from} =~ m/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/ )
{
    print STDERR "--from must be in the following format: YYYY-MM-DD.\n" ;
    &usage_and_die(1) ;
}

if(defined $options{from} and Date_Cmp(DateTime->now(), $options{from}) < 0)
{
    print STDERR "You cannot request a future date.\n" ;
    &usage_and_die(1) ;
}

if( not defined $options{from} )
{
    $options{from} = UnixDate(DateTime->now()->set_time_zone($timezone),"%Y-%m-%d") ;
}

# Connect to the database.
my $dbh ;
{
    my $timer = MKPTimer->new("DB Connection", *STDOUT, $options{timing}, 1) ;
    $dbh = DBI->connect("DBI:mysql:database=$options{database};host=$options{hostname}",
                       $options{username},
                       $options{password});
}

my $mws ;
{
    my $credentials ;
    my $sth = $dbh->prepare(${\SELECT_ORDER_CHANNEL_CREDENTIALS}) ;
    $sth->execute('www.amazon.com') or die $sth->errstr ;
    if( $sth->rows != 1 )
    {
        die "Found incorrect number of credentials" ;
    }
    my $string = $sth->fetchrow_hashref() ;
    foreach my $cred (split(',', $string->{credentials}))
    {
        my ($key,$value) = split('=',$cred) ;
        $value =~ s/^"(.*)"$/$1/g ;
        $credentials->{$key} = $value ;
    }
    $credentials->{logfile} = "/tmp/mws_log.txt" ;
    $credentials->{debug} = 0 ;
    $mws = Amazon::MWS::Client->new(%$credentials) ;
}

#
# Pull the MWS Inbound Shipment Information
my $inventoryItems ;

my $req ;
try
{
    $req = $mws->ListInventorySupply(QueryStartDateTime => $options{from}) ;
}
catch
{
    print "Catch exception " . Dumper($_) . "\n" ;
} ;
while(1)
{
    my $timer = MKPTimer->new("MWS Pull", *STDOUT, $options{timing}, 1) ;

    print Dumper($req) if $options{dumper} ;
    if( exists $req->{InventorySupplyList}->{member} )
    {
        foreach my $line (@{&force_array($req->{InventorySupplyList}->{member})})
        {
            print "ASIN                  = $line->{ASIN}                 \n" if $options{verbose} > 1 ;
            print "Condition             = $line->{Condition}            \n" if $options{verbose} > 1 ;
            print "InStockSupplyQuantity = $line->{InStockSupplyQuantity}\n" if $options{verbose} > 1 ;
            print "SellerSKU             = $line->{SellerSKU}            \n" if $options{verbose} > 1 ;
            print "FNSKU                 = $line->{FNSKU}                \n" if $options{verbose} > 1 ;
            print "TotalSupplyQuantity   = $line->{TotalSupplyQuantity}  \n" if $options{verbose} > 1 ;
            if(exists $line->{EarliestAvailability}->{TimepointType} and $line->{EarliestAvailability}->{TimepointType} eq "DateTime" )
            {
                $line->{EarliestAvailability}->{DateTime} = &convert_amazon_datetime($line->{EarliestAvailability}->{DateTime}) ;
            }
            else
            {
                $line->{EarliestAvailability}->{DateTime} = UnixDate(DateTime->now()->set_time_zone($timezone),"%Y-%m-%dT%H:00:00") ;
            }
            print "EarliestAvailability  = " . $line->{EarliestAvailability}->{DateTime} . "\n" if $options{verbose} > 1 ;
            print "$line->{SellerSKU} has $line->{InStockSupplyQuantity} in-stock and $line->{TotalSupplyQuantity} total.\n" if $options{verbose} ;
            print "\n" if $options{verbose} > 1 ;
            $inventoryItems->{$line->{SellerSKU}} = $line ;
        }
    }
    last if not defined $req->{NextToken} ;
    try
    {
        $req = $mws->ListInventorySupplyByNextToken(NextToken => $req->{NextToken}) ;
    }
    catch
    {
        print "Catch exception " . Dumper($_) . "\n" ;
    } ;
}

print Dumper($inventoryItems)    if $options{dumper} ;

foreach my $sku (keys %{$inventoryItems})
{
    my $timer = MKPTimer->new("Insert SKU $sku", *STDOUT, $options{timing}, 1) ;
    print "Inserting/updating sku $sku\n" if $options{verbose} ;
    my $s_sth = $dbh->prepare(${\SELECT_ONHAND_INVENTORY}) ;
    $s_sth->execute($sku) or die "'" . $s_sth->errstr . "'\n" ;
    if( $s_sth->rows > 0 )
    {
        my $localSKU = $s_sth->fetchrow_hashref() ;
        print "Found $sku; updating as necessary.\n" if $options{verbose} ;

        #
        # If the inventory has changed, update it
        if( not ($localSKU->{source_name}      eq "www.amazon.com"                                 and
                 $localSKU->{quantity_instock} eq $inventoryItems->{$sku}->{InStockSupplyQuantity} and
                 $localSKU->{quantity_total}   eq $inventoryItems->{$sku}->{TotalSupplyQuantity}   and
                 $localSKU->{instock_date}     eq $inventoryItems->{$sku}->{EarliestAvailability}->{DateTime}) )
        {
            my $u_sth = $dbh->prepare(${\UPDATE_ONHAND_INVENTORY}) ;
            if( not $u_sth->execute("www.amazon.com",
                                    $inventoryItems->{$sku}->{InStockSupplyQuantity},
                                    $inventoryItems->{$sku}->{TotalSupplyQuantity},
                                    $inventoryItems->{$sku}->{EarliestAvailability}->{DateTime},
                                    $sku) )
            {
                print STDERR "Failed to update realtime_inventory for sku $sku, DBI Error: \"" . $u_sth->errstr . "\"\n" ;
            }
        }

    }
    else
    {
        #
        # Skip new SKUs with no inventory en route or on hand
        next if $inventoryItems->{$sku}->{InStockSupplyQuantity} == 0 and $inventoryItems->{$sku}->{TotalSupplyQuantity} == 0 ;

        my $i_sth = $dbh->prepare(${\INSERT_ONHAND_INVENTORY}) ;
        if( not $i_sth->execute($inventoryItems->{$sku}->{SellerSKU},
                                "www.amazon.com",
                                $inventoryItems->{$sku}->{InStockSupplyQuantity},
                                $inventoryItems->{$sku}->{TotalSupplyQuantity},
                                $inventoryItems->{$sku}->{EarliestAvailability}->{DateTime}) )
        {
            print STDERR "Failed to insert realtime_inventroy for sku $sku, DBI Error: \"" . $i_sth->errstr . "\"\n"
        }
    }

    #
    # Update active sources
    my $as_sth = $dbh->prepare(${\SELECT_ACTIVE_SOURCES}) ;
    $as_sth->execute($sku) or die "'" . $as_sth->errstr . "'\n" ;
    if( $as_sth->rows > 0 )
    {
        #
        # Found, update the active source
        my $localAC = $as_sth->fetchrow_hashref() ;
        print "Found $sku active_source; updating as necessary.\n" if $options{verbose} ;

        #
        # If the active_source has changed, update it
        if( not ($localAC->{source_name}   eq "www.amazon.com" and
                 $localAC->{sku_source_id} eq $inventoryItems->{$sku}->{ASIN}) )
        {
            my $u_sth = $dbh->prepare(${\UPDATE_ACTIVE_SOURCES}) ;
            if( not $u_sth->execute($inventoryItems->{$sku}->{ASIN},
                                    "www.amazon.com",
                                    1,
                                    $sku) )
            {
                print STDERR "Failed to update active_sources for sku $sku, DBI Error: \"" . $u_sth->errstr . "\"\n" ;
            }
        }
    }
    else
    {
        #
        # Skip new SKUs with no inventory en route or on hand
        next if $inventoryItems->{$sku}->{InStockSupplyQuantity} == 0 and $inventoryItems->{$sku}->{TotalSupplyQuantity} == 0 ;

        #
        # Not found, insert the active source
        my $i_sth = $dbh->prepare(${\INSERT_ACTIVE_SOURCES}) ;
        if( not $i_sth->execute($inventoryItems->{$sku}->{SellerSKU},
                                $inventoryItems->{$sku}->{ASIN},
                                "www.amazon.com",
                                1) )
        {
            print STDERR "Failed to insert active_sources for sku $sku, DBI Error: \"" . $i_sth->errstr . "\"\n"
        }
    }
}

sub usage_and_die
{
    my $rc = shift || 0 ;
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program downloads current inventory

usage: $0 [options]
--from=YYYY-MM-DD pull SKUs that have changed as of the specified date [DEFAULT: today]
--usage|help|?    print this help
USAGE
    exit($rc) ;
}
