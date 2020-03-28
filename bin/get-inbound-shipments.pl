#!/usr/bin/perl -w

use strict;

use Amazon::MWS::Client ;
use DateTime ;
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

# mysql> desc inbound_shipments ;
# +-------------------+------------------+------+-----+-------------------+-----------------------------+
# | Field             | Type             | Null | Key | Default           | Extra                       |
# +-------------------+------------------+------+-----+-------------------+-----------------------------+
# | id                | int(10) unsigned | NO   | PRI | NULL              | auto_increment              |
# | source_name       | varchar(50)      | NO   | MUL | NULL              |                             |
# | condition_name    | varchar(50)      | NO   |     | NULL              |                             |
# | ext_shipment_id   | varchar(50)      | NO   |     | NULL              |                             |
# | ext_shipment_name | varchar(50)      | NO   |     | NULL              |                             |
# | destination       | varchar(50)      | NO   |     | NULL              |                             |
# | latest_user       | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update     | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user     | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date     | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +-------------------+------------------+------+-----+-------------------+-----------------------------+
use constant SELECT_INBOUND_SHIPMENTS => qq(
    select id
           , source_name
           , condition_name
           , ext_shipment_id
           , ext_shipment_name
           , destination
      from inbound_shipments
     where ext_shipment_id = ?
) ;
use constant INSERT_INBOUND_SHIPMENTS => qq(
    insert into inbound_shipments ( source_name , condition_name , ext_shipment_id , ext_shipment_name, destination )
    values ( ?, ?, ?, ?, ? )
) ;
use constant UPDATE_INBOUND_SHIPMENTS => qq(
    update inbound_shipments
       set condition_name = ?
     where id = ?
) ;

# +---------------------+------------------+------+-----+-------------------+-----------------------------+
# | Field               | Type             | Null | Key | Default           | Extra                       |
# +---------------------+------------------+------+-----+-------------------+-----------------------------+
# | id                  | int(10) unsigned | NO   | PRI | NULL              | auto_increment              |
# | sku                 | varchar(20)      | NO   | MUL | NULL              |                             |
# | inbound_shipment_id | int(10) unsigned | NO   | MUL | NULL              |                             |
# | quantity_shipped    | int(10) unsigned | NO   |     | NULL              |                             |
# | quantity_in_case    | int(10) unsigned | NO   |     | NULL              |                             |
# | quantity_received   | int(10) unsigned | NO   |     | NULL              |                             |
# | latest_user         | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update       | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user       | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date       | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +---------------------+------------------+------+-----+-------------------+-----------------------------+
use constant SELECT_INBOUND_SHIPMENT_ITEMS => qq(
    select id
           , sku
           , inbound_shipment_id
           , quantity_shipped
           , quantity_in_case
           , quantity_received
      from inbound_shipment_items
     where inbound_shipment_id = ?
       and sku = ?
) ;
use constant INSERT_INBOUND_SHIPMENT_ITEMS => qq(
    insert into inbound_shipment_items ( sku, inbound_shipment_id, quantity_shipped, quantity_in_case, quantity_received)
    values ( ?, ?, ?, ?, ? )
) ;
use constant UPDATE_INBOUND_SHIPMENT_ITEMS => qq(
    update inbound_shipment_items
       set quantity_shipped = ?
           , quantity_in_case = ?
           , quantity_received = ?
     where id = ?
       and sku = ?
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
    "dumper"         => \$options{dumper},
    "timing|t+"      => \$options{timing},
    "status=s@"      => \$options{statuses},
    "override"       => \$options{override},
    "verbose|v+"     => \$options{verbose},
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;


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
    my $ldate = UnixDate(DateTime->now()->set_time_zone($timezone),"%Y%m%d_%H%M%S") ;
    $credentials->{logfile} = "/var/tmp/mws_inbound-log.$ldate.txt" ;
    $credentials->{debug} = 1 ;
    $mws = Amazon::MWS::Client->new(%$credentials) ;
}

my @shipmentStatuses = ("WORKING","SHIPPED","IN_TRANSIT","DELIVERED","CHECKED_IN","RECEIVING") ;

#
# Check to see if the user has overwritten the default statuses
if( defined $options{statuses} )
{
    @shipmentStatuses = () ;
    foreach my $s (@{$options{statuses}})
    {
        push @shipmentStatuses, $s ;
    }
}

#
# Pull the MWS Inbound Shipment Information
my @shipments ;
my $shipmentItems ;
my $req = $mws->ListInboundShipments(ShipmentStatusList => \@shipmentStatuses) ;
while(1)
{
    my $timer = MKPTimer->new("MWS Pull", *STDOUT, $options{timing}, 1) ;

    if( exists $req->{ShipmentData}->{member} )
    {
        foreach my $shipment (@{&force_array($req->{ShipmentData}->{member})})
        {
            print "ShipmentId = $shipment->{ShipmentId} \n" if $options{verbose} > 0 ;
            print "\tShipmentStatus = $shipment->{ShipmentStatus}\n" if $options{verbose} > 1 ;
            print "\tShipmentName   = '$shipment->{ShipmentName}'\n" if $options{verbose} > 1 ;
            print "\tDestinationFulfillmentCenterId = $shipment->{DestinationFulfillmentCenterId}  \n" if $options{verbose} > 1 ;
            print "\tShipFromAddress = " . $shipment->{ShipFromAddress}->{Name}                . " " .
                                           $shipment->{ShipFromAddress}->{AddressLine1}        . " " .
                                           $shipment->{ShipFromAddress}->{City}                . ", " .
                                           $shipment->{ShipFromAddress}->{StateOrProvinceCode} . " " .
                                           $shipment->{ShipFromAddress}->{PostalCode}          . " " .
                                           $shipment->{ShipFromAddress}->{CountryCode}         . "\n" if $options{verbose} > 1 ;

            push @shipments, $shipment ;
            my $sReq = $mws->ListInboundShipmentItems(ShipmentId => $shipment->{ShipmentId}) ;
            sleep(1) ;
            while(1)
            {
                if( exists $sReq->{ItemData}->{member} )
                {
                    foreach my $item (@{&force_array($sReq->{ItemData}->{member})})
                    {
                        print "\tSKU = $item->{SellerSKU}\n" if $options{verbose} > 1 ;
                        print "\t\tQuantityShipped       = $item->{QuantityShipped}      \n" if $options{verbose} > 2 ;
                        print "\t\tQuantityInCase        = $item->{QuantityInCase}       \n" if $options{verbose} > 2 ;
                        print "\t\tQuantityReceived      = $item->{QuantityReceived}     \n" if $options{verbose} > 2 ;
                        print "\t\tFulfillmentNetworkSKU = $item->{FulfillmentNetworkSKU}\n" if $options{verbose} > 2 ;
                        push @{$shipmentItems->{$shipment->{ShipmentId}}}, $item ;
                    }
                }
                last if not defined $sReq->{NextToken} ;
                $sReq = $mws->ListInboundShipmentItemsByNextToken(NextToken => $sReq->{NextToken}) ;
            }
        }
    }
    last if not defined $req->{NextToken} ;
    $req = $mws->ListInboundShipmentsByNextToken(NextToken => $req->{NextToken}) ;
}

print Dumper(\@shipments)    if $options{dumper} ;
print Dumper($shipmentItems) if $options{dumper} ;

foreach my $s (@shipments)
{
    #
    # Insert or Update shipment
    print "Inserting/Updating Inbound Shipment $s->{ShipmentId}\n" if $options{verbose} ;
    my $s_sth = $dbh->prepare(${\SELECT_INBOUND_SHIPMENTS}) ;
    $s_sth->execute($s->{ShipmentId}) or die "'" . $s_sth->errstr . "'\n" ;
    my $localShipment ;
    if( $s_sth->rows > 0 )
    {
        $localShipment = $s_sth->fetchrow_hashref() ;
        print "Found $s->{ShipmentId}; updating as necessary.\n" if $options{verbose} ;

        #
        # If its changed; updated it
        if( $localShipment->{condition_name} ne $s->{ShipmentStatus} )
        {
            my $u_sth = $dbh->prepare(${\UPDATE_INBOUND_SHIPMENTS}) ;
            if( not $u_sth->execute( $s->{ShipmentStatus},
                                     $localShipment->{id}) )
            {
                print STDERR "Failed to update inbound_shipments DBI Error: \"" . $u_sth->errstr . "\"\n" ;
            }
        }
    }
    else
    {
        #
        # not found, insert it
        my $i_sth = $dbh->prepare(${\INSERT_INBOUND_SHIPMENTS}) ;
        if( not $i_sth->execute( "www.amazon.com",
                                 $s->{ShipmentStatus},
                                 $s->{ShipmentId},
                                 $s->{ShipmentName},
                                 $s->{DestinationFulfillmentCenterId}) )
        {
            print STDERR "Failed to insert inbound_shipments DBI Error: \"" . $i_sth->errstr . "\"\n" ;
        }

        my $new_sth = $dbh->prepare(${\SELECT_INBOUND_SHIPMENTS}) ;
        $new_sth->execute($s->{ShipmentId}) or die "'" . $new_sth->errstr . "'\n" ;
        $localShipment = $new_sth->fetchrow_hashref() ;
    }

    #
    # Load items
    foreach my $item (@{$shipmentItems->{$s->{ShipmentId}}})
    {
        print "Inserting/Updating Inbound Shipment Item $item->{SellerSKU}\n" if $options{verbose} ;
        my $s_sth = $dbh->prepare(${\SELECT_INBOUND_SHIPMENT_ITEMS}) ;
        $s_sth->execute($localShipment->{id}, $item->{SellerSKU}) or die "'" . $s_sth->errstr . "'\n" ;
        my $localItem ;
        if( $s_sth->rows > 0 )
        {
            $localItem = $s_sth->fetchrow_hashref() ;
            print "Found $item->{SellerSKU}; updating as necessary.\n" if $options{verbose} ;

            #
            # Its changed; updated it
            if( $localItem->{quantity_received} != $item->{QuantityReceived} )
            {
                my $u_sth = $dbh->prepare(${\UPDATE_INBOUND_SHIPMENT_ITEMS}) ;
                if( not $u_sth->execute( $item->{QuantityShipped},
                                         $item->{QuantityInCase},
                                         $item->{QuantityReceived},
                                         $localItem->{id},
                                         $localItem->{sku} ) )
                {
                    print STDERR "Failed to update inbound_shipment_items DBI Error: \"" . $u_sth->errstr . "\"\n" ;
                }
            }
        }
        else
        {
            #
            # Not found, insert it
            my $i_sth = $dbh->prepare(${\INSERT_INBOUND_SHIPMENT_ITEMS}) ;
            if( not $i_sth->execute( $item->{SellerSKU},
                                     $localShipment->{id},
                                     $item->{QuantityShipped},
                                     $item->{QuantityInCase},
                                     $item->{QuantityReceived}) )
            {
                print STDERR "Failed to insert inbound_shipment_items $item->{SellerSKU} DBI Error: \"" . $i_sth->errstr . "\"\n" ;
            }
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
This program downloads all inbound shipments
    DEFAULT: "WORKING","SHIPPED","IN_TRANSIT","DELIVERED","CHECKED_IN","RECEIVING"

usage: $0 [options]
--latest        pull latest states
--status=STATUS include 'STATUS' as part of the request
--usage|help|?  print this help
USAGE
    exit($rc) ;
}

__END__
Total Shipment statuses according to http://docs.developer.amazonservices.com/en_US/fba_inbound/FBAInbound_ListInboundShipments.html
WORKING - The shipment was created by the seller, but has not yet shipped.
SHIPPED - The shipment was picked up by the carrier.
IN_TRANSIT - The carrier has notified the Amazon fulfillment center that it is aware of the shipment.
DELIVERED - The shipment was delivered by the carrier to the Amazon fulfillment center.
CHECKED_IN - The shipment was checked-in at the receiving dock of the Amazon fulfillment center.
RECEIVING - The shipment has arrived at the Amazon fulfillment center, but not all items have been marked as received.
CLOSED - The shipment has arrived at the Amazon fulfillment center and all items have been marked as received.
CANCELLED - The shipment was cancelled by the seller after the shipment was sent to the Amazon fulfillment center.
DELETED - The shipment was cancelled by the seller before the shipment was sent to the Amazon fulfillment center.
ERROR - There was an error with the shipment and it was not processed by Amazon.
