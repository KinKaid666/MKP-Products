#!/usr/bin/perl -w

use strict;

use Amazon::MWS::Client ;
use DateTime;
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

#
# Parse options and set defaults
my %options ;
$options{username} = 'mkp_loader'      ;
$options{password} = 'mkp_loader_2018' ;
$options{database} = 'mkp_products'    ;
$options{hostname} = 'localhost'       ;
$options{timing}   = 0 ;
$options{verbose}  = 0 ; # default

&GetOptions(
    "database=s"     => \$options{database},
    "username=s"     => \$options{username},
    "password=s"     => \$options{password},
    "dumper"         => \$options{dumper},
    "timing|t+"      => \$options{timing},
    "status=s@"      => \$options{statuses},
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
    $credentials->{logfile} = "/var/tmp/mws_log.txt" ;
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
my $req = $mws->ListInboundShipments(ShipmentStatusList => \@shipmentStatuses) ;
while(1)
{
    my $timer = MKPTimer->new("MWS Pull", *STDOUT, $options{timing}, 1) ;
    print Dumper($req) if $options{dumper} ;

    if( exists $req->{ShipmentData}->{member} )
    {
        foreach my $shipment (@{$req->{ShipmentData}->{member}})
        {
            print "ShipmentId = $shipment->{ShipmentId} \n" if $options{verbose} > 0 ;
            print "\tShipmentStatus = $shipment->{ShipmentStatus}\n" if $options{verbose} > 1 ;
            print "\tShipmentName   = $shipment->{ShipmentName}  \n" if $options{verbose} > 1 ;
            print "\tDestinationFulfillmentCenterId   = $shipment->{DestinationFulfillmentCenterId}  \n" if $options{verbose} > 1 ;
            print "\tShipFromAddress  = " . $shipment->{ShipFromAddress}->{Name}         . " " .
                                            $shipment->{ShipFromAddress}->{AddressLine1} . " " .
                                            $shipment->{ShipFromAddress}->{City}         . ", " .
                                            $shipment->{ShipFromAddress}->{StateOrProvinceCode} . " " .
                                            $shipment->{ShipFromAddress}->{PostalCode}   . " " .
                                            $shipment->{ShipFromAddress}->{CountryCode}  . "\n" if $options{verbose} > 1 ;

            my $sReq = $mws->ListInboundShipmentItems(ShipmentId => $shipment->{ShipmentId}) ;
            while(1)
            {
                print Dumper($sReq) if $options{dumper} ;
                if( exists $sReq->{ItemData}->{member} )
                {
                    foreach my $item (@{&force_array($sReq->{ItemData}->{member})})
                    {
                        print "\tSKU = $item->{SellerSKU}\n" if $options{verbose} > 1 ;
                        print "\t\tQuantityShipped       = $item->{QuantityShipped}      \n" if $options{verbose} > 2 ;
                        print "\t\tQuantityInCase        = $item->{QuantityInCase}       \n" if $options{verbose} > 2 ;
                        print "\t\tQuantityReceived      = $item->{QuantityReceived}     \n" if $options{verbose} > 2 ;
                        print "\t\tFulfillmentNetworkSKU = $item->{FulfillmentNetworkSKU}\n" if $options{verbose} > 2 ;
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

sub force_array
{
    my $array = shift ;

    $array = [ $array ] if( ref $array ne "ARRAY" ) ;
    return $array ;
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
--status=STATUS include 'STATUS' as part of the request
--usage|help|?  print this help
USAGE
    exit($rc) ;
}

