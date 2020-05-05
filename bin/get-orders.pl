#!/usr/bin/perl -w

use strict;

use Try::Tiny ;
use Amazon::MWS::Client ;
use Amazon::MWS::Exception ;
use Error ;
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
use MKPMWS ;
use MKPDatabase ;

use constant SELECT_ORDERS => qq(
) ;
use constant INSERT_ORDERS => qq(
) ;
use constant UPDATE_ORDERS => qq(
) ;

use constant SELECT_ORDER_ITEMS => qq(
) ;
use constant INSERT_ORDER_ITEMS => qq(
) ;
use constant UPDATE_ORDER_ITEMS => qq(
) ;


#
# Parse options and set defaults
my %options ;
$options{timing}   = 0 ;
$options{verbose}  = 0 ; # default

&GetOptions(
    "start=s"                => \$options{start},
    "end=s"                  => \$options{end},
    "with-items"             => \$options{with_items},
    "with-shipping"          => \$options{with_shipping},
    "dumper"                 => \$options{dumper},
    "timing|t+"              => \$options{timing},
    "status=s@"              => \$options{statuses},
    "fulfillment-channel=s@" => \$options{fulfillment_channel},
    "override"               => \$options{override},
    "verbose|v+"             => \$options{verbose},
    "usage|help|?"           => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

if( (defined $options{start} and not $options{start} =~ m/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) or
    (defined $options{end}   and not $options{end}   =~ m/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/ ))
{
    print STDERR "--start and --end must be in the following format: YYYY-MM-DD.\n" ;
    &usage_and_die(1) ;
}

my ($start, $end) ;

if(defined $options{start})
{
    $start = $options{start} ;
}
else
{
    $start = UnixDate(DateTime->now()->add(days => -7)->set_time_zone($timezone),"%Y-%m-%d") ;
}

if(Date_Cmp(DateTime->now(), $start) < 0)
{
    print STDERR "You cannot request a future date window.\n" ;
    &usage_and_die(1) ;
}

#
# Amazon doesn't let you ask for anything that isn't already 2 minutes old
if( defined $end and Date_Cmp(DateTime->now()->add(minutes => -2), $end) < 0 )
{
    print "Date window must be at least 2 minutes old, updating to pull current information.\n" if $options{verbose} ;
    $end = DateTime->now()->add(minutes => -3) ;
}

print "Window requested is $start to " . (defined $end ? $end : "<no end defined>") . "\n" if $options{verbose} ;

#
# Pull the MWS Orders
my @orders ;
my $orderItems ;
my $req ;
my @marketplaces ;
push @marketplaces, "ATVPDKIKX0DER" ;
my $arguments ;
$arguments->{CreatedAfter}       = $start if(defined $start) ;
$arguments->{CreatedBefore}      = $end   if(defined $end) ;
$arguments->{MarketplaceId}      = \@marketplaces ;
$arguments->{FulfillmentChannel} = $options{fulfillment_channel} if(defined $options{fulfillment_channel}) ;
$arguments->{OrderStatus}        = $options{statuses}            if(defined $options{statuses}) ;

print Dumper($arguments) if $options{dumper} ;
try
{
    $req = $mws->ListOrders(%$arguments) ;
}
catch
{
    die "Caught exception " . Dumper($_) . "\n" ;
} ;

while(1)
{
    my $timer = MKPTimer->new("MWS Pull", *STDOUT, $options{timing}, 1) ;

    print Dumper($req) if $options{dumper} ;
    if( exists $req->{Orders}->{Order} )
    {
        foreach my $order (@{$req->{Orders}->{Order}})
        {
            push @orders, $order ;

            print "AmazonOrderId = $order->{AmazonOrderId}\n" if $options{verbose} ;
            print "    PurchaseDate                 = " . &format_date($order->{PurchaseDate}) . "\n" if $options{verbose} ;
            print "    LatestShipDate               = " . &format_date($order->{LatestShipDate}) . "\n" if $options{verbose} ;
            print "    OrderType                    = $order->{OrderType}\n" if $options{verbose} ;
            print "    OrderTotal                   = " . &nvl($order->{OrderTotal}->{Amount}) . "\n" if $options{verbose} ;
            print "    OrderStatus                  = $order->{OrderStatus}\n" if $options{verbose} ;
            print "    LastUpdateDate               = " . &format_date($order->{LastUpdateDate}) . "\n" if $options{verbose} ;
            print "    ShipmentServiceLevelCategory = $order->{ShipmentServiceLevelCategory}\n" if $options{verbose} ;

            next if(not $options{with_items} ) ;
            my $oReq ;
            try
            {
                $oReq = $mws->ListOrderItems(AmazonOrderId => $order->{AmazonOrderId}) ;
                sleep(1) ;
            }
            catch
            {
                print "Caught exception " . Dumper($_) . "\n" ;
            } ;
            print Dumper($oReq) if $options{dumper} ;
            while(1)
            {
                if( exists $oReq->{OrderItems}->{OrderItem} )
                {
                    foreach my $item (@{&force_array($oReq->{OrderItems}->{OrderItem})})
                    {
                        print "    SKU = $item->{SellerSKU}\n" if $options{verbose} ;
                        print "        ASIN              = $item->{ASIN}\n" if $options{verbose} ;
                        print "        Title             = $item->{Title}\n" if $options{verbose} ;
                        print "        QuantityOrdered   = " . &nvl($item->{QuantityOrdered})             . "\n" if $options{verbose} ;
                        print "        QuantityShipped   = " . &nvl($item->{QuantityShipped})             . "\n" if $options{verbose} ;
                        print "        OrderItemId       = " . &nvl($item->{OrderItemId})                 . "\n" if $options{verbose} ;
                        print "        ItemPrice         = " . &nvl($item->{ItemPrice}->{Amount})         . "\n" if $options{verbose} ;
                        print "        ShippingPrice     = " . &nvl($item->{ShippingPrice}->{Amount})     . "\n" if $options{verbose} ;
                        print "        ShippingDiscount  = " . &nvl($item->{ShippingDiscount}->{Amount})  . "\n" if $options{verbose} ;
                        print "        ShippingTax       = " . &nvl($item->{ShippingTax}->{Amount})       . "\n" if $options{verbose} ;
                        print "        GiftWrapPrice     = " . &nvl($item->{GiftWrapPrice}->{Amount})     . "\n" if $options{verbose} ;
                        print "        GiftWrapTax       = " . &nvl($item->{GiftWrapTax}->{Amount})       . "\n" if $options{verbose} ;
                        print "        ItemTax           = " . &nvl($item->{ItemTax}->{Amount})           . "\n" if $options{verbose} ;
                        print "        PromotionDiscount = " . &nvl($item->{PromotionDiscount}->{Amount}) . "\n" if $options{verbose} ;
                        push @{$orderItems->{$order->{AmazonOrderId}}}, $item ;
                    }
                }
                last if not defined $oReq->{NextToken} ;
                try
                {
                    $oReq = $mws->ListOrderItemsByNextToken(NextToken => $oReq->{NextToken}) ;
                }
                catch
                {
                    print "Caught exception " . Dumper($_) . "\n" ;
                } ;
            }
        }
    }
    last if not defined $req->{NextToken} ;
    try
    {
        $req = $mws->ListOrdersByNextToken(NextToken => $req->{NextToken}) ;
    }
    catch
    {
        print "Caught exception " . Dumper($_) . "\n" ;
    } ;
}

print Dumper(\@orders)    if $options{dumper} ;
print Dumper($orderItems) if $options{dumper} ;

my $orderCount = 0 ;
my $skuCount = 0 ;
foreach my $o (@orders)
{
    if($options{with_shipping})
    {
        try
        {
            print "About to call MWS::GetEligibleShippingServices\n" ;
            my $sReq = $mws->GetEligibleShippingServices(
                'ShipmentRequestDetails.AmazonOrderId' => $o->{AmazonOrderId},
                'ShipmentRequestDetails.ShipFromAddress.Name'             => 'Eric Ferguson',
                'ShipmentRequestDetails.ShipFromAddress.AddressLine1'     => '1750 W Division St Apt 401',
                'ShipmentRequestDetails.ShipFromAddress.City'             => 'Chicago',
                'ShipmentRequestDetails.ShipFromAddress.StateOrProvince'  => 'IL',
                'ShipmentRequestDetails.ShipFromAddress.PostalCode'       => '60622',
                'ShipmentRequestDetails.ShipFromAddress.CountryCode'      => 'US',
                #'ShipmentRequestDetails.PackageDimensions.Length'         => {                type => 'nonNegativeInteger' },
                #'ShipmentRequestDetails.PackageDimensions.Wdith'          => {                type => 'nonNegativeInteger' },
                #'ShipmentRequestDetails.PackageDimensions.Height'         => {                type => 'nonNegativeInteger' },
                #'ShipmentRequestDetails.PackageDimensions.Unit'           => {                type => 'nonNegativeInteger' },
                #'ShipmentRequestDetails.PackageDimensions.PredefinedPackageDimensions' => {    type => 'string' },
                'ShipmentRequestDetails.Weight.Value' => 3,
                'ShipmentRequestDetails.Weight.Unit'  => 'ounces',
            ) ;
            print Dumper($sReq) ;
        }
        catch
        {
            print "Caught exception " . Dumper($_) . "\n" ;
        } ;
    }
    $orderCount++ ;
if( 0 )
{

    #
    # Insert or Update order
    print "Inserting/Updating Order $o->{AmazonOrderId}\n" if $options{verbose} ;
    my $localOrder ;
    my $o_sth = $mkpDB->prepare(${\SELECT_ORDERS}) ;
    $o_sth->execute($o->{AmazonOrderId}) or die "'" . $o_sth->errstr . "'\n" ;
    if( $o_sth->rows > 0 )
    {
        $localOrder = $o_sth->fetchrow_hashref() ;
        print "Found $o->{AmazonOrderId}; updating as necessary.\n" if $options{verbose} ;

        #
        # If its changed; updated it
        if( $localOrder->{order_status} ne $o->{OrderStatus} )
        {
            my $u_sth = $mkpDB->prepare(${\UPDATE_ORDERS}) ;
            if( not $u_sth->execute( $o->{ShipmentStatus},
                                     $localOrder->{id}) )
            {
                print STDERR "Failed to update amazon_orders DBI Error: \"" . $u_sth->errstr . "\"\n" ;
            }
        }
    }
    else
    {
        #
        # not found, insert it
        my $i_sth = $mkpDB->prepare(${\INSERT_ORDERS}) ;
        if( not $i_sth->execute( "www.amazon.com",
                                 $o->{ShipmentStatus},
                                 $o->{ShipmentId},
                                 $o->{ShipmentName},
                                 $o->{DestinationFulfillmentCenterId}) )
        {
            print STDERR "Failed to insert amazon_orders DBI Error: \"" . $i_sth->errstr . "\"\n" ;
        }

        my $new_sth = $mkpDB->prepare(${\SELECT_ORDERS}) ;
        $new_sth->execute($o->{AmazonOrderId}) or die "'" . $new_sth->errstr . "'\n" ;
        $localOrder = $new_sth->fetchrow_hashref() ;
    }

    #
    # Load items
    foreach my $item (@{$orderItems->{$o->{AmazonOrderId}}})
    {
        $skuCount += $item->{QuantityOrdered} ;
        print "    Inserting/Updating Amazon Order Item $item->{SellerSKU} [" . (defined $item->{QuantityOrdered} ? $item->{QuantityOrdered} : 0) . "]\n" if $options{verbose} ;
        my $o_sth = $mkpDB->prepare(${\SELECT_ORDER_ITEMS}) ;
        $o_sth->execute($localOrder->{id}, $item->{SellerSKU}) or die "'" . $o_sth->errstr . "'\n" ;
        my $localItem ;
        if( $o_sth->rows > 0 )
        {
            $localItem = $o_sth->fetchrow_hashref() ;
            print "Found $item->{SellerSKU}; updating as necessary.\n" if $options{verbose} ;

            #
            # Its changed; updated it
            if( $localItem->{quantity_shipped} != $item->{QuantityShipped} )
            {
                my $u_sth = $mkpDB->prepare(${\UPDATE_ORDER_ITEMS}) ;
                if( not $u_sth->execute( $item->{QuantityShipped},
                                         $item->{QuantityInCase},
                                         $item->{QuantityReceived},
                                         $localItem->{id},
                                         $localItem->{sku} ) )
                {
                    print STDERR "Failed to update amazon_order_items DBI Error: \"" . $u_sth->errstr . "\"\n" ;
                }
            }
        }
        else
        {
            #
            # Not found, insert it
            my $i_sth = $mkpDB->prepare(${\INSERT_ORDER_ITEMS}) ;
            if( not $i_sth->execute( $item->{SellerSKU},
                                     $localOrder->{id},
                                     $item->{QuantityShipped},
                                     $item->{QuantityInCase},
                                     $item->{QuantityReceived}) )
            {
                print STDERR "Failed to insert amazon_order_items DBI Error: \"" . $i_sth->errstr . "\"\n" ;
            }
        }
    }
} # if 0
}

print "Found $orderCount orders" . ($options{with_items} ? ", with $skuCount units" : "") . ".\n" ;

sub usage_and_die
{
    my $rc = shift || 0 ;
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program downloads all orders

usage: $0 [options]
--start
--end   
--usage|help|?  print this help
USAGE
    exit($rc) ;
}

__END__
