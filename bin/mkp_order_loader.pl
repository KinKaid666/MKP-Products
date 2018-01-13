#!/usr/bin/perl -w

use strict ;
use warnings;
use charnames qw(:full);
use Encode ;

binmode STDOUT, ":encoding(UTF-8)" ;


# normal use POSIX ;
use Data::Dumper ;
use Getopt::Long ;
use IO::Handle ;
use Date::Manip ;
use Date::Calc ;
use DBI;

# AMZL Specific Libraries
use File::Basename qw(dirname basename) ;
use Cwd qw(abs_path) ;
use lib &dirname(&abs_path($0)) . "/lib" ;
use MKPTimer ;

use constant ORDER_CHANNEL_QUERY     => qq(select id, source from order_channels) ;
use constant ORDERS_INSERT_STATEMENT => qq(
    insert into sku_orders ( source_id,
                             order_datetime,
                             settlement_id,
                             type,
                             source_order_id,
                             sku,
                             quantity,
                             marketplace,
                             fulfillment,
                             order_city,
                             order_state,
                             order_postal_code,
                             product_sales,
                             shipping_credits,
                             gift_wrap_credits,
                             promotional_rebates,
                             sales_tax_collected,
                             marketplace_facilitator_tax,
                             selling_fees,
                             fba_fees,
                             transaction_fees,
                             other,
                             total
    ) value ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
) ;

my %options ;
$options{username} = 'mkp_loader'      ;
$options{password} = 'mkp_loader_2018' ;
$options{database} = 'mkp_products'    ;
$options{hostname} = 'localhost'       ;
$options{timing}   = 0 ;
$options{print}    = 0 ;
$options{debug}    = 0 ; # default

&GetOptions(
    "database=s"     => \$options{database},
    "email=s"        => \$options{email},
    "filename=s"     => \$options{filename},
    "print"          => \$options{print},
    "timing"         => sub { $options{timing}++ },
    "debug"          => sub { $options{debug}++ },
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

die "You must provide a filename." if (not defined $options{filename}) ;

my @orders ;

#
# ingest file
#
# Example:
# "Includes Amazon Marketplace, Fulfillment by Amazon (FBA), and Amazon Webstore transactions"
# "All amounts in USD, unless specified"
# "Definitions:"
# "Sales tax collected: Includes sales tax collected from buyers for product sales, shipping, and gift wrap."
# "Selling fees: Includes variable closing fees and referral fees."
# "Other transaction fees: Includes shipping chargebacks, shipping holdbacks, per-item fees  and sales tax collection fees."
# "Other: Includes non-order transaction amounts. For more details, see the ""Type"" and ""Description"" columns for each order ID."
# "date/time","settlement id","type","order id","sku","description","quantity","marketplace","fulfillment","order city","order state","order postal","product sales","shipping credits","gift wrap credits","promotional rebates","sales tax collected","Marketplace Facilitator Tax","selling fees","fba fees","other transaction fees","other","total"
# "Dec 1, 2017 12:38:23 AM PST","6503097031","Order","113-0013318-7697855","MKP-FDW8652-U","Adfors FibaFuse FDW8652 Paperless Drywall Joint Tape 2 in. x 250 ft. White, Pack of 10","1","amazon.com","Amazon","BAINBRIDGE ISLAND","WA","98110-1523","57.96","0","0","0","0","0","-8.69","-10.19","0","0","39.08"
{
    my $timer = MKPTimer->new("File processing", *STDOUT, $options{timing}, 1) ;
    my $lineNumber = 0 ;
    open(INPUTFILE, $options{filename}) or die "Can't open $options{filename}: $!" ;
    while(my $line = <INPUTFILE>)
    {
        chomp($line) ;

        ++$lineNumber ;

        #
        # TODO FIGURE OUT ENCODING
        # Skip the default headers; there something
        next if $line =~ m/.*Includes Amazon Marketplace, Fulfillment by Amazon \(FBA\), and Amazon Webstore transactions.*/ ;
        #next if $line eq qq("Includes Amazon Marketplace, Fulfillment by Amazon (FBA), and Amazon Webstore transactions") ;
        next if $line eq qq("All amounts in USD, unless specified") ;
        next if $line eq qq("Definitions:") ;
        next if $line eq qq("Sales tax collected: Includes sales tax collected from buyers for product sales, shipping, and gift wrap.") ;
        next if $line eq qq("Selling fees: Includes variable closing fees and referral fees.") ;
        next if $line eq qq("Other transaction fees: Includes shipping chargebacks, shipping holdbacks, per-item fees  and sales tax collection fees.") ;
        next if $line eq qq("Other: Includes non-order transaction amounts. For more details, see the ""Type"" and ""Description"" columns for each order ID.") ;
        next if $line eq qq("date/time","settlement id","type","order id","sku","description","quantity","marketplace","fulfillment","order city","order state","order postal","product sales","shipping credits","gift wrap credits","promotional rebates","sales tax collected","Marketplace Facilitator Tax","selling fees","fba fees","other transaction fees","other","total") ;

        #
        # Amazon has quotes around every field and sometimes some empty, unquote fields
        #    First strip the leading and trailing quote
        $line =~ s/^\"(.*)\"$/$1/ ;
        #    Second if there are any empty fields (,,), make sure they are formatted correct (,"",)
        $line =~ s/,,/,"",/g ;
        #    lastly cut all the fields by ","
        my @subs = split(/","/, $line) ;

        my $orderLine ;
        $orderLine->{order_datetime}              = &format_date($subs[ 0]);
        $orderLine->{settlement_id}               = $subs[ 1] ;
        $orderLine->{type}                        = $subs[ 2] ; #unused
        $orderLine->{source_order_id}             = $subs[ 3] ;
        $orderLine->{sku}                         = $subs[ 4] ;
        $orderLine->{description}                 = $subs[ 5] ;
        $orderLine->{quantity}                    = $subs[ 6] ;
        $orderLine->{marketplace}                 = $subs[ 7] ;
        $orderLine->{fulfillment}                 = $subs[ 8] ;
        $orderLine->{order_city}                  = $subs[ 9] ;
        $orderLine->{order_state}                 = $subs[10] ;
        $orderLine->{order_postal}                = $subs[11] ;
        $orderLine->{product_sales}               = $subs[12] ;
        $orderLine->{shipping_credits}            = $subs[13] ;
        $orderLine->{gift_wrap_credits}           = $subs[14] ;
        $orderLine->{promotional_rebates}         = $subs[15] ;
        $orderLine->{sales_tax_colected}          = $subs[16] ;
        $orderLine->{marketplace_facilitator_tax} = $subs[17] ;
        $orderLine->{selling_fees}                = $subs[18] ;
        $orderLine->{fba_fees}                    = $subs[19] ;
        $orderLine->{other_transaction_fees}      = $subs[20] ;
        $orderLine->{other}                       = $subs[21] ;
        $orderLine->{total}                       = $subs[22] ;

        next if( $orderLine->{type} eq "Service Fee" or
                 $orderLine->{type} eq "FBA Inventory Fee" or
                 $orderLine->{type} eq "Transfer" or
                 $orderLine->{sku}  eq "" ) ;

        #die "invalid line $lineNumber : $line" if scalar @subs != 23 ;
        die "invalid line $lineNumber : $line" if scalar @subs != 23 ;

        print "Found on line " . $lineNumber . " Order " . $orderLine->{source_order_id} . " from " . $orderLine->{order_datetime} . " on SKU " . $orderLine->{sku} . "\n" if $options{debug} > 1 ;
        push @orders, $orderLine ;
    }
    close INPUTFILE;
    print "Process file containing $lineNumber line(s).\n" if $options{debug} > 0 ;
    print "  -> Found " . @orders . " record(s).\n"        if $options{debug} > 0 ;
    print "\@orders = " . Dumper(\@orders) . "\n"          if $options{debug} > 2 ;
}

# Connect to the database.
my $dbh ;
{
    my $timer = MKPTimer->new("DB Connection", *STDOUT, $options{timing}, 1) ;
    $dbh = DBI->connect("DBI:mysql:database=$options{database};host=$options{hostname}",
                       $options{username},
                       $options{password},
                       {'RaiseError' => 1});
}


#
# Insert each order
{
    my $timer = MKPTimer->new("INSERT", *STDOUT, $options{timing}, 1) ;

    my $sth = $dbh->prepare(${\ORDERS_INSERT_STATEMENT}) ;
    foreach my $order (@orders)
    {
        print "About to load " . $order->{source_order_id} . " from " . $order->{order_datetime} . " on SKU " . $order->{sku} . "\n" if $options{debug} > 0 ;
        if( not $sth->execute( 1                                    , # TODO: Fix by using query
                               $order->{order_datetime}             ,
                               $order->{settlement_id}              ,
                               $order->{type}                       ,
                               $order->{source_order_id}            ,
                               $order->{sku}                        ,
                               $order->{quantity}                   ,
                               $order->{marketplace}                ,
                               $order->{fulfillment}                ,
                               $order->{order_city}                 ,
                               $order->{order_state}                ,
                               $order->{order_postal}               ,
                               $order->{product_sales}              ,
                               $order->{shipping_credits}           ,
                               $order->{gift_wrap_credits}          ,
                               $order->{promotional_rebates}        ,
                               $order->{sales_tax_colected}         ,
                               $order->{marketplace_facilitator_tax},
                               $order->{selling_fees}               ,
                               $order->{fba_fees}                   ,
                               $order->{other_transaction_fees}     ,
                               $order->{other}                      ,
                               $order->{total}                      ) )
        {
            print STDERR "Failed to insert " . $order->{source_order_id} . "from " . $order->{order_datetime} . " on SKU " . $order->{sku} . "\n" ;
        }
    }
    $sth->finish();
}
# Disconnect from the database.
$dbh->disconnect();


#
# Amazon file has a odd date, need to convert to what mysql wants
#    Amazon example: "Dec 1, 2017 12:38:23 AM PST"
#    MYSQL  example: 2017-12-01 12:38:23 AM
sub format_date($)
{
    my $s = shift ;
    my $date = ParseDate($s) ;
    return UnixDate($date, "%Y-%m-%d %H:%M:%S") ;
}

sub usage_and_die
{
    my $rc = shift || 0 ; 
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program emails or prints the EAD performance for Amazon Logistics

usage: $0 [options]
--email           send to this email address
--print           print instead of email
--usage|help|?    print this help
USAGE
    exit($rc) ;
}

