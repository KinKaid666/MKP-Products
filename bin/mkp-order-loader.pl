#!/usr/bin/perl -w

use strict ;
use warnings;

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

use constant EXPENSES_INSERT_STATEMENT => qq( insert into expenses ( source_name, expense_datetime, type, description, total ) value ( ?, ?, ?, ?, ? ) ) ;
use constant EXPENSES_SELECT_STATEMENT => qq( select expense_datetime, type, description from expenses where expense_datetime = ? and type = ? and description = ? ) ;

use constant ORDERS_SELECT_STATEMENT => qq( select order_datetime, source_order_id, type, sku from sku_orders where order_datetime = ? and source_order_id = ? and type = ? and sku = ? ) ;
use constant ORDERS_INSERT_STATEMENT => qq(
    insert into sku_orders ( source_name,
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
    "filename=s"     => \$options{filename},
    "print"          => \$options{print},
    "timing"         => sub { $options{timing}++ },
    "debug"          => sub { $options{debug}++ },
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

die "You must provide a filename." if (not defined $options{filename}) ;

my $sku_orders ;
my $expenses ;

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
    my $skipped_lines_count = 0 ;
    my $order_count = 0 ;
    my $uniq_skuorder_count = 0 ;
    my $total_skuorder_count = 0 ;
    my $expense_count = 0 ;
    my $uniq_expense_count = 0 ;

    open(INPUTFILE, $options{filename}) or die "Can't open $options{filename}: $!" ;
    while(my $line = <INPUTFILE>)
    {
        chomp($line) ;

        ++$lineNumber ;

        #
        # TODO FIGURE OUT ENCODING
        # Skip the default headers; there something
        if( $line =~ m/.*Includes Amazon Marketplace, Fulfillment by Amazon \(FBA\), and Amazon Webstore transactions.*/
            or $line eq qq("Includes Amazon Marketplace, Fulfillment by Amazon (FBA), and Amazon Webstore transactions")
            or $line eq qq("All amounts in USD, unless specified")
            or $line eq qq("Definitions:")
            or $line eq qq("Sales tax collected: Includes sales tax collected from buyers for product sales, shipping, and gift wrap.")
            or $line eq qq("Selling fees: Includes variable closing fees and referral fees.")
            or $line eq qq("Other transaction fees: Includes shipping chargebacks, shipping holdbacks, per-item fees  and sales tax collection fees.")
            or $line eq qq("Other: Includes non-order transaction amounts. For more details, see the ""Type"" and ""Description"" columns for each order ID.")
            or $line eq qq("date/time","settlement id","type","order id","sku","description","quantity","marketplace","fulfillment","order city","order state","order postal","product sales","shipping credits","gift wrap credits","promotional rebates","sales tax collected","Marketplace Facilitator Tax","selling fees","fba fees","other transaction fees","other","total") )
        {
            $skipped_lines_count++ ;
            next ;
        }

        #
        # Amazon has quotes around every field and sometimes some empty, unquote fields
        #    First strip the leading and trailing quote
        $line =~ s/^\"(.*)\"$/$1/ ;
        #    Second if there are any empty fields (,,), make sure they are formatted correct (,"",)
        $line =~ s/,,/,"",/g ;
        #    lastly cut all the fields by ","
        my @subs = split(/","/, $line) ;
        die "invalid line $lineNumber : $line" if scalar @subs != 23 ;

        my $orderLine ;
        $orderLine->{order_datetime}              = &format_date($subs[0]);
        $orderLine->{settlement_id}               = $subs[ 1] ;
        $orderLine->{type}                        = $subs[ 2] ;
        $orderLine->{source_order_id}             = (length $subs[3] ? $subs[3] : "NO_ORDER");
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

        #
        # Amazon paying our bank account
        if( $orderLine->{type} eq "Transfer" )
        {
            $skipped_lines_count++ ;
            next ;
        }

        #
        # Amazon non-SKU and non-order expenses
        if( $orderLine->{type} eq "Service Fee" or $orderLine->{type} eq "FBA Inventory Fee" or $orderLine->{sku}  eq "" )
        {
            print "Found expense on line " . $lineNumber . " type '" . $orderLine->{type} . "' with description '" . $orderLine->{description} .
                  "' from " . $orderLine->{order_datetime} . " for " . $orderLine->{total} . "\n" if $options{debug} > 1 ;
            my $expense ;

            my $key = $orderLine->{order_datetime} . "~" . $orderLine->{type} . "~" . $orderLine->{description} ;
            if( exists $expenses->{$key} )
            {
                $expenses->{$key}->{total}           += $orderLine->{total} ;
            }
            else
            {
                $expense->{expense_datetime} = $orderLine->{order_datetime} ;
                $expense->{type}             = $orderLine->{type} ;
                $expense->{description}      = $orderLine->{description} ;
                $expense->{total}            = $orderLine->{total} ;
                $uniq_expense_count++ ;
                $expenses->{$key} = $expense ;
            }
            $expense_count++ ;
        }
        else
        {
            #
            # have we seen this order before?
            if( exists $sku_orders->{$orderLine->{type}} and exists $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}} )
            {
                #
                # have we seen this sku for this order before?
                #   if so just add it in
                if( exists $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}} )
                {
                    print "Found additional initial skuorder line on " . $lineNumber . " Order " . $orderLine->{source_order_id} . " on SKU " . $orderLine->{sku} . "\n" if $options{debug} > 1 ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{quantity}                    += $orderLine->{quantity}                    ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{product_sales}               += $orderLine->{product_sales}               ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{shipping_credits}            += $orderLine->{shipping_credits}            ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{gift_wrap_credits}           += $orderLine->{gift_wrap_credits}           ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{promotional_rebates}         += $orderLine->{promotional_rebates}         ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{sales_tax_colected}          += $orderLine->{sales_tax_colected}          ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{marketplace_facilitator_tax} += $orderLine->{marketplace_facilitator_tax} ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{selling_fees}                += $orderLine->{selling_fees}                ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{fba_fees}                    += $orderLine->{fba_fees}                    ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{other_transaction_fees}      += $orderLine->{other_transaction_fees}      ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{other}                       += $orderLine->{other}                       ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}}->{total}                       += $orderLine->{total}                       ;
                }
                else
                {
                    print "Found initial skuorder line on " . $lineNumber . " Order " . $orderLine->{source_order_id} . " on SKU " . $orderLine->{sku} . "\n" if $options{debug} > 1 ;
                    $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}} = $orderLine ;
                    $uniq_skuorder_count++ ;
                }
            }
            else
            {
                print "Found initial order line on " . $lineNumber . " Order " . $orderLine->{source_order_id} . " on SKU " . $orderLine->{sku} . "\n" if $options{debug} > 1 ;
                $sku_orders->{$orderLine->{type}}->{$orderLine->{source_order_id}}->{$orderLine->{sku}} = $orderLine ;
                $order_count++ ;
                $uniq_skuorder_count++ ;
            }
            $total_skuorder_count++ ;
        }
    }
    close INPUTFILE;
    print "Process file containing $lineNumber line(s).\n"                                    if $options{debug} > 0 ;
    print "  -> Skipped " . $order_count . " orders related record(s).\n"                     if $options{debug} > 0 ;
    print "  -> Found " . $order_count . " orders related record(s).\n"                       if $options{debug} > 0 ;
    print "  -> Found " . $uniq_skuorder_count . " unique sku orders related record(s).\n"    if $options{debug} > 0 ;
    print "  -> Found " . $total_skuorder_count . " total sku orders related record(s).\n"    if $options{debug} > 0 ;
    print "\@sku_orders = " . Dumper(\$sku_orders) . "\n"                                     if $options{debug} > 2 ;
    print "  -> Found " . $expense_count . " non-SKU related expense record(s).\n"            if $options{debug} > 0 ;
    print "  -> Found " . $uniq_expense_count . " uniqu non-SKU related expense record(s).\n" if $options{debug} > 0 ;
    print "\@expenses = " . Dumper(\$expenses) . "\n"                                         if $options{debug} > 2 ;
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
    my $timer = MKPTimer->new("Insert orders", *STDOUT, $options{timing}, 1) ;

    my $i_sth = $dbh->prepare(${\ORDERS_INSERT_STATEMENT}) ;
    my $s_sth = $dbh->prepare(${\ORDERS_SELECT_STATEMENT}) ;
    foreach my $type (keys %$sku_orders)
    {
        foreach my $order (keys %{$sku_orders->{$type}})
        {
            foreach my $sku (keys %{$sku_orders->{$type}->{$order}} )
            {
                my $skuorder = $sku_orders->{$type}->{$order}->{$sku} ;

                #
                # If we have a cost for this SKU, delete all future dates, terminate the latest and insert the new
                $s_sth->execute($skuorder->{order_datetime}, $order,$type,$sku) or die $DBI::errstr ;
                if( $s_sth->rows > 0 )
                {
                    print STDERR "Skipping duplicate sku_order found: order " . $order . " SKU " . $sku . " type " . $type . "\n" ;
                }
                else
                {
                    print "About to load order " . $skuorder->{source_order_id} . " from " . $skuorder->{order_datetime} . " on SKU " . $skuorder->{sku} . "\n" if $options{debug} > 0 ;
                    if( not $i_sth->execute( "www.amazon.com"                     ,
                                             $skuorder->{order_datetime}             ,
                                             $skuorder->{settlement_id}              ,
                                             $skuorder->{type}                       ,
                                             $skuorder->{source_order_id}            ,
                                             $skuorder->{sku}                        ,
                                             $skuorder->{quantity}                   ,
                                             $skuorder->{marketplace}                ,
                                             $skuorder->{fulfillment}                ,
                                             $skuorder->{order_city}                 ,
                                             $skuorder->{order_state}                ,
                                             $skuorder->{order_postal}               ,
                                             $skuorder->{product_sales}              ,
                                             $skuorder->{shipping_credits}           ,
                                             $skuorder->{gift_wrap_credits}          ,
                                             $skuorder->{promotional_rebates}        ,
                                             $skuorder->{sales_tax_colected}         ,
                                             $skuorder->{marketplace_facilitator_tax},
                                             $skuorder->{selling_fees}               ,
                                             $skuorder->{fba_fees}                   ,
                                             $skuorder->{other_transaction_fees}     ,
                                             $skuorder->{other}                      ,
                                             $skuorder->{total}                      ) )
                    {
                        print STDERR "Failed to insert " . $skuorder->{source_order_id} . "from " . $skuorder->{order_datetime} . " on SKU " . $skuorder->{sku} . "\n" ;
                    }
                }
            }
        }
    }
    $i_sth->finish();
    $s_sth->finish();
}

#
# Insert each expense
{
    my $timer = MKPTimer->new("Insert expenses", *STDOUT, $options{timing}, 1) ;

    my $i_sth = $dbh->prepare(${\EXPENSES_INSERT_STATEMENT}) ;
    my $s_sth = $dbh->prepare(${\EXPENSES_SELECT_STATEMENT}) ;
    foreach my $key (keys %$expenses) 
    {
        my $expense = $expenses->{$key} ;
        #
        # If we have a cost for this SKU, delete all future dates, terminate the latest and insert the new
        $s_sth->execute($expense->{expense_datetime},$expense->{type},$expense->{description}) or die $DBI::errstr ;
        if( $s_sth->rows > 0 )
        {
            print STDERR "Skipping duplicate expense entry found: expenses_datetime " . $expense->{expense_datetime} . " type " . $expense->{type} . " description " . $expense->{description} . "\n" ;
        }
        else
        {

            #
            # TODO Remove hardcoded Amazon.com
            print "About to load expense type '" . $expense->{type} . "' with description '" . $expense->{description} .
                  "' from " . $expense->{expense_datetime} . " for " . $expense->{total} . "\n" if $options{debug} > 0 ;
            if( not $i_sth->execute( "www.amazon.com"            ,
                                     $expense->{expense_datetime},
                                     $expense->{type}            ,
                                     $expense->{description}     ,
                                     $expense->{total}           ) )
            {
                print STDERR "Failed to insert '" . $expense->{type} . "' from " . $expense->{expense_datetime} . "\n" ;
            }
        }
    }
    $i_sth->finish() ;
    $s_sth->finish() ;
}

# Disconnect from the database.
$dbh->disconnect() ;


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
This program adds order entries to the database

usage: $0 [options]
--database      the database to use
--filename      the filename containing the orders
--usage|help|?  print this help
USAGE
    exit($rc) ;
}

