#!/usr/bin/perl -w

#
# TODO: CouponPaymentEventList
use strict;

use Amazon::MWS::Client ;
use DateTime ;
use Date::Manip ;
use Data::Dumper ;
use Getopt::Long ;
use DBI ;

use Encode ;
binmode(STDOUT, ":utf8");

# MKP Specific Libraries
use File::Basename qw(dirname basename) ;
use Cwd qw(abs_path) ;
use lib &dirname(&abs_path($0)) . "/lib" ;

use MKPFormatter ;
use MKPTimer ;

# SQL Statements
# mysql> desc financial_event_groups ;
# +------------------------------+------------------+------+-----+-------------------+-----------------------------+
# | Field                        | Type             | Null | Key | Default           | Extra                       |
# +------------------------------+------------------+------+-----+-------------------+-----------------------------+
# | id                           | int(10) unsigned | NO   | PRI | NULL              | auto_increment              |
# | source_name                  | varchar(50)      | NO   |     | NULL              |                             |
# | ext_financial_event_group_id | varchar(50)      | NO   |     | NULL              |                             |
# | fund_transfer_dt             | timestamp        | YES  |     | NULL              |                             |
# | transfer_status              | varchar(50)      | YES  |     | NULL              |                             |
# | processing_status            | varchar(50)      | NO   |     | NULL              |                             |
# | event_start_dt               | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# | event_end_dt                 | timestamp        | YES  |     | NULL              |                             |
# | trace_id                     | varchar(50)      | YES  |     | NULL              |                             |
# | account_tail                 | varchar(50)      | YES  |     | NULL              |                             |
# | beginning_balance            | decimal(13,2)    | NO   |     | NULL              |                             |
# | total                        | decimal(13,2)    | NO   |     | NULL              |                             |
# | currenty_code                | varchar(3)       | NO   |     | NULL              |                             |
# | latest_user                  | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update                | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user                | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date                | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +------------------------------+------------------+------+-----+-------------------+-----------------------------+
use constant SELECT_FEG_STATEMENT => qq(
    select id
           , processing_status
           , total
      from financial_event_groups
     where ext_financial_event_group_id = ?
) ;
use constant INSERT_FEG_STATEMENT => qq(
    insert into financial_event_groups (
          source_name                  ,
          ext_financial_event_group_id ,
          fund_transfer_dt             ,
          transfer_status              ,
          processing_status            ,
          event_start_dt               ,
          event_end_dt                 ,
          trace_id                     ,
          account_tail                 ,
          beginning_balance            ,
          total                        ,
          currency_code
    ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
) ;
use constant UPDATE_FEG_STATEMENT => qq(
    update financial_event_groups set
          source_name                  = ?,
          ext_financial_event_group_id = ?,
          fund_transfer_dt             = ?,
          transfer_status              = ?,
          processing_status            = ?,
          event_start_dt               = ?,
          event_end_dt                 = ?,
          trace_id                     = ?,
          account_tail                 = ?,
          beginning_balance            = ?,
          total                        = ?,
          currency_code                = ?
    where id = ?
) ;

# mysql> desc financial_shipment_events ;
# +-----------------------------+------------------+------+-----+-------------------+-----------------------------+
# | Field                       | Type             | Null | Key | Default           | Extra                       |
# +-----------------------------+------------------+------+-----+-------------------+-----------------------------+
# | id                          | int(10) unsigned | NO   | PRI | NULL              | auto_increment              |
# | feg_id                      | int(10) unsigned | NO   | MUL | NULL              |                             |
# | event_type                  | varchar(50)      | NO   |     | NULL              |                             |
# | posted_dt                   | timestamp        | YES  | MUL | NULL              |                             |
# | source_order_id             | varchar(50)      | NO   | MUL | NULL              |                             |
# | marketplace                 | varchar(50)      | NO   |     | NULL              |                             |
# | sku                         | varchar(20)      | NO   | MUL | NULL              |                             |
# | quantity                    | int(10) unsigned | NO   |     | NULL              |                             |
# | product_charges             | decimal(13,2)    | NO   |     | NULL              |                             |
# | product_charges_tax         | decimal(13,2)    | NO   |     | NULL              |                             |
# | shipping_charges            | decimal(13,2)    | NO   |     | NULL              |                             |
# | shipping_charges_tax        | decimal(13,2)    | NO   |     | NULL              |                             |
# | giftwrap_charges            | decimal(13,2)    | NO   |     | NULL              |                             |
# | giftwrap_charges_tax        | decimal(13,2)    | NO   |     | NULL              |                             |
# | marketplace_facilitator_tax | decimal(13,2)    | NO   |     | NULL              |                             |
# | promotional_rebates         | decimal(13,2)    | NO   |     | NULL              |                             |
# | selling_fees                | decimal(13,2)    | NO   |     | NULL              |                             |
# | fba_fees                    | decimal(13,2)    | NO   |     | NULL              |                             |
# | other_fees                  | decimal(13,2)    | NO   |     | NULL              |                             |
# | total                       | decimal(13,2)    | NO   |     | NULL              |                             |
# | currency_code               | varchar(3)       | NO   |     | NULL              |                             |
# | latest_user                 | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update               | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user               | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date               | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +-----------------------------+------------------+------+-----+-------------------+-----------------------------+
use constant SELECT_FSE_STATEMENT => qq(
    select *
      from financial_shipment_events
     where feg_id = ?
) ;
use constant INSERT_FSE_STATEMENT => qq(
    insert into financial_shipment_events (
        feg_id                     ,
        event_type                 ,
        posted_dt                  ,
        source_order_id            ,
        marketplace                ,
        sku                        ,
        quantity                   ,
        product_charges            ,
        product_charges_tax        ,
        shipping_charges           ,
        shipping_charges_tax       ,
        giftwrap_charges           ,
        giftwrap_charges_tax       ,
        marketplace_facilitator_tax,
        promotional_rebates        ,
        selling_fees               ,
        fba_fees                   ,
        other_fees                 ,
        total                      ,
        currency_code
    ) value ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
) ;
use constant DELETE_FSE_STATEMENT => qq(
    delete from financial_shipment_events where feg_id = ?
) ;

# mysql> desc financial_expense_events ;
# +------------------+------------------+------+-----+-------------------+-----------------------------+
# | Field            | Type             | Null | Key | Default           | Extra                       |
# +---------------+------------------+------+-----+-------------------+-----------------------------+
# | id            | int(10) unsigned | NO   | PRI | NULL              | auto_increment              |
# | feg_id        | int(10) unsigned | NO   |     | NULL              |                             |
# | expense_dt    | timestamp        | NO   | MUL | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | type          | varchar(50)      | YES  | MUL | NULL              |                             |
# | description   | varchar(150)     | YES  |     | NULL              |                             |
# | total         | decimal(13,2)    | NO   |     | NULL              |                             |
# | currency_code | varchar(3)       | NO   |     | NULL              |                             |
# | latest_user   | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +---------------+------------------+------+-----+-------------------+-----------------------------+
use constant SELECT_FEE_STATEMENT => qq(
    select *
      from financial_expense_events
     where feg_id = ?
) ;
use constant INSERT_FEE_STATEMENT => qq(
    insert into financial_expense_events (
        feg_id       ,
        expense_dt   ,
        type         ,
        description  ,
        total        ,
        currency_code
    ) value ( ?, ?, ?, ?, ?, ? )
) ;
use constant DELETE_FEE_STATEMENT => qq(
    delete from financial_expense_events where feg_id = ?
) ;

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
    "start=s"        => \$options{start},
    "end=s"          => \$options{end},
    "duration=s"     => \$options{duration},
    "dumper"         => \$options{dumper},
    "timing|t+"      => \$options{timing},
    "verbose|d+"     => \$options{verbose},
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

if( $options{verbose} or $options{timing} )
{
    print "Debug  = $options{verbose}\n" ;
    print "Timing = $options{timing}\n" ;
}

# pure verbose; remove before prod
if( not ((defined $options{start} and defined $options{end}     )  or
         (defined $options{start} and defined $options{duration})  or
         (defined $options{end}   and defined $options{duration})) or
    (defined $options{start} and defined $options{end} and $options{duration}) )
{
    #
    # default to today
    $options{start} = UnixDate(DateTime->now()->set_time_zone($timezone), "%Y-%m-%d") if not defined $options{start} ;
}

if( defined $options{duration} and
    not ($options{duration} eq "MONTH" or
         $options{duration} eq "WEEK"  or
         $options{duration} eq "DAY" )  )
{
    print STDERR "--duration must be MONTH, WEEK, or DAY\n" ;
    &usage_and_die(1) ;
}

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
    $start = UnixDate(DateTime->now()->set_time_zone($timezone),"%Y-%m-%d") ;
}

if(defined $options{end})
{
    $end = $options{end} ;
}
if(defined $options{duration})
{
    $end = UnixDate(DateCalc(ParseDate($options{start}), "+ 1 $options{duration}"),"%Y-%m-%d") ;
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
    $mws = Amazon::MWS::Client->new(%$credentials) ;
}

my $groupReq ;

if( defined $end )
{
    $groupReq = $mws->ListFinancialEventGroups(FinancialEventGroupStartedAfter  => $start,
                                               FinancialEventGroupStartedBefore => $end) ;
}
else
{
    $groupReq = $mws->ListFinancialEventGroups(FinancialEventGroupStartedAfter  => $start) ;
}

print "groupReq = " . Dumper($groupReq) . "\n" if $options{dumper} ;
#
# Pull all the data from Amazon and merge into one data structure
my $fegs ;
my $gTokens = 0 ;
while(1)
{
    my $timer = MKPTimer->new("MWS Pull", *STDOUT, $options{timing}, 1) ;
    foreach my $fGroup (@{&force_array($groupReq->{FinancialEventGroupList}->{FinancialEventGroup})})
    {
        $fegs->{$fGroup->{FinancialEventGroupId}} = $fGroup ;

        my $feTokens = 0 ;
        my $req = $mws->ListFinancialEvents(FinancialEventGroupId => $fGroup->{FinancialEventGroupId}) ;
        while(1)
        {
            print "[$gTokens] FinancialEventGroupId = $fGroup->{FinancialEventGroupId} [$feTokens]\n" if $options{verbose} ;
            print Dumper($req) . "\n" if $options{dumper} ;

            #
            # if this has financial events
            if( exists $fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents} )
            {
                #
                # Find all the lists underneath it
                foreach my $list (keys %{$req->{FinancialEvents}})
                {
                    my $name = $list ;
                    $name =~ s/^(.*)List$/$1/g ;

                    #
                    # Only tag that doesn't follow the schema
                    $name = "ShipmentEvent" if($list eq "RefundEventList" or $list eq "ChargebackEventList") ;

                    #
                    # Merge arrays
                    if( exists $req->{FinancialEvents}->{$list} and exists $req->{FinancialEvents}->{$list}->{$name} )
                    {
                        if( exists $fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents}->{$list} and exists $fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents}->{$list}->{$name} )
                        {
                            #
                            # Return value isn't an array if there is only one value; so ensure it's always an array
                            $fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents}->{$list}->{$name} = &force_array($fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents}->{$list}->{$name}) ;

                            foreach my $element (@{&force_array($req->{FinancialEvents}->{$list}->{$name})})
                            {
                                push @{$fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents}->{$list}->{$name}}, $element ;
                            }
                        }
                        else
                        {
                            #
                            # Return value isn't an array if there is only one value; so ensure it's always an array
                            $fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents}->{$list}->{$name} = &force_array($req->{FinancialEvents}->{$list}->{$name}) ;
                        }
                    }
                }
            }
            else
            {
                $fegs->{$fGroup->{FinancialEventGroupId}}->{FinancialEvents} = $req->{FinancialEvents} ;
            }

            last if( not defined $req->{NextToken} ) ;
            $req = $mws->ListFinancialEventsByNextToken(NextToken => $req->{NextToken}) ;
            $feTokens++ ;
        }
    }

    $gTokens++ ;
    last if( not defined $groupReq->{NextToken} ) ;
    $groupReq = $mws->ListFinancialEventGroupsByNextToken(NextToken => $groupReq->{NextToken}) ;
}

print "MWS Response = " . Dumper($fegs) . "\n" if $options{dumper} ;

#
# Amazon lists orders in a random way, this routine cleans up the data and then 
# compresses (puts all order->sku) together
my $financialEventGroups ;
my $financialShipmentEvents ;
my $financialExpenseEvents ;
{
    foreach my $feg_id (keys %$fegs)
    {
        my $feg = $fegs->{$feg_id} ;
        my $cc ;

        $feg->{FundTransferDate}         = &convert_amazon_datetime($feg->{FundTransferDate})          if defined $feg->{FundTransferDate}         ;
        $feg->{FinancialEventGroupStart} = &convert_amazon_datetime($feg->{FinancialEventGroupStart})  if defined $feg->{FinancialEventGroupStart} ;
        $feg->{FinancialEventGroupEnd}   = &convert_amazon_datetime($feg->{FinancialEventGroupEnd})    if defined $feg->{FinancialEventGroupEnd}   ;
        $cc = &die_or_set_currency($cc,$feg->{OriginalTotal}->{CurrencyCode}) ;

        print "Processing feg $feg_id\n" if $options{verbose} > 0 ;
        print "\tFinancialEventGroupId    = " . &nvl($feg->{FinancialEventGroupId})                                                                     . "\n" if $options{verbose} > 1 ;
        print "\tFundTransferStatus       = " . &nvl($feg->{FundTransferStatus})                                                                        . "\n" if $options{verbose} > 1 ;
        print "\tFinancialEventGroupStart = " . &nvl($feg->{FinancialEventGroupStart})                                                                  . "\n" if $options{verbose} > 1 ;
        print "\tFinancialEventGroupEnd   = " . &nvl($feg->{FinancialEventGroupEnd})                                                                    . "\n" if $options{verbose} > 1 ;
        print "\tOriginalTotal            = " . &nvl($feg->{OriginalTotal}->{CurrencyAmount}) . " " . &nvl($feg->{OriginalTotal}->{CurrencyCode})       . "\n" if $options{verbose} > 1 ;
        print "\tFundTransferDate         = " . &nvl($feg->{FundTransferDate})                                                                          . "\n" if $options{verbose} > 1 ;
        print "\tAccountTail              = " . &nvl($feg->{AccountTail})                                                                               . "\n" if $options{verbose} > 1 ;
        print "\tTraceId                  = " . &nvl($feg->{TraceId})                                                                                   . "\n" if $options{verbose} > 1 ;
        print "\tProcessingStatus         = " . &nvl($feg->{ProcessingStatus})                                                                          . "\n" if $options{verbose} > 1 ;
        print "\tBeginningBalance         = " . &nvl($feg->{BeginningBalance}->{CurrencyAmount}) . " " . &nvl($feg->{BeginningBalance}->{CurrencyCode}) . "\n" if $options{verbose} > 1 ;

        $feg->{FundTransferDate}         = &convert_amazon_datetime($feg->{FundTransferDate})          if defined $feg->{FundTransferDate}         ;
        $feg->{FinancialEventGroupStart} = &convert_amazon_datetime($feg->{FinancialEventGroupStart})  if defined $feg->{FinancialEventGroupStart} ;
        $feg->{FinancialEventGroupEnd}   = &convert_amazon_datetime($feg->{FinancialEventGroupEnd})    if defined $feg->{FinancialEventGroupEnd}   ;
        $cc = &die_or_set_currency($cc,$feg->{OriginalTotal}->{CurrencyCode}) ;
        $feg->{CurrencyCode} = $cc ;

        #
        # Add it to the compressed data structure
        $financialEventGroups->{$feg_id} = $feg ;

        #
        # Process Shipment List
        if( exists $feg->{FinancialEvents}->{ShipmentEventList}->{ShipmentEvent} )
        {
            foreach my $shipment (@{&force_array($feg->{FinancialEvents}->{ShipmentEventList}->{ShipmentEvent})})
            {
                my $timer = MKPTimer->new("\tShipment $shipment->{AmazonOrderId}", *STDOUT, $options{timing}, 1) ;
                $shipment->{PostedDate} = &convert_amazon_datetime($shipment->{PostedDate}) if defined $shipment->{PostedDate} ;

                print "\tProcessing Shipment $feg_id : $shipment->{AmazonOrderId}\n" if $options{verbose} > 1 ;
                print "\t\tAmazonOrderId   = " . &nvl($shipment->{AmazonOrderId})   . "\n" if $options{verbose} > 2 ;
                print "\t\tSellerOrderId   = " . &nvl($shipment->{SellerOrderId})   . "\n" if $options{verbose} > 2 ;
                print "\t\tPostedDate      = " . &nvl($shipment->{PostedDate})      . "\n" if $options{verbose} > 2 ;
                print "\t\tMarketplaceName = " . &nvl($shipment->{MarketplaceName}) . "\n" if $options{verbose} > 2 ;

                if( exists $shipment->{ShipmentItemList} and exists $shipment->{ShipmentItemList}->{ShipmentItem} )
                {
                    foreach my $item (@{&force_array($shipment->{ShipmentItemList}->{ShipmentItem})})
                    {
                        my $products_charges            = 0 ;
                        my $products_charges_tax        = 0 ;
                        my $shipping_charges            = 0 ;
                        my $shipping_charges_tax        = 0 ;
                        my $giftwrap_charges            = 0 ;
                        my $giftwrap_charges_tax        = 0 ;
                        my $marketplace_facilitator_tax = 0 ;
                        my $promotional_rebates         = 0 ;
                        my $selling_fees                = 0 ;
                        my $fba_fees                    = 0 ;
                        my $other_fees                  = 0 ;
                        my $total                       = 0 ;
                        print "\t\t\tOrderItemId     = " . &nvl($item->{OrderItemId})     . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tQuantityShipped = " . &nvl($item->{QuantityShipped}) . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tSellerSKU       = " . &nvl($item->{SellerSKU})       . "\n" if $options{verbose} > 2 ;

                        #
                        # Check for item fees (to us)
                        if( exists $item->{ItemFeeList} and exists $item->{ItemFeeList}->{FeeComponent} )
                        {
                            print "\t\t\t\tFound Fees\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{ItemFeeList}->{FeeComponent})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{FeeType}) . " = " . &nvl($fee->{FeeAmount}->{CurrencyAmount}) . " " . &nvl($fee->{FeeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;

                                $cc = &die_or_set_currency($cc,$fee->{FeeAmount}->{CurrencyCode}) ;
                                if    ( $fee->{FeeType} =~ m/^FBA.*$/      ) { $fba_fees     += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{FeeType} =~ m/^Commission$/ ) { $selling_fees += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                else                                         { $other_fees   += $fee->{FeeAmount}->{CurrencyAmount} ; }
                            }
                        }

                        #
                        # Check for item charges (to customer)
                        if( exists $item->{ItemChargeList} and exists $item->{ItemChargeList}->{ChargeComponent} )
                        {
                            print "\t\t\t\tFound Charges\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{ItemChargeList}->{ChargeComponent})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{ChargeType}) .  " = " . &nvl($fee->{ChargeAmount}->{CurrencyAmount}) . " " . &nvl($fee->{ChargeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                                $cc = &die_or_set_currency($cc,$fee->{ChargeAmount}->{CurrencyCode}) ;
                                if    ( $fee->{ChargeType} =~ m/^Principal$/      ) { $products_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^Tax$/            ) { $products_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ShippingCharge$/ ) { $shipping_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ShippingTax$/    ) { $shipping_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^GiftWrap$/       ) { $giftwrap_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^GiftWrapTax$/    ) { $giftwrap_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                else                                                { print STDERR "unknown fee . $fee->{ChargeType}\n" if $options{verbose} ;  $products_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ;}
                            }
                        }

                        #
                        # Check for promotions
                        if( exists $item->{PromotionList} and exists $item->{PromotionList}->{Promotion} )
                        {
                            print "\t\t\t\tFound Promotions\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{PromotionList}->{Promotion})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{PromotionId}) . " | " . &nvl($fee->{PromotionType}) . " = " . &nvl($fee->{PromotionAmount}->{CurrencyAmount}) . " " . &nvl($fee->{PromotionAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                                $cc = &die_or_set_currency($cc,$fee->{PromotionAmount}->{CurrencyCode}) ;
                                $promotional_rebates += $fee->{PromotionAmount}->{CurrencyAmount} ;
                            }
                        }

                        #
                        # Check for taxes (given back to us)
                        if( exists $item->{ItemTaxWithheldList} and
                            exists $item->{ItemTaxWithheldList}->{TaxWithheldComponent} and
                            exists $item->{ItemTaxWithheldList}->{TaxWithheldComponent}->{TaxesWithheld} and
                            exists $item->{ItemTaxWithheldList}->{TaxWithheldComponent}->{TaxesWithheld}->{ChargeComponent} )
                        {
                            print "\t\t\t\tFound Taxes\n" if $options{verbose} > 2 ;
                            print "\t\t\t\t\tTaxe Model = " . &nvl($item->{ItemTaxWithheldList}->{TaxWithheldComponent}->{TaxCollectionModel}) . "\n" if $options{verbose} > 2 ;
                            foreach my $tax (@{&force_array($item->{ItemTaxWithheldList}->{TaxWithheldComponent}->{TaxesWithheld}->{ChargeComponent})})
                            {
                                print "\t\t\t\t\t" . &nvl($tax->{ChargeType}) . " = " . &nvl($tax->{ChargeAmount}->{CurrencyAmount}) . " " . &nvl($tax->{ChargeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;

                                $cc = &die_or_set_currency($cc,$tax->{ChargeAmount}->{CurrencyCode}) ;
                                $marketplace_facilitator_tax += $tax->{ChargeAmount}->{CurrencyAmount} ;
                            }
                        }

                        $total = $products_charges + $products_charges_tax + $shipping_charges + $shipping_charges_tax + $giftwrap_charges + $giftwrap_charges_tax + $marketplace_facilitator_tax + $promotional_rebates + $selling_fees + $fba_fees + $other_fees ;

                        print "\t\t\t\t\$products_charges            = $products_charges\n"             if $options{verbose} > 2 ;
                        print "\t\t\t\t\$products_charges_tax        = $products_charges_tax\n"         if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges            = $shipping_charges\n"             if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges_tax        = $shipping_charges_tax\n"         if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges            = $giftwrap_charges\n"             if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges_tax        = $giftwrap_charges_tax\n"         if $options{verbose} > 2 ;
                        print "\t\t\t\t\$marketplace_facilitator_tax = $marketplace_facilitator_tax\n"  if $options{verbose} > 2 ;
                        print "\t\t\t\t\$promotional_rebates         = $promotional_rebates\n"          if $options{verbose} > 2 ;
                        print "\t\t\t\t\$selling_fees                = $selling_fees\n"                 if $options{verbose} > 2 ;
                        print "\t\t\t\t\$fba_fees                    = $fba_fees\n"                     if $options{verbose} > 2 ;
                        print "\t\t\t\t\$other_fees                  = $other_fees\n"                   if $options{verbose} > 2 ;
                        print "\t\t\t\t\$cc                          = $cc\n"                           if $options{verbose} > 2 ;
                        print "\t\t\tTotal = $total\n" if $options{verbose} > 1 ;

                        #
                        # Add each event to the compressed data structure
                        if( exists $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}} )
                        {
                            # Add
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{QuantityShipped}           += $item->{QuantityShipped}     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ProductCharges}            += $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ProductChargesTax}         += $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ShippingCharges}           += $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ShippingChargesTax}        += $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{GiftwrapCharges}           += $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        += $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} += $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{PromotionalRebates}        += $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{SellingFees}               += $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{FBAFees}                   += $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{OtherFees}                 += $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{Total}                     += $total                       ;
                        }
                        else
                        {
                            # Insert
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{Type}                      = "Order"                      ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{PostDate}                  = $shipment->{PostedDate}      ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{AmazonOrderId}             = $shipment->{AmazonOrderId}   ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{MarketplaceName}           = $shipment->{MarketplaceName} ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{SellerSKU}                 = $item->{SellerSKU}           ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{QuantityShipped}           = $item->{QuantityShipped}     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ProductCharges}            = $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ProductChargesTax}         = $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ShippingCharges}           = $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{ShippingChargesTax}        = $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{GiftwrapCharges}           = $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        = $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} = $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{PromotionalRebates}        = $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{SellingFees}               = $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{FBAFees}                   = $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{OtherFees}                 = $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{Total}                     = $total                       ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Order}->{$item->{SellerSKU}}->{CurrencyCode}              = $cc                          ;
                        }
                    }
                }
            }
        }

        #
        # Process Refund List
        if( exists $feg->{FinancialEvents}->{RefundEventList}->{ShipmentEvent} )
        {
            foreach my $shipment (@{&force_array($feg->{FinancialEvents}->{RefundEventList}->{ShipmentEvent})})
            {
                $shipment->{PostedDate} = &convert_amazon_datetime($shipment->{PostedDate}) if defined $shipment->{PostedDate} ;

                print "\tProcessing Refund $feg_id : $shipment->{AmazonOrderId}\n" if $options{verbose} > 1 ;
                print "\t\tAmazonOrderId   = " . &nvl($shipment->{AmazonOrderId})   . "\n" if $options{verbose} > 2 ;
                print "\t\tSellerOrderId   = " . &nvl($shipment->{SellerOrderId})   . "\n" if $options{verbose} > 2 ;
                print "\t\tPostedDate      = " . &nvl($shipment->{PostedDate})      . "\n" if $options{verbose} > 2 ;
                print "\t\tMarketplaceName = " . &nvl($shipment->{MarketplaceName}) . "\n" if $options{verbose} > 2 ;

                if( exists $shipment->{ShipmentItemAdjustmentList} and exists $shipment->{ShipmentItemAdjustmentList}->{ShipmentItem} )
                {

                    foreach my $item (@{&force_array($shipment->{ShipmentItemAdjustmentList}->{ShipmentItem})})
                    {
                        my $products_charges            = 0 ;
                        my $products_charges_tax        = 0 ;
                        my $shipping_charges            = 0 ;
                        my $shipping_charges_tax        = 0 ;
                        my $giftwrap_charges            = 0 ;
                        my $giftwrap_charges_tax        = 0 ;
                        my $marketplace_facilitator_tax = 0 ;
                        my $promotional_rebates         = 0 ;
                        my $selling_fees                = 0 ;
                        my $fba_fees                    = 0 ;
                        my $other_fees                  = 0 ;
                        my $total                       = 0 ;
                        print "\t\t\tOrderItemId     = " . &nvl($item->{OrderAdjustmentItemId}) . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tQuantityShipped = " . &nvl($item->{QuantityShipped})       . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tSellerSKU       = " . &nvl($item->{SellerSKU})             . "\n" if $options{verbose} > 2 ;
                        if( exists $item->{ItemFeeAdjustmentList} and exists $item->{ItemFeeAdjustmentList}->{FeeComponent} )
                        {
                            print "\t\t\t\tFound Fees\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{ItemFeeAdjustmentList}->{FeeComponent})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{FeeType}) . " = " . &nvl($fee->{FeeAmount}->{CurrencyAmount}) . " " . &nvl($fee->{FeeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;

                                $cc = &die_or_set_currency($cc,$fee->{FeeAmount}->{CurrencyCode}) ;
                                if    ( $fee->{FeeType} =~ m/^FBA.*$/              ) { $fba_fees     += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{FeeType} =~ m/^ShippingChargeback$/ ) { $fba_fees     += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{FeeType} =~ m/^Commission$/         ) { $selling_fees += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{FeeType} =~ m/^RefundCommission$/   ) { $selling_fees += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                else                                                 { $other_fees   += $fee->{FeeAmount}->{CurrencyAmount} ; }
                            }
                        }

                        if( exists $item->{ItemChargeAdjustmentList} and exists $item->{ItemChargeAdjustmentList}->{ChargeComponent} )
                        {
                            print "\t\t\t\tFound Charges\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{ItemChargeAdjustmentList}->{ChargeComponent})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{ChargeType}) .  " = " . &nvl($fee->{ChargeAmount}->{CurrencyAmount}) . " " . &nvl($fee->{ChargeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                                $cc = &die_or_set_currency($cc,$fee->{ChargeAmount}->{CurrencyCode}) ;
                                if    ( $fee->{ChargeType} =~ m/^Principal$/      ) { $products_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^Tax$/            ) { $products_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ReturnShipping$/ ) { $shipping_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ShippingCharge$/ ) { $shipping_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ShippingTax$/    ) { $shipping_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^GiftWrap$/       ) { $giftwrap_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^GiftWrapTax$/    ) { $giftwrap_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                else                                                { print STDERR "unknown fee . $fee->{ChargeType}\n" if $options{verbose} ;  $products_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ;}
                            }
                        }

                        if( exists $item->{PromotionAdjustmentList} and exists $item->{PromotionAdjustmentList}->{Promotion} )
                        {
                            print "\t\t\t\tFound Promotions\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{PromotionAdjustmentList}->{Promotion})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{PromotionId}) . " | " . &nvl($fee->{PromotionType}) . " = " . &nvl($fee->{PromotionAmount}->{CurrencyAmount}) . " " . &nvl($fee->{PromotionAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                                $cc = &die_or_set_currency($cc,$fee->{PromotionAmount}->{CurrencyCode}) ;
                                $promotional_rebates += $fee->{PromotionAmount}->{CurrencyAmount} ;
                            }
                        }
                        $total = $products_charges + $products_charges_tax + $shipping_charges + $shipping_charges_tax + $giftwrap_charges + $giftwrap_charges_tax + $marketplace_facilitator_tax + $promotional_rebates + $selling_fees + $fba_fees + $other_fees;

                        print "\t\t\t\t\$products_charges            = $products_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$products_charges_tax        = $products_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges            = $shipping_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges_tax        = $shipping_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges            = $giftwrap_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges_tax        = $giftwrap_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$marketplace_facilitator_tax = $marketplace_facilitator_tax\n" if $options{verbose} > 2 ;
                        print "\t\t\t\t\$promotional_rebates         = $promotional_rebates\n"         if $options{verbose} > 2 ;
                        print "\t\t\t\t\$selling_fees                = $selling_fees\n"                if $options{verbose} > 2 ;
                        print "\t\t\t\t\$fba_fees                    = $fba_fees\n"                    if $options{verbose} > 2 ;
                        print "\t\t\t\t\$other_fees                  = $other_fees\n"                  if $options{verbose} > 2 ;
                        print "\t\t\t\t\$cc                          = $cc\n"                          if $options{verbose} > 2 ;
                        print "\t\t\tTotal = $total\n" if $options{verbose} > 1 ;

                        #
                        # Add each event to the compressed data structure
                        if( exists $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}} )
                        {
                            # Add
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{QuantityShipped}           += $item->{QuantityShipped}     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductCharges}            += $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductChargesTax}         += $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingCharges}           += $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingChargesTax}        += $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapCharges}           += $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        += $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} += $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{PromotionalRebates}        += $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{SellingFees}               += $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{FBAFees}                   += $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{OtherFees}                 += $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{Total}                     += $total                       ;
                        }
                        else
                        {
                            # Insert
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{Type}                      = "Refund"                     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{PostDate}                  = $shipment->{PostedDate}      ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{AmazonOrderId}             = $shipment->{AmazonOrderId}   ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{MarketplaceName}           = $shipment->{MarketplaceName} ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{SellerSKU}                 = $item->{SellerSKU}           ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{QuantityShipped}           = $item->{QuantityShipped}     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductCharges}            = $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductChargesTax}         = $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingCharges}           = $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingChargesTax}        = $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapCharges}           = $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        = $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} = $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{PromotionalRebates}        = $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{SellingFees}               = $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{FBAFees}                   = $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{OtherFees}                 = $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{Total}                     = $total                       ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{CurrencyCode}              = $cc                          ;
                        }
                    }
                }
            }
        }

        #
        # Process Adjustment List
        if( exists $feg->{FinancialEvents}->{AdjustmentEventList}->{AdjustmentEvent} )
        {
            foreach my $adj (@{&force_array($feg->{FinancialEvents}->{AdjustmentEventList}->{AdjustmentEvent})})
            {
                $adj->{PostedDate} = &convert_amazon_datetime($adj->{PostedDate}) if defined $adj->{PostedDate} ;

                print "\tProcessing Adjustment $feg_id : $adj->{PostedDate}\n" if $options{verbose} > 1 ;
                print "\t\tAdjustmentType   = " . &nvl($adj->{AdjustmentType})  . "\n" if $options{verbose} > 2 ;
                print "\t\tPostedDate       = " . &nvl($adj->{PostedDate})      . "\n" if $options{verbose} > 2 ;
                print "\t\tAdjustmentAmount = " . &nvl($adj->{AdjustmentAmount}->{CurrencyAmount}) . " " . &nvl($adj->{AdjustmentAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                $cc = &die_or_set_currency($cc,$adj->{AdjustmentAmount}->{CurrencyCode}) ;

                if( exists $adj->{AdjustmentItemList} and exists $adj->{AdjustmentItemList}->{AdjustmentItem} )
                {

                    foreach my $item (@{&force_array($adj->{AdjustmentItemList}->{AdjustmentItem})})
                    {
                        my $products_charges            = 0 ;
                        my $products_charges_tax        = 0 ;
                        my $shipping_charges            = 0 ;
                        my $shipping_charges_tax        = 0 ;
                        my $giftwrap_charges            = 0 ;
                        my $giftwrap_charges_tax        = 0 ;
                        my $marketplace_facilitator_tax = 0 ;
                        my $promotional_rebates         = 0 ;
                        my $selling_fees                = 0 ;
                        my $fba_fees                    = 0 ;
                        my $other_fees                  = 0 ;
                        my $total                       = 0 ;
                        print "\t\t\tProductDescription = " . &nvl($item->{ProductDescription}) . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tQuantity           = " . &nvl($item->{Quantity})           . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tSellerSKU          = " . &nvl($item->{SellerSKU})          . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tTotalAmount        = " . &nvl($item->{TotalAmount}->{CurrencyAmount})   . " " . &nvl($item->{TotalAmount}->{CurrencyCode})   . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tPerUnitAmount      = " . &nvl($item->{PerUnitAmount}->{CurrencyAmount}) . " " . &nvl($item->{PerUnitAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                        $cc = &die_or_set_currency($cc,$item->{TotalAmount}->{CurrencyCode}) ;
                        $cc = &die_or_set_currency($cc,$item->{PerUnitAmount}->{CurrencyCode}) ;

                        $other_fees = $item->{TotalAmount}->{CurrencyAmount} ;
                        $total = $products_charges + $products_charges_tax + $shipping_charges + $shipping_charges_tax + $giftwrap_charges + $giftwrap_charges_tax + $promotional_rebates + $selling_fees + $fba_fees + $other_fees;

                        print "\t\t\t\t\$products_charges            = $products_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$products_charges_tax        = $products_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges            = $shipping_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges_tax        = $shipping_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges            = $giftwrap_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges_tax        = $giftwrap_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$marketplace_facilitator_tax = $marketplace_facilitator_tax\n" if $options{verbose} > 2 ;
                        print "\t\t\t\t\$promotional_rebates         = $promotional_rebates\n"         if $options{verbose} > 2 ;
                        print "\t\t\t\t\$selling_fees                = $selling_fees\n"                if $options{verbose} > 2 ;
                        print "\t\t\t\t\$fba_fees                    = $fba_fees\n"                    if $options{verbose} > 2 ;
                        print "\t\t\t\t\$other_fees                  = $other_fees\n"                  if $options{verbose} > 2 ;
                        print "\t\t\t\t\$cc                          = $cc\n"                          if $options{verbose} > 2 ;
                        print "\t\t\tTotal = $total\n" if $options{verbose} > 1 ;

                        $adj->{AmazonOrderId}   = 'NO_ORDER' ;
                        $adj->{MarketplaceName} = 'Amazon.com' ;
                        #
                        # Add each event to the compressed data structure
                        if( exists $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}} )
                        {
                            # Add
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{QuantityShipped}           += $item->{Quantity}            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ProductCharges}            += $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ProductChargesTax}         += $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ShippingCharges}           += $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ShippingChargesTax}        += $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{GiftwrapCharges}           += $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        += $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} += $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{PromotionalRebates}        += $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{SellingFees}               += $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{FBAFees}                   += $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{OtherFees}                 += $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{Total}                     += $total                       ;
                        }
                        else
                        {
                            # Insert
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{Type}                      = $adj->{AdjustmentType}       ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{PostDate}                  = $adj->{PostedDate}           ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{AmazonOrderId}             = $adj->{AmazonOrderId}        ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{MarketplaceName}           = $adj->{MarketplaceName}      ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{SellerSKU}                 = $item->{SellerSKU}           ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{QuantityShipped}           = $item->{Quantity}            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ProductCharges}            = $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ProductChargesTax}         = $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ShippingCharges}           = $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{ShippingChargesTax}        = $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{GiftwrapCharges}           = $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        = $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} = $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{PromotionalRebates}        = $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{SellingFees}               = $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{FBAFees}                   = $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{OtherFees}                 = $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{Total}                     = $total                       ;
                            $financialShipmentEvents->{$feg_id}->{$adj->{AmazonOrderId}}->{$adj->{AdjustmentType}}->{$item->{SellerSKU}}->{CurrencyCode}              = $cc                          ;
                        }
                    }
                }
                else
                {
                    #
                    # We have an adjustment without a shipment or SKU
                    $financialExpenseEvents->{$feg_id}->{$adj->{PostedDate}}->{$adj->{AdjustmentType}}->{Type}         = $adj->{AdjustmentType};
                    $financialExpenseEvents->{$feg_id}->{$adj->{PostedDate}}->{$adj->{AdjustmentType}}->{Description}  = $adj->{AdjustmentType};
                    $financialExpenseEvents->{$feg_id}->{$adj->{PostedDate}}->{$adj->{AdjustmentType}}->{Value}        = $adj->{AdjustmentAmount}->{CurrencyAmount} ;
                    $financialExpenseEvents->{$feg_id}->{$adj->{PostedDate}}->{$adj->{AdjustmentType}}->{CurrencyCode} = $cc ;
                }
            }
        }

        #
        # Chargeback Event List
        if( exists $feg->{FinancialEvents}->{ChargebackEventList}->{ShipmentEvent} )
        {
            foreach my $shipment (@{&force_array($feg->{FinancialEvents}->{ChargebackEventList}->{ShipmentEvent})})
            {
                $shipment->{PostedDate} = &convert_amazon_datetime($shipment->{PostedDate}) if defined $shipment->{PostedDate} ;

                print "\tProcessing ChargebackEvent $feg_id : $shipment->{AmazonOrderId}\n" if $options{verbose} > 1 ;
                print "\t\tAmazonOrderId   = " . &nvl($shipment->{AmazonOrderId})   . "\n" if $options{verbose} > 2 ;
                print "\t\tSellerOrderId   = " . &nvl($shipment->{SellerOrderId})   . "\n" if $options{verbose} > 2 ;
                print "\t\tPostedDate      = " . &nvl($shipment->{PostedDate})      . "\n" if $options{verbose} > 2 ;
                print "\t\tMarketplaceName = " . &nvl($shipment->{MarketplaceName}) . "\n" if $options{verbose} > 2 ;
                if( exists $shipment->{ShipmentItemAdjustmentList} and exists $shipment->{ShipmentItemAdjustmentList}->{ShipmentItem} )
                {

                    foreach my $item (@{&force_array($shipment->{ShipmentItemAdjustmentList}->{ShipmentItem})})
                    {
                        my $products_charges            = 0 ;
                        my $products_charges_tax        = 0 ;
                        my $shipping_charges            = 0 ;
                        my $shipping_charges_tax        = 0 ;
                        my $giftwrap_charges            = 0 ;
                        my $giftwrap_charges_tax        = 0 ;
                        my $marketplace_facilitator_tax = 0 ;
                        my $promotional_rebates         = 0 ;
                        my $selling_fees                = 0 ;
                        my $fba_fees                    = 0 ;
                        my $other_fees                  = 0 ;
                        my $total                       = 0 ;
                        print "\t\t\tOrderItemId     = " . &nvl($item->{OrderAdjustmentItemId}) . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tQuantityShipped = " . &nvl($item->{QuantityShipped})       . "\n" if $options{verbose} > 2 ;
                        print "\t\t\tSellerSKU       = " . &nvl($item->{SellerSKU})             . "\n" if $options{verbose} > 2 ;
                        if( exists $item->{ItemFeeAdjustmentList} and exists $item->{ItemFeeAdjustmentList}->{FeeComponent} )
                        {
                            print "\t\t\t\tFound Fees\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{ItemFeeAdjustmentList}->{FeeComponent})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{FeeType}) . " = " . &nvl($fee->{FeeAmount}->{CurrencyAmount}) . " " . &nvl($fee->{FeeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;

                                $cc = &die_or_set_currency($cc,$fee->{FeeAmount}->{CurrencyCode}) ;
                                if    ( $fee->{FeeType} =~ m/^FBA.*$/              ) { $fba_fees     += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{FeeType} =~ m/^ShippingChargeback$/ ) { $fba_fees     += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{FeeType} =~ m/^Commission$/         ) { $selling_fees += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{FeeType} =~ m/^RefundCommission$/   ) { $selling_fees += $fee->{FeeAmount}->{CurrencyAmount} ; }
                                else                                                 { $other_fees   += $fee->{FeeAmount}->{CurrencyAmount} ; }
                            }
                        }

                        if( exists $item->{ItemChargeAdjustmentList} and exists $item->{ItemChargeAdjustmentList}->{ChargeComponent} )
                        {
                            print "\t\t\t\tFound Charges\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{ItemChargeAdjustmentList}->{ChargeComponent})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{ChargeType}) .  " = " . &nvl($fee->{ChargeAmount}->{CurrencyAmount}) . " " . &nvl($fee->{ChargeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                                $cc = &die_or_set_currency($cc,$fee->{ChargeAmount}->{CurrencyCode}) ;
                                if    ( $fee->{ChargeType} =~ m/^Principal$/      ) { $products_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^Tax$/            ) { $products_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ReturnShipping$/ ) { $shipping_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ShippingCharge$/ ) { $shipping_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^ShippingTax$/    ) { $shipping_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^GiftWrap$/       ) { $giftwrap_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                elsif ( $fee->{ChargeType} =~ m/^GiftWrapTax$/    ) { $giftwrap_charges_tax += $fee->{ChargeAmount}->{CurrencyAmount} ; }
                                else                                                { print STDERR "unknown fee . $fee->{ChargeType}\n" if $options{verbose} ;  $products_charges     += $fee->{ChargeAmount}->{CurrencyAmount} ;}
                            }
                        }

                        if( exists $item->{PromotionAdjustmentList} and exists $item->{PromotionAdjustmentList}->{Promotion} )
                        {
                            print "\t\t\t\tFound Promotions\n" if $options{verbose} > 2 ;
                            foreach my $fee (@{&force_array($item->{PromotionAdjustmentList}->{Promotion})})
                            {
                                print "\t\t\t\t\t" . &nvl($fee->{PromotionId}) . " | " . &nvl($fee->{PromotionType}) . " = " . &nvl($fee->{PromotionAmount}->{CurrencyAmount}) . " " . &nvl($fee->{PromotionAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                                $cc = &die_or_set_currency($cc,$fee->{PromotionAmount}->{CurrencyCode}) ;
                                $promotional_rebates += $fee->{PromotionAmount}->{CurrencyAmount} ;
                            }
                        }
                        $total = $products_charges + $products_charges_tax + $shipping_charges + $shipping_charges_tax + $giftwrap_charges + $giftwrap_charges_tax + $marketplace_facilitator_tax + $promotional_rebates + $selling_fees + $fba_fees + $other_fees;

                        print "\t\t\t\t\$products_charges            = $products_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$products_charges_tax        = $products_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges            = $shipping_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$shipping_charges_tax        = $shipping_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges            = $giftwrap_charges\n"            if $options{verbose} > 2 ;
                        print "\t\t\t\t\$giftwrap_charges_tax        = $giftwrap_charges_tax\n"        if $options{verbose} > 2 ;
                        print "\t\t\t\t\$marketplace_facilitator_tax = $marketplace_facilitator_tax\n" if $options{verbose} > 2 ;
                        print "\t\t\t\t\$promotional_rebates         = $promotional_rebates\n"         if $options{verbose} > 2 ;
                        print "\t\t\t\t\$selling_fees                = $selling_fees\n"                if $options{verbose} > 2 ;
                        print "\t\t\t\t\$fba_fees                    = $fba_fees\n"                    if $options{verbose} > 2 ;
                        print "\t\t\t\t\$other_fees                  = $other_fees\n"                  if $options{verbose} > 2 ;
                        print "\t\t\t\t\$cc                          = $cc\n"                          if $options{verbose} > 2 ;
                        print "\t\t\tTotal = $total\n" if $options{verbose} > 1 ;

                        #
                        # Add each event to the compressed data structure
                        if( exists $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}} )
                        {
                            # Add
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{QuantityShipped}           += $item->{QuantityShipped}     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductCharges}            += $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductChargesTax}         += $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingCharges}           += $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingChargesTax}        += $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapCharges}           += $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        += $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} += $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{PromotionalRebates}        += $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{SellingFees}               += $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{FBAFees}                   += $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{OtherFees}                 += $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{Total}                     += $total                       ;
                        }
                        else
                        {
                            # Insert
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{Type}                      = "Refund"                     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{PostDate}                  = $shipment->{PostedDate}      ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{AmazonOrderId}             = $shipment->{AmazonOrderId}   ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{MarketplaceName}           = $shipment->{MarketplaceName} ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{SellerSKU}                 = $item->{SellerSKU}           ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{QuantityShipped}           = $item->{QuantityShipped}     ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductCharges}            = $products_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ProductChargesTax}         = $products_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingCharges}           = $shipping_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{ShippingChargesTax}        = $shipping_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapCharges}           = $giftwrap_charges            ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{GiftwrapChargesTax}        = $giftwrap_charges_tax        ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{MarketplaceFacilitatorTax} = $marketplace_facilitator_tax ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{PromotionalRebates}        = $promotional_rebates         ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{SellingFees}               = $selling_fees                ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{FBAFees}                   = $fba_fees                    ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{OtherFees}                 = $other_fees                  ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{Total}                     = $total                       ;
                            $financialShipmentEvents->{$feg_id}->{$shipment->{AmazonOrderId}}->{Refund}->{$item->{SellerSKU}}->{CurrencyCode}              = $cc                          ;
                        }
                    }
                }
            }
        }

        #
        # Process Service List
        if( exists $feg->{FinancialEvents}->{ServiceFeeEventList}->{ServiceFeeEvent} )
        {
            foreach my $fee (@{&force_array($feg->{FinancialEvents}->{ServiceFeeEventList}->{ServiceFeeEvent})})
            {
                print "\tProcessing Service Fee $feg_id\n" if $options{verbose} > 1 ;
                foreach my $comp (@{&force_array($fee->{FeeList}->{FeeComponent})})
                {
                    print "\t\tFeeType    = " . &nvl($comp->{FeeType}) . "\n" if $options{verbose} > 2 ;
                    print "\t\tFeeAmount  = " . &nvl($comp->{FeeAmount}->{CurrencyAmount}) . " " . &nvl($comp->{FeeAmount}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                    $cc = &die_or_set_currency($cc,$comp->{FeeAmount}->{CurrencyCode}) ;

                    if( exists $financialExpenseEvents->{$feg_id}->{$feg->{FinancialEventGroupStart}}->{$comp->{FeeType}} )
                    {
                        $financialExpenseEvents->{$feg_id}->{$feg->{FinancialEventGroupStart}}->{$comp->{FeeType}}->{Value} += $comp->{FeeAmount}->{CurrencyAmount} ;
                    }
                    else
                    {
                        $financialExpenseEvents->{$feg_id}->{$feg->{FinancialEventGroupStart}}->{$comp->{FeeType}}->{Type}         = $comp->{FeeType} ;
                        $financialExpenseEvents->{$feg_id}->{$feg->{FinancialEventGroupStart}}->{$comp->{FeeType}}->{Description}  = $comp->{FeeType} ;
                        $financialExpenseEvents->{$feg_id}->{$feg->{FinancialEventGroupStart}}->{$comp->{FeeType}}->{Value}        = $comp->{FeeAmount}->{CurrencyAmount} ;
                        $financialExpenseEvents->{$feg_id}->{$feg->{FinancialEventGroupStart}}->{$comp->{FeeType}}->{CurrencyCode} = $cc ;
                    }
                }
            }
        }

        #
        # Process Product Ads List
        if( exists $feg->{FinancialEvents}->{ProductAdsPaymentEventList}->{ProductAdsPaymentEvent} )
        {
            foreach my $ad (@{&force_array($feg->{FinancialEvents}->{ProductAdsPaymentEventList}->{ProductAdsPaymentEvent})})
            {
                $ad->{postedDate} = &convert_amazon_datetime($ad->{postedDate}) if defined $ad->{postedDate} ;

                print "\tProcessing Product Ad Fees $feg_id : $ad->{invoiceId}\n" if $options{verbose} > 1 ;
                print "\t\ttransactionType  = " . &nvl($ad->{transactionType}) . "\n" if $options{verbose} > 2 ;
                print "\t\tpostedDate       = " . &nvl($ad->{postedDate})      . "\n" if $options{verbose} > 2 ;
                print "\t\tinvoiceId        = " . &nvl($ad->{invoiceId})       . "\n" if $options{verbose} > 2 ;
                print "\t\ttaxValue         = " . &nvl($ad->{taxValue}->{CurrencyAmount})         . " " . &nvl($ad->{taxValue}->{CurrencyCode})         . "\n" if $options{verbose} > 2 ;
                print "\t\ttransactionValue = " . &nvl($ad->{transactionValue}->{CurrencyAmount}) . " " . &nvl($ad->{transactionValue}->{CurrencyCode}) . "\n" if $options{verbose} > 2 ;
                print "\t\tbaseValue        = " . &nvl($ad->{baseValue}->{CurrencyAmount})        . " " . &nvl($ad->{baseValue}->{CurrencyCode})        . "\n" if $options{verbose} > 2 ;
                $cc = &die_or_set_currency($cc,$ad->{taxValue}->{CurrencyCode}) ;
                $cc = &die_or_set_currency($cc,$ad->{transactionValue}->{CurrencyCode}) ;
                $cc = &die_or_set_currency($cc,$ad->{baseValue}->{CurrencyCode}) ;

                my $feeType = 'Advertising' ;
                if( exists $financialExpenseEvents->{$feg_id}->{$ad->{postedDate}}->{$feeType} )
                {
                    $financialExpenseEvents->{$feg_id}->{$ad->{postedDate}}->{$feeType}->{Value}       += $ad->{transactionValue}->{CurrencyAmount} ;
                    $financialExpenseEvents->{$feg_id}->{$ad->{postedDate}}->{$feeType}->{Description} += ", " . $ad->{invoiceId} ;
                }
                else
                {
                    $financialExpenseEvents->{$feg_id}->{$ad->{postedDate}}->{$feeType}->{Type}         = $feeType ;
                    $financialExpenseEvents->{$feg_id}->{$ad->{postedDate}}->{$feeType}->{Description}  = $ad->{invoiceId} ;
                    $financialExpenseEvents->{$feg_id}->{$ad->{postedDate}}->{$feeType}->{Value}        = $ad->{transactionValue}->{CurrencyAmount} ;
                    $financialExpenseEvents->{$feg_id}->{$ad->{postedDate}}->{$feeType}->{CurrencyCode} = $cc ;
                }
            }
        }
    }
}

print "financialEventGroups = "    . Dumper($financialEventGroups)    . "\n" if $options{dumper} ;
print "financialShipmentEvents = " . Dumper($financialShipmentEvents) . "\n" if $options{dumper} ;
print "financialExpenseEvents = "  . Dumper($financialExpenseEvents)  . "\n" if $options{dumper} ;
die if $options{dumper} ;

#
# DB Load function
{
    my $timer = MKPTimer->new("Load", *STDOUT, $options{timing}, 1) ;


    foreach my $feg_id (keys %$financialEventGroups)
    {
        my $timer = MKPTimer->new("\tLoading financial_event_group $feg_id", *STDOUT, $options{timing}, 2) ;
        my $feg = $financialEventGroups->{$feg_id} ;

        print "Loading financial_event_group $feg_id\n" if $options{verbose} > 0 ;

        my $s_sth = $dbh->prepare(${\SELECT_FEG_STATEMENT}) ;
        $s_sth->execute($feg->{FinancialEventGroupId}) or die $s_sth->errstr ;

        if( $s_sth->rows > 0 )
        {
            my $localFeg = $s_sth->fetchrow_hashref() ;

            $feg->{Id} = $localFeg->{id} ;

            if( $localFeg->{processing_status} eq "Closed" )
            {
                print "Skipping existing Closed feg $feg_id\n" if $options{verbose} ;
                next ;
            }

            if( $localFeg->{total} == $feg->{OriginalTotal}->{CurrencyAmount} and
                $localFeg->{processing_status} eq $feg->{ProcessingStatus} )
            {
                print "Skipping feg $feg_id as status and value haven't changed\n" if $options{verbose} ;
                next ;
            }
        }

        #
        # update if it already exists
        if( $s_sth->rows > 0 )
        {
            print "Found non-closed feg $feg->{Id}, reloading\n" if $options{verbose} > 0 ;
            #
            # otherwise insert
            my $u_sth = $dbh->prepare(${\UPDATE_FEG_STATEMENT}) ;
            if( not $u_sth->execute( "www.amazon.com"                          ,
                                     $feg->{FinancialEventGroupId}             ,
                                     $feg->{FundTransferDate}                  ,
                                     $feg->{FundTransferStatus}                ,
                                     $feg->{ProcessingStatus}                  ,
                                     $feg->{FinancialEventGroupStart}          ,
                                     $feg->{FinancialEventGroupEnd}            ,
                                     $feg->{TraceId}                           ,
                                     $feg->{AccountTail}                       ,
                                     $feg->{BeginningBalance}->{CurrencyAmount},
                                     $feg->{OriginalTotal}->{CurrencyAmount}   ,
                                     $feg->{CurrencyCode}                      ,
                                     $feg->{Id}                                ))
            {
                print STDERR "Failed to update FEG " . $feg->{FinancialEventGroupId} . " DBI Error: \"" . $u_sth->errstr . "\".\n" ;
                next ;
            }

            my $dfse_sth = $dbh->prepare(${\DELETE_FSE_STATEMENT}) ;
            if( not $dfse_sth->execute($feg->{Id}) )
            {
                print STDERR "Failed to delete FSE while reloading " . $feg->{FinancialEventGroupId} . " DBI Error: \"" . $dfse_sth->errstr . "\".\n" ;
            }
            my $dfee_sth = $dbh->prepare(${\DELETE_FEE_STATEMENT}) ;
            if( not $dfee_sth->execute($feg->{Id}) )
            {
                print STDERR "Failed to delete FEE while reloading " . $feg->{FinancialEventGroupId} . " DBI Error: \"" . $dfee_sth->errstr . "\".\n" ;
            }
        }
        else
        {
            #
            # otherwise insert
            my $i_sth = $dbh->prepare(${\INSERT_FEG_STATEMENT}) ;
            if( not $i_sth->execute( "www.amazon.com"                          ,
                                     $feg->{FinancialEventGroupId}             ,
                                     $feg->{FundTransferDate}                  ,
                                     $feg->{FundTransferStatus}                ,
                                     $feg->{ProcessingStatus}                  ,
                                     $feg->{FinancialEventGroupStart}          ,
                                     $feg->{FinancialEventGroupEnd}            ,
                                     $feg->{TraceId}                           ,
                                     $feg->{AccountTail}                       ,
                                     $feg->{BeginningBalance}->{CurrencyAmount},
                                     $feg->{OriginalTotal}->{CurrencyAmount}   ,
                                     $feg->{CurrencyCode}                      ) )
            {
                print STDERR "Failed to insert FEG " . $feg->{FinancialEventGroupId} . " DBI Error: \"" . $i_sth->errstr . "\".\n" ;
                next ;
            }

            # pull the id from the new created feg
            $s_sth->execute($feg->{FinancialEventGroupId}) or die $s_sth->errstr ;
            if( $s_sth->rows > 0 )
            {
                $feg->{Id} = $s_sth->fetchrow_hashref()->{id} ;
            }

        }

        #
        # Process Shipment List
        foreach my $order (keys %{$financialShipmentEvents->{$feg_id}})
        {
            my $timer = MKPTimer->new("\tLoading financial_shipment_events $order", *STDOUT, $options{timing}, 2) ;
            foreach my $type (keys %{$financialShipmentEvents->{$feg_id}->{$order}})
            {
                foreach my $sku (keys %{$financialShipmentEvents->{$feg_id}->{$order}->{$type}})
                {
                    print "Load FSE $order $type $sku\n" if $options{verbose} > 0 ;
                    #
                    # insert the item
                    my $ii_sth = $dbh->prepare(${\INSERT_FSE_STATEMENT}) ;
                    if( not $ii_sth->execute( $feg->{Id}                                                                                  ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{Type}                      ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{PostDate}                  ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{AmazonOrderId}             ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{MarketplaceName}           ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{SellerSKU}                 ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{QuantityShipped}           ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{ProductCharges}            ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{ProductChargesTax}         ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{ShippingCharges}           ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{ShippingChargesTax}        ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{GiftwrapCharges}           ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{GiftwrapChargesTax}        ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{MarketplaceFacilitatorTax} ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{PromotionalRebates}        ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{SellingFees}               ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{FBAFees}                   ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{OtherFees}                 ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{Total}                     ,
                                              $financialShipmentEvents->{$feg_id}->{$order}->{$type}->{$sku}->{CurrencyCode}              ) )
                    {
                        print STDERR "Failed to insert FSE DBI Error: \"" . $ii_sth->errstr . "\".\n" ;
                    }
                }
            }
        }

        #
        # Process Expense List
        foreach my $date (keys %{$financialExpenseEvents->{$feg_id}})
        {
            my $timer = MKPTimer->new("\tLoading financial_expense_event $date", *STDOUT, $options{timing}, 2) ;
            foreach my $type (keys %{$financialExpenseEvents->{$feg_id}->{$date}})
            {
                print "Load FEE $date $type\n" if $options{verbose} > 0 ;

                #
                # insert the expense item
                my $ii_sth = $dbh->prepare(${\INSERT_FEE_STATEMENT}) ;
                if( not $ii_sth->execute( $feg->{Id}                                                           ,
                                          $date                                                                ,
                                          $financialExpenseEvents->{$feg_id}->{$date}->{$type}->{Type}         ,
                                          $financialExpenseEvents->{$feg_id}->{$date}->{$type}->{Description}  ,
                                          $financialExpenseEvents->{$feg_id}->{$date}->{$type}->{Value}        ,
                                          $financialExpenseEvents->{$feg_id}->{$date}->{$type}->{CurrencyCode} ) )
                {
                    print STDERR "Failed to insert FEE DBI Error: \"" . $ii_sth->errstr . "\".\n" ;
                }
            }
        }
    }

    $dbh->disconnect() ;
}

sub die_or_set_currency
{
    my $current_cc = shift ;
    my $new_cc     = shift ;

    return $new_cc if not defined $current_cc ;

    die "mix currencies not supported" if $current_cc ne $new_cc ;
    return $new_cc ;
}

sub usage_and_die
{
    my $rc = shift || 0 ;
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program downloads financial data from Amazon MWS and loads it locally
    DEFAULT: Download the current day

usage: $0 [options]
--start         the start time (YYYY-MM-DD) you want to pull
--end           the end   time (YYYY-MM-DD) you want to pull
                    if end is in the future, it'll pull to current date
--duration      alternatively you can use either --start or --end and --duration
                        --duration=(MONTH|WEEK|DAY)
--usage|help|?  print this help
USAGE
    exit($rc) ;
}

