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

use constant SKUS_SELECT_STATEMENT => qq( select sku from skus where sku = ? ) ;
use constant OHIS_INSERT_STATEMENT => qq(
    insert into onhand_inventory_reports ( sku, report_date, source_id, condition_name, quantity )
                                  values (   ?,           ?,         ?,              ?,        ? )
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
    "date=s"         => \$options{date},
    "filename=s"     => \$options{filename},
    "print"          => \$options{print},
    "timing"         => sub { $options{timing}++ },
    "debug"          => sub { $options{debug}++ },
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

die "You must provide a --filename and a --date." if (not defined $options{filename} or not defined $options{date}) ;
die "Date format must be YYYY-MM-DD" if (not $options{date} =~ m/[0-9]{4}-[0-9]{2}-[0-9]{2}) ;

my @ohis ;

#
# ingest file
#
# Example:
# seller-sku      fulfillment-channel-sku asin    condition-type  Warehouse-Condition-code        Quantity Available
# MKP-05520       X001D0ASLN      B06XRMDJX3      NewItem SELLABLE        0
# MKP-4413-3      B06XZTZVJP      B06XZTZVJP      NewItem SELLABLE        2
{
    my $timer = MKPTimer->new("File processing", *STDOUT, $options{timing}, 1) ;
    my $lineNumber = 0 ;
    open(INPUTFILE, $options{filename}) or die "Can't open $options{filename}: $!" ;
    while(my $line = <INPUTFILE>)
    {
        chomp($line) ;
        ++$lineNumber ;

        #
        # in case it comes over as a dos file
        $line =~ s/^(.*)\r$/$1/ ;

        #
        # Skip the default headers; there something
        next if $line =~ m/.*sku.*/ || $line =~ m/.*SKU.*/ ;

        # breakdown the row
        my @subs = split(/\t/, $line) ;

        my $ohiLine ;
        $ohiLine->{sku}                     = $subs[0] ;
        $ohiLine->{fulfillment_channel_sku} = $subs[1] ;
        $ohiLine->{asin}                    = $subs[2] ;
        $ohiLine->{condition_type}          = $subs[3] ;
        $ohiLine->{condition_code}          = $subs[4] ;
        $ohiLine->{quantity}                = $subs[5] ;

        die "invalid line $lineNumber : $line" if scalar @subs != 6 ;

        print "Found on line " . $lineNumber . " SKU " . $ohiLine->{sku} . "\n" if $options{debug} > 1 ;
        push @ohis, $ohiLine ;
    }
    close INPUTFILE;
    print "Process file containing $lineNumber line(s).\n" if $options{debug} > 0 ;
    print "  -> Found " . @ohis . " record(s).\n"          if $options{debug} > 0 ;
    print "\@ohis = " . Dumper(\@ohis) . "\n"              if $options{debug} > 1 ;
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

    my $s_stmt = $dbh->prepare(${\SKUS_SELECT_STATEMENT}) ;
    my $i_stmt = $dbh->prepare(${\SKUS_INSERT_STATEMENT}) ;
    foreach my $ohi (@ohis)
    {
        $s_stmt->execute( $ohi->{sku} ) or die $DBI::errstr ;

        if( $s_stmt->rows > 0 )
        {
use constant OHIS_INSERT_STATEMENT => qq(
    insert into onhand_inventory_reports ( sku, report_date, source_id, condition_id, quantity )
                                  values (   ?,           ?,         ?,            ?,        ? )
) ;
            if( not $i_stmt->execute( $ohi->{sku}, $options{report_date}, $ohi->{source_id}, $ohi->{condition_code}, $ohi->{quantity} ) )
            {
                print STDERR "Failed to update " . $ohi->{sku} . ", with error: " . $DBI::errstr . "\n" ;
            }
        }
        else
        {
            die "SKU " . $ohi->{sku} . " not found!\n" ;
        }
    }
    $i_stmt->finish();
    $u_stmt->finish();
    $s_stmt->finish();
}
# Disconnect from the database.
$dbh->disconnect();

sub usage_and_die
{
    my $rc = shift || 0 ; 
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program inserts or updates the skus

usage: $0 [options]
--database     The database to load
--filename     the filename that contains the SKUs
--usage|help|? print this help
USAGE
    exit($rc) ;
}

