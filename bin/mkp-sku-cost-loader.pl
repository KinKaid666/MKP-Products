#!/usr/bin/perl -w

use strict ;
use warnings;

# normal use POSIX ;
use Data::Dumper ;
use Getopt::Long ;
use POSIX ;
use IO::Handle ;
use Date::Manip ;
use Date::Calc ;
use DBI;

# AMZL Specific Libraries
use File::Basename qw(dirname basename) ;
use Cwd qw(abs_path) ;
use lib &dirname(&abs_path($0)) . "/lib" ;
use MKPTimer ;

use constant SKUS_SELECT_STATEMENT      => qq( select sku from skus where sku = ? ) ;
use constant SKU_COSTS_SELECT_STATEMENT => qq( select sku from sku_costs where sku = ? and end_date is null ) ;

use constant SKU_COSTS_UPDATE_STATEMENT => qq( update sku_costs set end_date = ? where sku = ? and (end_date is null or end_date > ?) ) ;
use constant SKU_COSTS_DELETE_STATEMENT => qq( delete from sku_costs where sku = ? and start_date > ? ) ;
use constant SKU_COSTS_INSERT_STATEMENT => qq( insert into sku_costs ( sku, cost, start_date ) value ( ?, ?, ? ) ) ;

my %options ;
$options{username} = 'mkp_loader'      ;
$options{password} = 'mkp_loader_2018' ;
$options{database} = 'mkp_products'    ;
$options{hostname} = 'mkp.cjulnvkhabig.us-east-2.rds.amazonaws.com'       ;
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

my $sku_costs ;

#
# ingest file
#
# Example:
# SKU,cost
# 7A-8ANN-TPQI,17.58
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
        my @subs = split(/,/, $line) ;
        my $elements = scalar @subs ;

        my $skuLine ;
        $skuLine->{sku}        = $subs[0] ;
        $skuLine->{cost}       = $subs[1] ;
        $skuLine->{start_date} = ( $elements > 2 ? $subs[2] : undef ) ;

        die "invalid line $lineNumber : $line" if ( $elements < 2 or $elements > 3 ) ;
        die "invalid date format on line $lineNumber : $line\n   REQUIRED: YYYY-MM-DD" if $elements > 2 and not ($skuLine->{start_date} =~ m/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) ;

        print STDERR "skipping duplicate entry for " . $skuLine->{sku} . "\n" if $sku_costs->{$skuLine->{sku}} ;

        print "Found on line " . $lineNumber . " SKU " . $skuLine->{sku} . "\n" if $options{debug} > 1 ;
        $sku_costs->{$skuLine->{sku}} = $skuLine ;
    }
    close INPUTFILE;
    print "Process file containing $lineNumber line(s).\n" if $options{debug} > 0 ;

    # not sure why Perl won't let me do this inline
    my @a = keys %$sku_costs ;
    print "  -> Found " . scalar @a . " unique record(s).\n"     if $options{debug} > 0 ;
    print "\$sku_costs = " . Dumper($sku_costs) . "\n\n"     if $options{debug} > 1 ;
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

    my $sku_s_stmt = $dbh->prepare(${\SKUS_SELECT_STATEMENT}) ;

    my $s_stmt = $dbh->prepare(${\SKU_COSTS_SELECT_STATEMENT}) ;
    my $u_stmt = $dbh->prepare(${\SKU_COSTS_UPDATE_STATEMENT}) ;
    my $d_stmt = $dbh->prepare(${\SKU_COSTS_DELETE_STATEMENT}) ;
    my $i_stmt = $dbh->prepare(${\SKU_COSTS_INSERT_STATEMENT}) ;
    foreach my $sku (keys %$sku_costs)
    {
        my $sku_cost = $sku_costs->{$sku} ;

        #
        # Check to see if this is a known SKU
        $sku_s_stmt->execute( $sku_cost->{sku} ) or die $DBI::errstr ;
        if( $sku_s_stmt->rows > 0 )
        {
            #
            # Check to see if there is an exists cost
            $s_stmt->execute( $sku_cost->{sku} ) or die $DBI::errstr ;

            my $start_date = (defined $sku_cost->{start_date} ? $sku_cost->{start_date} : strftime "%Y-%m-%d", localtime) ;
            my $end_date   = UnixDate(DateCalc($start_date, "- 1 day"), "%Y-%m-%d") ;

            #
            # If we have a cost for this SKU, delete all future dates, terminate the latest and insert the new
            if( $s_stmt->rows > 0 )
            {
                print STDOUT "SKU " . $sku_cost->{sku} . " found in DB, updating\n" if $options{debug} > 0 ;
                #
                # delete future costs
                if( not $d_stmt->execute( $sku_cost->{sku}, $end_date ) )
                {
                    print STDERR "Failed to end cost of " . $sku_cost->{sku} . ", with error: " . $DBI::errstr . "\n" ;
                }
                #
                # end the found cost
                if( not $u_stmt->execute( $end_date, $sku_cost->{sku}, $end_date ) )
                {
                    print STDERR "Failed to end cost of " . $sku_cost->{sku} . ", with error: " . $DBI::errstr . "\n" ;
                }

                if( not $i_stmt->execute( $sku_cost->{sku}, $sku_cost->{cost}, $start_date ) )
                {
                    print STDERR "Failed to insert " . $sku_cost->{sku} . ", with error: " . $DBI::errstr . "\n" ;
                }
            }
            else
            {
                #
                # We've never seen this before, insert a new cost starting from the beginning of time
                print STDOUT "SKU " . $sku_cost->{sku} . " not found in DB, inserting\n" if $options{debug} > 0 ;
                if( not $i_stmt->execute( $sku_cost->{sku}, $sku_cost->{cost}, "1970/01/01" ) )
                {
                    print STDERR "Failed to insert " . $sku_cost->{sku} . ", with error: " . $DBI::errstr . "\n" ;
                }
            }
        }
        else
        {
            print STDERR "Skipping unknown SKU " . $sku_cost->{sku} . "\n" ;
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
This program inserts the SKU costs

usage: $0 [options]
--database     The database to load to
--filename     The filename that contains the SKU costs
--usage|help|? print this help
USAGE
    exit($rc) ;
}

