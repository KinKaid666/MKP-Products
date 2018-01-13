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
use constant SKUS_UPDATE_STATEMENT => qq( update skus set vendor_name = ?, title = ?, description = ? where sku = ? ) ;
use constant SKUS_INSERT_STATEMENT => qq( insert into skus ( sku, vendor_name, title, description ) value ( ?, ?, ?, ? ) ) ;

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

my @skus ;

#
# ingest file
#
# Example:
# "sku","vendor name","Title","Description"
# "MKP-RR013","Wooster","12 Pack Wooster RR013 Jumbo-Koter Roller Frame for 4-1/2" and 6-1/2" Covers - 12" Length","12 Pack Wooster RR013 Jumbo-Koter Roller Frame for 4-1/2" and 6-1/2" Covers - 12" Length"
# "MKP-RR308","Wooster","12 Pack Wooster RR308-4-1/2 Pro Foam 4-1/2" Jumbo-Koter Foam Roller Cover - 2 per Package","12 Pack Wooster RR308-4-1/2 Pro Foam 4-1/2" Jumbo-Koter Foam Roller Cover - 2 per Package"
{
    my $timer = MKPTimer->new("File processing", *STDOUT, $options{timing}, 1) ;
    my $lineNumber = 0 ;
    open(INPUTFILE, $options{filename}) or die "Can't open $options{filename}: $!" ;
    while(my $line = <INPUTFILE>)
    {
        chomp($line) ;
        ++$lineNumber ;

        #
        # Skip the default headers; there something
        next if $line =~ m/.*sku.*/ ;

        #
        # there are quotes around every field and sometimes some empty, unquote fields
        #    First strip the leading and trailing quote
        $line =~ s/^\"(.*)\"$/$1/ ;
        #    Second if there are any empty fields (,,), make sure they are formatted correct (,"",)
        $line =~ s/,,/,"",/g ;
        #    lastly cut all the fields by ","
        my @subs = split(/","/, $line) ;

        my $skuLine ;
        $skuLine->{sku}         = $subs[0] ;
        $skuLine->{vendor_name} = $subs[1] ;
        $skuLine->{title}       = $subs[2] ;
        $skuLine->{description} = $subs[3] ;

        die "invalid line $lineNumber : $line" if scalar @subs != 4 ;

        print "Found on line " . $lineNumber . " SKU " . $skuLine->{sku} . " from vendor " . $skuLine->{vendor_name} . "\n" if $options{debug} > 1 ;
        push @skus, $skuLine ;
    }
    close INPUTFILE;
    print "Process file containing $lineNumber line(s).\n" if $options{debug} > 0 ;
    print "  -> Found " . @skus . " record(s).\n"          if $options{debug} > 0 ;
    print "\@skus = " . Dumper(\@skus) . "\n"              if $options{debug} > 2 ;
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
    my $u_stmt = $dbh->prepare(${\SKUS_UPDATE_STATEMENT}) ;
    my $i_stmt = $dbh->prepare(${\SKUS_INSERT_STATEMENT}) ;
    foreach my $sku (@skus)
    {
        $s_stmt->execute( $sku->{sku} ) or die $DBI::errstr ;

        if( $s_stmt->rows > 0 )
        {
            print STDOUT "SKU " . $sku->{sku} . " found in DB, updating\n" if $options{debug} > 0 ;
            if( not $u_stmt->execute( $sku->{vendor_name}, $sku->{title}, $sku->{description}, $sku->{sku} ) )
            {
                print STDERR "Failed to update " . $sku->{sku} . ", with error: " . $DBI::errstr . "\n" ;
            }
        }
        else
        {
            print STDOUT "SKU " . $sku->{sku} . " not found in DB, inserting\n" if $options{debug} > 0 ;
            if( not $i_stmt->execute( $sku->{sku}, $sku->{vendor_name}, $sku->{title}, $sku->{description} ) )
            {
                print STDERR "Failed to insert " . $sku->{sku} . ", with error: " . $DBI::errstr . "\n" ;
            }
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

