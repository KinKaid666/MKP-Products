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
    "verbose|d+"     => \$options{verbose},
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
    $mws = Amazon::MWS::Client->new(%$credentials) ;
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

