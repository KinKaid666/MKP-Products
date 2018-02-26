#!/usr/bin/perl -w

use strict;

use Amazon::MWS::Client;
use DateTime;
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


#
# https://docs.developer.amazonservices.com/en_US/reports/Reports_ReportType.html#ReportTypeCategories__OrderReports
my $req = $mws->RequestReport(ReportType => '_GET_AFN_INVENTORY_DATA_',
                              StartDate => DateTime->now->add(weeks => -1),
                              EndDate => DateTime->now);

if (my $req_id = $req->{ReportRequestInfo}->[0]->{ReportRequestId}) {
    open my $req, "> inv-request.${req_id}";
    close $req ;
}
