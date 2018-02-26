#!/usr/bin/perl -w

use strict ;

use Amazon::MWS::Client ;
use DateTime ;
use Data::Dumper ;
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

opendir(my $dir, ".") ;
my @requests = map { /(\d+)/ } grep { /^inv\-request\.\d+$/ && -f $_ } readdir($dir) ;
closedir $dir ;

exit unless @requests ;

for my $req (@{$mws->GetReportRequestList(ReportRequestIdList => \@requests)->{ReportRequestInfo}})
{
    if ($req->{ReportProcessingStatus} eq '_DONE_' && (my $report_id = $req->{GeneratedReportId}))
    {
        my $report = $mws->GetReport(ReportId => $report_id) ;
        if (length($report))
        {
            my $date = $req->{CompletedDate} ;
            $date =~ s/^.*([0-9]{4})-([0-9]{2})-([0-9]{2})T.*$/$2-$3-$1/ ;
            open my $file, "> Amazon-fulfilled+Inventory+$date.txt" ;
            print $file $report ;
            close $file ;
            unlink "inv-request.$req->{ReportRequestId}" ;
        }
    }
}
