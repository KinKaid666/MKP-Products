#!/usr/bin/perl -w

use strict ;

use Amazon::MWS::Client ;
use DateTime ;
use Data::Dumper ;

my $mws = Amazon::MWS::Client->new(access_key_id  =>"AKIAIJV4HNLPVHMOOCPQ",
                                   secret_key     => "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",
                                   merchant_id    => "A1FU4DHGLE4M6A",
                                   marketplace_id => "ATVPDKIKX0DER") ;

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
