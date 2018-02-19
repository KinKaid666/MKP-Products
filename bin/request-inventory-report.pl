#!/usr/bin/perl -w

use strict;

use Amazon::MWS::Client;
use DateTime;

my $mws = Amazon::MWS::Client->new(access_key_id=>"AKIAIJV4HNLPVHMOOCPQ",
                                   secret_key => "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",
                                   merchant_id => "A1FU4DHGLE4M6A",
                                   marketplace_id => "ATVPDKIKX0DER") ;


#
# https://docs.developer.amazonservices.com/en_US/reports/Reports_ReportType.html#ReportTypeCategories__OrderReports
my $req = $mws->RequestReport(ReportType => '_GET_AFN_INVENTORY_DATA_',
                              StartDate => DateTime->now->add(weeks => -1),
                              EndDate => DateTime->now);

if (my $req_id = $req->{ReportRequestInfo}->[0]->{ReportRequestId}) {
    open my $req, "> inv-request.${req_id}";
    close $req ;
}
