package MKPMWS ;

use strict ;
use warnings ;

use vars qw(@ISA @EXPORT) ;

require Exporter;
use DBI ;
use DateTime ;
use Date::Manip ;
use MKPFormatter ;
use MKPDatabase ;

@ISA = qw (Exporter);
@EXPORT = qw ($mws);

use constant SELECT_ORDER_CHANNEL_CREDENTIALS => qq(
    select credentials
      from order_channel_credentials
     where source_name = ?
) ;

our $mws ;
{
    my $credentials ;
    my $sth = $mkpDB->prepare(${\SELECT_ORDER_CHANNEL_CREDENTIALS}) ;
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
    my $ldate = UnixDate(DateTime->now()->set_time_zone($timezone),"%Y%m%d_%H%M%S") ;
    $credentials->{logfile} = "/var/tmp/mws-log.$ldate.txt" ;
    $credentials->{debug} = 0 ;
    $mws = Amazon::MWS::Client->new(%$credentials) ;
}

1;

__END__
