package MKPFormatter ;

use strict ;
use warnings ;

use vars qw(@ISA @EXPORT) ;

require Exporter;
use POSIX ;
use Locale::Currency::Format ;
use DateTime::Format::ISO8601 ;
use Date::Manip ;

@ISA = qw (Exporter);
@EXPORT = qw (format_column
              format_html_column
              format_integer
              format_decimal
              format_percent
              format_currency
              format_date
              $timezone
              convert_amazon_datetime
              convert_amazon_datetime_to_print
              force_array
              nvl             );

our $timezone ;
{
    open my $tz, '<', '/etc/timezone' or die $!;
    my $timezone_name = <$tz>;
    chomp($timezone_name) ;
    $timezone = DateTime::TimeZone->new( name => $timezone_name );
}


sub convert_amazon_datetime
{
    my $date = DateTime::Format::ISO8601->parse_datetime(shift) ;
    $date->set_time_zone($timezone) ;
    return $date ;
}

sub format_date
{
    my $x = shift ;
    my $date ;
    if($x =~ m/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]\.[0-9]+Z$/ ||
       $x =~ m/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]+Z$/)
    {
        $date = DateTime::Format::ISO8601->parse_datetime($x) ;
        $date->set_time_zone($timezone) ;
    }
    elsif($x =~ m/^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-2][0-9]):([0-5][0-9]):([0-5][0-9])$/)
    {
        $date = DateTime->new( year      => $1,
                               month     => $2,
                               day       => $3,
                               hour      => $4,
                               minute    => $5,
                               second    => $6,
                               time_zone => $timezone) ;
    }
    else
    {
        die "Unknown date format: " . $x ;
    }
    return join ' ', $date->ymd, $date->hms, $timezone->short_name_for_datetime($date) ;
}

sub force_array
{
    my $array = shift ;

    $array = [ $array ] if( ref $array ne "ARRAY" ) ; 
    return $array ;
}

#
# print $value no more than $length, justified 0 = left, 1 = right
#
sub format_column
{
    my $value         = shift ;
    my $length        = shift || 0 ;
    my $justification = shift || 0 ;

    my $column = "" ;
    if($justification == 0)
    {
        $column .= $value ;
        $column .= " " x ($length - length($value)) ;
    }
    else
    {
        $column .= " " x ($length - length($value)) ;
        $column .= $value ;
    }

    return substr($column,0,$length) ;
}

sub format_integer
{
    my $number = shift || 0 ;

    # natural round
    $number = int($number + 0.5) ;
    $number =~ s/(\d{1,3}?)(?=(\d{3})+$)/$1,/g ;

    return $number ;
}

sub format_decimal
{
    my $number = shift || 0 ;
    my $places = shift || 0 ;

    my $isNeg = ($number < 0) ;

    if($isNeg)
    {
        $number *= -1 ;
    }

    my $whole = floor($number) ;
    my $part = ($number - $whole) ;

    # natural round and turn into a whole number
    $part = int(($part * (10**$places))+0.5) ;

    if($part >= (10**$places))
    {
        $part = 0 ;
        $whole += 1 ;
    }

    my $wholeText = &format_integer($whole) ;
    my $partText = "" ;

    $partText .= ("0" x ($places - length($part))) if($part < 10 and $part >= 0 and ($places - length($part)) > 0) ;
    $partText .= $part ;

    my $decimal = "" ;
    $decimal .= ($isNeg?"-":"") ;
    $decimal .= $wholeText ;
    $decimal .= "." . $partText if($places) ;

    return $decimal ;
}

sub format_percent
{
    my $number = shift || 0 ;
    my $places = shift || 0 ;

    my $percent = &format_decimal($number * 100,$places) . "%" ;
    return $percent ;
}

sub nvl
{
    my $val  = shift ;
    my $nval = shift || "" ;

    my $rval = $nval ;
    $rval = $val if defined $val ;

    return $rval ;
}

#
# print $value no more than $length, justified 0 = left, 1 = right
#
sub format_html_column
{
    my $value     = shift ;
    my $is_header = shift ;
    my $css_class = shift || undef ;
    my $css_id    = shift || undef ;

    my $type = "td" ;
    $type = "th" if $is_header ;

    my $columnText = "<" . $type ;
    $columnText .= " class=\"$css_class\"" if defined $css_class ;
    $columnText .= "    id=\"$css_id\""    if defined $css_id    ;
    $columnText .= ">" . $value . "</" . $type . ">" ;
    return $columnText ;
}

# Need to write my own
sub format_currency
{
    my $number = shift || 0 ; 
    my $places = shift || 0 ; 

    ### HACK
    $number = &format_decimal($number,$places) ;
    $number =~ s/,//g ;
    $number = currency_format('USD', $number, FMT_SYMBOL) ;
    if($places == 0)
    {
        $number =~ s/\.00$//g ;
    }
    return $number ;
}


1;

__END__
