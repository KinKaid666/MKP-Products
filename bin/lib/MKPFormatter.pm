package MKPFormatter ;

use strict ;
use warnings ;

use vars qw(@ISA @EXPORT) ;

require Exporter;
use POSIX ;
use Locale::Currency::Format ;

@ISA = qw (Exporter);
@EXPORT = qw (format_column
              format_html_column
              format_integer
              format_decimal
              format_percent
              format_currency
              nvl             );

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

    $partText .= ("0" x ($places - length($part))) if($part < 10 and $part > 0) ;
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

#Ũ
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
