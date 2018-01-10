package MKPTimer ;

use strict ;
use warnings ;

use IO::Handle ;
use Data::Dumper ;
use POSIX ;

use constant seconds_per_minute => 60 ;
use constant seconds_per_hour   => ${\seconds_per_minute} * 60 ;
use constant seconds_per_day    => ${\seconds_per_hour} * 24 ;

sub new
{
    my $class     = shift ;
    my $text      = shift ;
    my $ioHandle  = shift || *STDOUT ;
    my $level     = shift || 0 ;
    my $threshold = shift || 0 ;

    my $self = {
        text      => $text,
        ioHandle  => $ioHandle,
        threshold => int($threshold),
        level     => int($level),
        start     => time,
        end       => undef,
        duration  => undef,
    } ;

    if($self->{level} >= $self->{threshold})
    {
        $self->{ioHandle}->print("MKPTimer '" . $self->{text} . "' started at " . &date_pretty($self->{start}) . "\n") ;
    }
    return bless $self, $class ;
}

sub DESTROY
{
    my $self = shift ;

    $self->{end} = time ;
    $self->{duration} = $self->{end} - $self->{start} ;

    if($self->{level} >= $self->{threshold})
    {
        $self->{ioHandle}->print("MKPTimer '" . $self->{text} . "'   ended at " . &date_pretty($self->{end}) . " ; duration " . &duration_pretty($self->{duration}) . "\n") ;
    }
}

sub date_pretty($)
{
    my $seconds = shift || 0 ;
    my $text = POSIX::strftime( "%Y/%m/%d %H:%M:%S", localtime($seconds) ) ;
    return $text ;
}

sub duration_pretty($)
{
    my $duration = shift || 0 ;

    # Simple duration
    my $days    = &floor($duration / ${\seconds_per_day}) ;
    $duration  -= ($days * ${\seconds_per_day}) ;

    my $hours   = &floor($duration / ${\seconds_per_hour}) ;
    $duration  -= ($hours * ${\seconds_per_hour}) ;

    my $minutes = &floor($duration / ${\seconds_per_minute}) ;
    $duration  -= ($minutes * ${\seconds_per_minute}) ;

    my $seconds = $duration ;

    my $num_elements = 0 ;
    $num_elements += 1 if $days ;
    $num_elements += 1 if $hours ;
    $num_elements += 1 if $minutes ;

    my $text = "" ;
    $text .= $days    . " day"    . ($days    >= 2 ? "s" : "" ) . ($num_elements == 3 ? ", " : " ") if $days    ;
    $text .= $hours   . " hour"   . ($hours   >= 2 ? "s" : "" ) . ($num_elements == 3 ? ", " : " ") if $hours   ;
    $text .= $minutes . " minute" . ($minutes >= 2 ? "s" : "" ) . ($num_elements == 3 ? ", " : " ") if $minutes ;
    $text .= ($days or $hours or $minutes ? "and " : "") ;
    $text .= $seconds . " second" . ($seconds != 1 ? "s" : "" ) ;

    return $text ;
}

1;

__END__
