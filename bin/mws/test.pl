#! /usr/bin/perl -wT

use strict ;

use DateTime ;
use Date::Manip ;
use Data::Dumper ;

my $date = DateTime->today() ;
my $string = UnixDate($date, "%Y-%m-%d") ;

print "Today = $string\n" ;
print "Today = " . Dumper($date) . "\n" ;
