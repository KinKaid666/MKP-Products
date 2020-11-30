#!/usr/bin/perl -w

use strict ;
use warnings;

use Data::Dumper ;
use Getopt::Long ;
use IO::Handle ;
use Date::Manip ;
use Date::Calc ;
use DBI;

# AMZL Specific Libraries
use File::Basename qw(dirname basename) ;
use Cwd qw(abs_path) ;
use lib &dirname(&abs_path($0)) . "/lib" ;
use MKPTimer ;
use MKPDatabase ;

use constant EXPENSES_INSERT_STATEMENT => qq( insert into expenses ( source_name, expense_datetime, type, description, total ) value ( ?, ?, ?, ?, ? ) ) ;
use constant EXPENSES_SELECT_STATEMENT => qq( select expense_datetime, type, description from expenses where expense_datetime = ? and type = ? and description = ? ) ;

my %options ;
$options{source_name} = 'www.mkpproducts.com' ;
$options{timing}   = 0 ;
$options{print}    = 0 ;
$options{debug}    = 0 ; # default

&GetOptions(
    "source_name=s"  => \$options{source_name},
    "type=s"         => \$options{type},
    "description=s"  => \$options{description},
    "value=s"        => \$options{value},
    "datetime=s"     => \$options{datetime},
    "frequency=s"    => \$options{frequency},
    "print"          => \$options{print},
    "timing"         => sub { $options{timing}++ },
    "debug"          => sub { $options{debug}++ },
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

if( not defined $options{type}        and
    not defined $options{source_name} and
    not defined $options{description} and
    not defined $options{value}       and
    (not defined $options{frequency} or
     not defined $options{datetime}))
{
    print STDERR "You must provide a source_name, type, description, value, and either a frequency or a datetime.\n" ;
    &usage_and_die(1) ;
}

if( defined $options{frequency} and defined $options{datetime} )
{
    print STDERR "You must provide a frequency or a datetime.\n" ;
    &usage_and_die(1) ;
}

if( $options{type} eq "Salary" and $options{value} < -4000 )
{
    print STDERR "Salary cannot exceed \$4k\n" ;
    exit(1) ;
}

my @expenses ;

#
# Convert argument to expense
#
if( defined $options{frequency} )
{
    if( not ($options{frequency} eq "DAILY" or
             $options{frequency} eq "WEEKLY" or
             $options{frequency} eq "MONTHLY" or
             $options{frequency} eq "YEARLY"))
    {
        print STDERR "Frequency must be DAILY, WEEKLY, MONTHLY, or YEARLY.\n" ; &usage_and_die(1) ;
    }

    die "Feature not implemented yet.\n" ;
}
else
{
    if( not defined $options{datetime} or not $options{datetime} =~ m/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$/ )
    {
        print STDERR "--datetime must be in the following format: YYYY-MM-DD HH:MM:SS.\n" ;
        &usage_and_die(1) ;
    }
    my $expense ;
    $expense->{datetime}    = $options{datetime} ;
    $expense->{source_name} = $options{source_name} ;
    $expense->{type}        = $options{type} ;
    $expense->{description} = $options{description} ;
    $expense->{value}       = $options{value} ;
    push @expenses, $expense ;
}
print "  -> Found " . @expenses . " non-SKU related expense record(s).\n" if $options{debug} > 0 ;
print "\@expenses = " . Dumper(\@expenses) . "\n"                         if $options{debug} > 1 ;

#
# Insert each expense
{
    my $timer = MKPTimer->new("Insert expenses", *STDOUT, $options{timing}, 1) ;

    my $i_sth = $mkpDB->prepare(${\EXPENSES_INSERT_STATEMENT}) ;
    my $s_sth = $mkpDB->prepare(${\EXPENSES_SELECT_STATEMENT}) ;
    foreach my $expense (@expenses)
    {
        #
        # If we have a cost for this SKU, delete all future dates, terminate the latest and insert the new
        $s_sth->execute($expense->{datetime},$expense->{type},$expense->{description}) or die $DBI::errstr ;
        if( $s_sth->rows > 0 )
        {
            print STDERR "Skipping duplicate expense entry found: expenses_datetime " . $expense->{datetime} . " type " . $expense->{type} . " description " . $expense->{description} . "\n" ;
        }
        else
        {

            #
            # TODO Remove hardcoded Amazon.com
            print "About to load expense type '" . $expense->{type} . "' with description '" . $expense->{description} .
                  "' from " . $expense->{datetime} . " for " . $expense->{value} . "\n" if $options{debug} > 0 ;
            if( not $i_sth->execute( $expense->{source_name},
                                     $expense->{datetime}   ,
                                     $expense->{type}       ,
                                     $expense->{description},
                                     $expense->{value}      ) )
            {
                print STDERR "Failed to insert '" . $expense->{type} . "' from " . $expense->{datetime} . "\n" ;
            }
        }
    }
    $i_sth->finish() ;
    $s_sth->finish() ;
}

# Disconnect from the database.
$mkpDB->disconnect() ;

sub usage_and_die
{
    my $rc = shift || 0 ; 
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program expenses to the database

usage: $0 [options]
--source_name   website that generated the expense (default: www.mkpproducts.com)
--type          type of expense
--description   description of the expense
--value         amount of the expense
--datetime      when the expense occurred
--frequencey    how often the expense should occur (DAILY,WEEKLY,MONTHLY, or YEARLY)
--usage|help|?  print this help
USAGE
    exit($rc) ;
}

