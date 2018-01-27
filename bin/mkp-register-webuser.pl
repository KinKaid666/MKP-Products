#!/usr/bin/perl -w
use Email::Valid ;
use DBI ;
use Getopt::Long ;

use strict ;

my %options ;
&GetOptions(
    "username=s"   => \$options{username},
    "password=s"   => \$options{password},
    "name=s"       => \$options{name},
    "email=s"      => \$options{email},
    "usage|help|?" => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

if( not defined $options{username} and
    not defined $options{name} and
    not defined $options{email} )
{
    die "--username, --password, --name, and --email are mandatory!" ;
}

my $username = $options{username} ;
my $password = $options{password} ;
my $realname = $options{name} ;
my $email    = $options{email} ;

if( not defined $password )
{
    print "Enter password: " ;
    system('/bin/stty', '-echo');  # Disable echoing
    my $password1 = <>;
    chomp $password1 ;
    system('/bin/stty', 'echo');   # Turn it back on
    print "\nEnter password again: " ;
    system('/bin/stty', '-echo');  # Disable echoing
    my $password2 = <>;
    chomp $password2 ;
    system('/bin/stty', 'echo');   # Turn it back on
    print "\n" ;
    die "Passwords do not match!" if $password1 ne $password2 ;

    $password = $password1 ;
}

# be sure the username is alphanumeric - no spaces or funny characters
if ($username !~ /^\w{3,}$/)
{
    die "Please use an alphanumeric username at least 3 letters long, with no spaces." ;
}

# be sure the password isn't blank or shorter than 6 chars
if (length($password) < 6) {
    die "Please enter a password at least 6 characters long." ;
}

# be sure they gave a valid e-mail address
unless (Email::Valid->address($email)) {
    die "Please enter a valid e-mail address." ;
}

# check the db first and be sure the username isn't already registered
my $dbh = DBI->connect( "dbi:mysql:usertable", "usertable", "2018userLogin") or die "Can't connect to db: $DBI::errstr" ;
my $sth = $dbh->prepare("select * from users where username = ?") or die $DBI::errstr ;
$sth->execute($username) or &dbdie ;
if (my $rec = $sth->fetchrow_hashref) {
    die "The username `$username' is already in use. Please choose another." ;
}

# we're going to encrypt the password first, then store the encrypted
# version in the database.
my $encpass = &encrypt($password) ;

$sth = $dbh->prepare("insert into users (username, password, status, realname, email) values(?, ?, ?, ?, ?)")  or die $DBI::errstr ;
$sth->execute($username, $encpass, "CURRENT", $realname, $email)  or die $DBI::errstr ;

sub encrypt {
    my($plain) = @_ ;
    my(@salt) = ('a'..'z', 'A'..'Z', '0'..'9', '.', '/') ;
    return crypt($plain, $salt[int(rand(@salt))] .  $salt[int(rand(@salt))] 	) ;
}

sub usage_and_die
{
    my $rc = shift || 0 ;
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program expenses to the database

usage: $0 [options]
--username      username
--password      password
--name          full name
--email         email
--usage|help|?  print this help
USAGE
    exit($rc) ;
}

