package MKPUser ;

use base qw(Exporter);
use strict;

our @EXPORT = qw($userdbh validate record_visit dienice dbdie);
our @EXPORT_OK = qw();

use DBI;
use CGI qw(:standard);

our $userdbh = DBI->connect( "dbi:mysql:usertable", "usertable", "2018userLogin") or
    &dienice("Can't connect to db: $DBI::errstr");

sub validate
{
    # look for the cookie. if it exists and is valid, return the
    # username associated with that cookie.
     my $username = "";
     if (cookie('cid'))
     {
         my $sth = $userdbh->prepare("select * from user_cookies where cookie_id=?") or &dbdie;
         $sth->execute(cookie('cid')) or &dbdie;
         my $rec;

         # there's a cookie set in the browser but we don't have a record
         # for it in the db.
         unless ($rec = $sth->fetchrow_hashref)
         {
             &goto_login() ;
         }

         # their IP address has changed since the last time they 
         # were here.
         if ($rec->{remote_ip} ne $ENV{REMOTE_ADDR})
         {
             &goto_login() ;
         }
         $username = $rec->{username};
    }
    else
    {
        # no cookie is set. go to the login page.
        &goto_login() ;
    }
    &record_visit($username) ;
    return $username ;
}

sub record_visit
{
    my $user = shift ;
    my $ip = $ENV{REMOTE_ADDR} ;
    my $url = $ENV{REQUEST_URI};
    use constant INSERT_USER_VIEW => qq (
        insert into user_views (username, remote_ip, page ) values ( ?, ?, ? )
    ) ;
    my $i_sth = $userdbh->prepare(${\INSERT_USER_VIEW}) ;
    $i_sth->execute($user, $ip, $url) or &dbdie ;
}

sub dienice
{
    my($msg) = @_;
    print header;
    print start_html("Error");
    print "<h2>Error</h2>\n";
    print $msg;
    exit;
}

sub goto_login
{
    my $url = $ENV{REQUEST_URI};
    $url =~ s/^\/(.*)$/$1/g if defined $url ;
    print redirect("http://prod.mkpproducts.com/login.cgi" . ((defined $url) ? "?$url":""));
    exit;
}

sub dbdie {
    my($package, $filename, $line) = caller;
    my($errmsg) = "Database error: $DBI::errstr<br>\n called from $package $filename line $line";
    &dienice($errmsg);
}

1;
