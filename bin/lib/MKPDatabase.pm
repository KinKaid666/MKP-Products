package MKPDatabase ;

use strict ;
use warnings ;

use vars qw(@ISA @EXPORT) ;

require Exporter;
use DBI ;
use MKPTimer ;

@ISA = qw (Exporter);
@EXPORT = qw ( $mkpDB
               $mkpDBro );

use constant MKP_DB_LOADER_FILE => qq(/mkp/private/dbloaderinfo) ;
use constant MKP_DB_WWW_FILE    => qq(/mkp/private/dbwwwinfo) ;

our $mkpDB ;
{
    if( getlogin() eq "ericferg" )
    {
        open my $infofile, '<', ${\MKP_DB_LOADER_FILE} or die $!;
        my $info = <$infofile> ;
        close $infofile ;
        chomp($info) ;
        my ($host, $user, $pass, $dbname) = split(":",$info) ;
        $mkpDB = DBI->connect("DBI:mysql:database=$dbname;host=$host",
                           $user,
                           $pass,
                           {'PrintError' => 1});

    }
}

our $mkpDBro ;
{
    open my $infofile, '<', ${\MKP_DB_WWW_FILE} or die $!;
    my $info = <$infofile> ;
    close $infofile ;
    chomp($info) ;
    my ($host, $user, $pass, $dbname) = split(":",$info) ;
    $mkpDBro = DBI->connect("DBI:mysql:database=$dbname;host=$host",
                                    $user,
                                    $pass,
                                    {'PrintError' => 1});

}
1;

__END__
