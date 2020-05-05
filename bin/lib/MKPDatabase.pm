package MKPDatabase ;

use strict ;
use warnings ;

use vars qw(@ISA @EXPORT) ;

require Exporter;
use DBI ;
use MKPTimer ;

@ISA = qw (Exporter);
@EXPORT = qw ( $mwsDB
               $mwsDBro );

use constant MKP_DB_LOADER_FILE => qq(/mkp/private/dbloaderinfo) ;
use constant MKP_DB_WWW_FILE    => qq(/mkp/private/dbwwwinfo) ;

our $mwsDB ;
{
    if( getlogin() eq "ericferg" )
    {
        open my $infofile, '<', ${\MKP_DB_LOADER_FILE} or die $!;
        my $info = <$infofile> ;
        close $infofile ;
        chomp($info) ;
        my ($host, $user, $pass, $dbname) = split(":",$info) ;
        $mwsDB = DBI->connect("DBI:mysql:database=$dbname;host=$host",
                           $user,
                           $pass,
                           {'PrintError' => 1});

    }
}

our $mwsDBro ;
{
    open my $infofile, '<', ${\MKP_DB_WWW_FILE} or die $!;
    my $info = <$infofile> ;
    close $infofile ;
    chomp($info) ;
    my ($host, $user, $pass, $dbname) = split(":",$info) ;
    $mwsDBro = DBI->connect("DBI:mysql:database=$dbname;host=$host",
                                    $user,
                                    $pass,
                                    {'PrintError' => 1});

}
1;

__END__
