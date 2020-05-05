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

# mysql> desc skus;
# +---------------+--------------+------+-----+-------------------+-----------------------------+
# | Field         | Type         | Null | Key | Default           | Extra                       |
# +---------------+--------------+------+-----+-------------------+-----------------------------+
# | sku           | varchar(20)  | NO   | PRI | NULL              |                             |
# | vendor_name   | varchar(50)  | YES  | MUL | NULL              |                             |
# | title         | varchar(150) | YES  |     | NULL              |                             |
# | description   | varchar(500) | YES  |     | NULL              |                             |
# | latest_user   | varchar(30)  | YES  |     | NULL              |                             |
# | latest_update | timestamp    | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user | varchar(30)  | YES  |     | NULL              |                             |
# | creation_date | timestamp    | NO   |     | CURRENT_TIMESTAMP |                             |
# +---------------+--------------+------+-----+-------------------+-----------------------------+
use constant SKUS_SELECT_STATEMENT => qq( select sku from skus where sku = ? ) ;
use constant SKUS_UPDATE_STATEMENT => qq( update skus set vendor_name = ?, title = ?, description = ? where sku = ? ) ;
use constant SKUS_INSERT_STATEMENT => qq( insert into skus ( sku, vendor_name, title, description ) value ( ?, ?, ?, ? ) ) ;

# mysql> desc sku_case_packs ;
# +---------------+------------------+------+-----+-------------------+-----------------------------+
# | Field         | Type             | Null | Key | Default           | Extra                       |
# +---------------+------------------+------+-----+-------------------+-----------------------------+
# | sku           | varchar(20)      | NO   | PRI | NULL              |                             |
# | vendor_sku    | varchar(20)      | NO   | PRI | NULL              |                             |
# | pack_size     | int(10) unsigned | NO   | PRI | NULL              |                             |
# | latest_user   | varchar(30)      | YES  |     | NULL              |                             |
# | latest_update | timestamp        | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
# | creation_user | varchar(30)      | YES  |     | NULL              |                             |
# | creation_date | timestamp        | NO   |     | CURRENT_TIMESTAMP |                             |
# +---------------+------------------+------+-----+-------------------+-----------------------------+
use constant SKU_CASE_PACKS_SELECT_STATEMENT => qq( select sku, vendor_sku, pack_size from sku_case_packs where sku = ? ) ;
use constant SKU_CASE_PACKS_UPDATE_STATEMENT => qq( update sku_case_packs set vendor_sku = ?, pack_size = ? where sku = ? ) ;
use constant SKU_CASE_PACKS_INSERT_STATEMENT => qq( insert into sku_case_packs ( sku, vendor_sku, pack_size ) value ( ?, ?, ? ) ) ;


my %options ;
$options{timing}   = 0 ;
$options{print}    = 0 ;
$options{debug}    = 0 ; # default

&GetOptions(
    "database=s"     => \$options{database},
    "filename=s"     => \$options{filename},
    "print"          => \$options{print},
    "timing"         => sub { $options{timing}++ },
    "debug"          => sub { $options{debug}++ },
    "usage|help|?"   => sub { &usage_and_die(0) },
) || &usage_and_die(1) ;

die "You must provide a filename." if (not defined $options{filename}) ;

my @skus ;

#
# ingest file
#
# Example:
# "sku","vendor name","Title","Description","vendor sku","pack size"
# "MKP-RR013","Wooster","12 Pack Wooster RR013 Jumbo-Koter Roller Frame for 4-1/2" and 6-1/2" Covers - 12" Length","12 Pack Wooster RR013 Jumbo-Koter Roller Frame for 4-1/2" and 6-1/2" Covers - 12" Length","RR013","10"
# "MKP-RR308","Wooster","12 Pack Wooster RR308-4-1/2 Pro Foam 4-1/2" Jumbo-Koter Foam Roller Cover - 2 per Package","12 Pack Wooster RR308-4-1/2 Pro Foam 4-1/2" Jumbo-Koter Foam Roller Cover - 2 per Package","RR308","10"
{
    my $timer = MKPTimer->new("File processing", *STDOUT, $options{timing}, 1) ;
    my $lineNumber = 0 ;
    open(INPUTFILE, $options{filename}) or die "Can't open $options{filename}: $!" ;
    while(my $line = <INPUTFILE>)
    {
        chomp($line) ;
        ++$lineNumber ;

        #
        # Skip the default headers; there something
        next if $line =~ m/.*sku.*/ ;

        #
        # there are quotes around every field and sometimes some empty, unquote fields
        #    First strip the leading and trailing quote
        $line =~ s/^\"(.*)\"$/$1/ ;
        #    Second if there are any empty fields (,,), make sure they are formatted correct (,"",)
        $line =~ s/,,/,"",/g ;
        #    lastly cut all the fields by ","
        my @subs = split(/","/, $line) ;

        my $skuLine ;
        $skuLine->{sku}         = $subs[0] ;
        $skuLine->{vendor_name} = $subs[1] ;
        $skuLine->{title}       = $subs[2] ;
        $skuLine->{description} = $subs[3] ;

        if( scalar @subs == 6 )
        {
            $skuLine->{has_pack_info} = 1 ;
            $skuLine->{vendor_sku}  = $subs[4] ;
            $skuLine->{pack_size}   = $subs[5] ;
        }

        die "invalid line $lineNumber : $line " if( not( scalar @subs == 4 or scalar @subs == 6 ) ) ;

        print "Found on line " . $lineNumber . " SKU " . $skuLine->{sku} . " from vendor " . $skuLine->{vendor_name} . "\n" if $options{debug} > 1 ;
        push @skus, $skuLine ;
    }
    close INPUTFILE;
    print "Process file containing $lineNumber line(s).\n" if $options{debug} > 0 ;
    print "  -> Found " . @skus . " record(s).\n"          if $options{debug} > 0 ;
    print "\@skus = " . Dumper(\@skus) . "\n"              if $options{debug} > 2 ;
}

#
# Insert each order
{
    my $timer = MKPTimer->new("INSERT", *STDOUT, $options{timing}, 1) ;

    my $s_stmt = $mkpDB->prepare(${\SKUS_SELECT_STATEMENT}) ;
    my $u_stmt = $mkpDB->prepare(${\SKUS_UPDATE_STATEMENT}) ;
    my $i_stmt = $mkpDB->prepare(${\SKUS_INSERT_STATEMENT}) ;
    foreach my $sku (@skus)
    {
        $s_stmt->execute( $sku->{sku} ) or die $s_stmt->errstr ;

        if( $s_stmt->rows > 0 )
        {
            print STDOUT "SKU " . $sku->{sku} . " found in DB, updating\n" if $options{debug} > 0 ;
            if( not $u_stmt->execute( $sku->{vendor_name}, $sku->{title}, $sku->{description}, $sku->{sku} ) )
            {
                print STDERR "Failed to update " . $sku->{sku} . ", with error: " . $u_stmt->errstr . "\n" ;
            }
        }
        else
        {
            print STDOUT "SKU " . $sku->{sku} . " not found in DB, inserting\n" if $options{debug} > 0 ;
            if( not $i_stmt->execute( $sku->{sku}, $sku->{vendor_name}, $sku->{title}, $sku->{description} ) )
            {
                print STDERR "Failed to insert " . $sku->{sku} . ", with error: " . $i_stmt->errstr . "\n" ;
            }
        }

        if($sku->{has_pack_info})
        {
            my $pack_s_stmt = $mkpDB->prepare(${\SKU_CASE_PACKS_SELECT_STATEMENT}) ;
            $pack_s_stmt->execute($sku->{sku}) or die "'" . $pack_s_stmt->errstr . "'\n" ;

            my $localSCP ;
            if($pack_s_stmt->rows > 0)
            {
                $localSCP = $pack_s_stmt->fetchrow_hashref() ;
                if( $localSCP->{vendor_sku} ne $sku->{vendor_sku} or
                    $localSCP->{pack_size}  ne $sku->{pack_size} )
                {
                    my $pack_u_stmt = $mkpDB->prepare(${\SKU_CASE_PACKS_UPDATE_STATEMENT}) ;
                    if( not $pack_u_stmt->execute($sku->{vendor_sku},$sku->{pack_size},$sku->{sku}) )
                    {
                        print STDERR "Failed to update sku_case_packs DBI Error: \"" . $pack_u_stmt->errstr . "\"\n" ;
                    }
                }
            }
            else
            {
                #
                # not found, insert it
                my $pack_i_stmt = $mkpDB->prepare(${\SKU_CASE_PACKS_INSERT_STATEMENT}) ;
                if( not ($pack_i_stmt->execute($sku->{sku},
                                               $sku->{vendor_sku},
                                               $sku->{pack_size})) )
                {
                    print STDERR "Failed to insert sku_case_pack DBI Error: \"" . $pack_i_stmt->errstr . "\"\n" ;
                }
            }
        }
    }
    $i_stmt->finish();
    $u_stmt->finish();
    $s_stmt->finish();
}
# Disconnect from the database.
$mkpDB->disconnect();

sub usage_and_die
{
    my $rc = shift || 0 ; 
    local $0 = basename($0) ;

# 80 character widge line
#23456789!123456789"1234567890123456789$123456789%123456789^123456789&123456789*
    print <<USAGE;
This program inserts or updates the skus

usage: $0 [options]
--filename     the filename that contains the SKUs
--usage|help|? print this help
USAGE
    exit($rc) ;
}

