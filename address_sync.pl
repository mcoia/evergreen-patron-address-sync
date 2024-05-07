#!/usr/bin/perl


use lib qw(../../../ ../../ ../ ./);
use Data::Dumper;
use Encode;
use Text::CSV;
use DBD::Pg;
use DateTime;
use DateTime::Format::Duration;
use DateTime::Span;
use Getopt::Long;
use XML::Simple;
use Email::MIME;
use Net::SFTP;

my $xmlconf = "/openils/conf/opensrf.xml";
our $log;
our $logWrites = 0;
our $configFile;
our $dbHandler;
our $debug   = 0;
our %conf;
our %importFileColMap = ();
our %correctedColMap = ();
our $dbtable;
our $baseArchiveFolder;
our $importFlag;
our $testFTP;

my $logFile;
my %fileParsingReport = ();

our @ogColMapOrder = ('address_id', 'address_type', 'street1', 'street2', 'city', 'county', 'state', 'post_code', 'valid');
our @correctedColMapOrder = ('marStreet1', 'marStreet2', 'marCity', 'marPost_Code', 'matching_score', 'distance');
our %addressComparison =
(
'street1' => 'marStreet1',
'street2' => 'marStreet2',
'city' => 'marCity',
'post_code' => 'marPost_Code'
);

GetOptions (
    "xmlconfig=s" => \$xmlconf,
    "config=s"    => \$configFile,
    "import" => \$importFlag,
    "testftp" => \$testFTP,
    "debug" => \$debug
) or printHelp();

checkCMDArgs();
   
log_write( " ---------------- Script Starting ---------------- ", 1 );
    
runTestFTP() if($testFTP);

fillColMaps();

setupSchema();

cleanDatabase();

generateExport() if(!$importFlag);

import() if($importFlag);

log_write( " ---------------- Script End ---------------- ", 1 );

close($log);


sub runTestFTP
{
    `touch testfile.txt`;

    my @files = ('testfile.txt');

    send_sftp($conf{'ftphost'}, $conf{'ftplogin'}, $conf{'ftppass'}, $conf{'remote_directory'}, \@files);

    unlink 'testfile.txt';

    exit;
}

sub import
{
    my @files = @{getFiles($conf{'importpath'})};
    loadImportFile($_) foreach(@files);

    # filter rows where the incoming data is out dated compared to production columns
    filterProductionChanged();
    importUpgradedAddresses();
    reportImportResults();
}

sub generateExport
{   
    my $libs = trim( $conf{"scopedlibs"} );
    @includedOrgUnitIDs = @{ getOrgUnits($libs) };
    my $pgLibs  = makeLibList( \@includedOrgUnitIDs );
    my $dt      = DateTime->now( time_zone => "local" );
    my $fdate   = $dt->ymd;

    my $filenameprefix = trim( $conf{"filenameprefix"} );
    my $limit          = 500;
    my $offset         = 0;
    my $file           = chooseNewFileName( $baseArchiveFolder, $filenameprefix . "_" . $fdate, "tsv" );
    print "Creating: $file\n";
    my $fileHandle = startFile($file);
    my @ids;
    my $count = 0;

    @ids = @{getAddressIDs($pgLibs, $limit, $offset)};
    my $firstTime  = 1;
    while ( $#ids > -1 )
    {
        my @data;
        @data = @{getAddressChunk(\@ids)};
        #last row has header info
        my $h = pop @data;

        if ($firstTime)
        {
            $firstTime = 0;
            my @head = ( [ @{$h} ] );
            writeData( \@head, $fileHandle );
        }
        $count += $#data;
        print "$count\n" if( ($count % 50000 == 0) && $debug);
        writeData( \@data, $fileHandle );
        $offset += $limit;

        undef $h;
        @ids = @{getAddressIDs($pgLibs, $limit, $offset)};
    }
    close($fileHandle);
    print "$count record(s) written to: $file\n";
    if($count > 0 && $conf{'transfermethod'} eq 'sftp')
    {
        my @files = ($file);
        send_sftp($conf{'ftphost'}, $conf{'ftplogin'}, $conf{'ftppass'}, $conf{'remote_directory'}, \@files);
        undef @files;
    }
}

sub writeData
{
    my $dataRef    = shift;
    my @data       = @{$dataRef};
    my $fileHandle = shift;
    my $output     = '';
    foreach (@data)
    {
        my @row = @{$_};
        $output .= join( "\t", @row );
        $output .= "\n";
    }
    print $fileHandle $output;
}

sub getAddressChunk
{
    my $idRef   = shift;
    my @ids     = @{$idRef};
    my $pgArray = join( ',', @ids );
    my $query = "select ";
    foreach(@ogColMapOrder)
    {
        my $col = $_;
        $query .= 'id "address_id",'."\n" if($col eq 'address_id');
        $query .= '(CASE WHEN ' . $col . ' IS NULL THEN \'NULL\' ELSE '.
                  'REGEXP_REPLACE(' . $col . '::TEXT ,$$\t$$::TEXT, $$$$::TEXT ,$$g$$::TEXT) '.
                  'END) "' . $col . '",' . "\n"
            if($col ne 'address_id');

    }
    # remove trailing newline character and comma
    $query = substr($query,0,-2);
    $query .= "\nFROM actor.usr_address";
    $query .= "\nWHERE id IN( $pgArray )";
    log_write($query) if $debug;
    return dbhandler_query($query,0,1);
}

sub getAddressIDs
{
    my $pgLibs = shift;
    my $limit  = shift;
    my $offset = shift;
    my @ret    = ();
    my $query  = "
    SELECT aua.id
    FROM
    actor.usr_address aua
    JOIN actor.usr au ON(au.id=aua.usr)
    WHERE
    NOT au.deleted AND
    au.active AND
    au.home_ou IN( $pgLibs )
    GROUP BY 1
    ORDER BY 1";
    my @results = @{ getDataChunk( $query, $limit, $offset ) };
    #last row has column header info, dropping it
    pop @results;

    foreach (@results)
    {
        my @row = @{$_};
        push( @ret, @row[0] );
    }
    return \@ret;
}

sub makeLibList
{
    my $libsRef = shift;
    my @libs    = $libsRef ? @{$libsRef} : ();
    my $pgLibs  = join( ',', @libs );
    return -1 if $#libs == -1;
    return $pgLibs;
}

sub getDataChunk
{
    my $query  = shift;
    my $limit  = shift;
    my $offset = shift;
    $query .= "\nLIMIT $limit OFFSET $offset";
    log_write($query) if $debug;
    return dbhandler_query($query);
}

sub loadImportFile
{
    my $file = shift;
    print "Processing $file\n";
    my $path;
    my @sp = split('/',$file);
    $path = substr($file,0,( (length(@sp[$#sp]))*-1) );
    my $bareImportFilename = pop @sp;
    $fileParsingReport{"*** $bareImportFilename ***"} = "\r\n";

    checkFileReady($file);
    my $csv = Text::CSV->new ( { sep_char => "\t" } )
        or die "Cannot use CSV: ".Text::CSV->error_diag ();
    open my $fh, "<:encoding(utf8)", $file or die "$file: $!";
    my $rownum = 0;
    my $success = 0;
    my $accumulatedTotal = 0;
    my $queryByHand = '';
    my $parameterCount = 1;
    
    my $queryInserts = "INSERT INTO $dbtable(";
    $queryByHand = "INSERT INTO $dbtable(";
    my @order = ();
    my $sanitycheckcolumnnums = 0;
    my @queryValues = ();
    while ( (my $key, my $value) = each(%importFileColMap) )
    {
        $queryInserts .= $value.",";
        $queryByHand .= $value.",";
        push @order, $key;
        $sanitycheckcolumnnums++
    }
    log_write("Expected columns: $sanitycheckcolumnnums");
    $queryInserts = substr($queryInserts,0,-1);
    $queryByHand = substr($queryByHand,0,-1);
    
    $queryInserts .= ")\nVALUES \n";
    $queryByHand  .= ")\nVALUES \n";
    
    my $queryInsertsHead = $queryInserts;
    my $queryByHandHead = $queryByHand;
    
    while ( my $row = $csv->getline( $fh ) )
    {
        my $valid = 0;
        my @rowarray = @{$row};
        if(scalar @rowarray != $sanitycheckcolumnnums )
        {
            log_write("Error parsing line $rownum\nIncorrect number of columns: ". scalar @rowarray);
            $fileParsingReport{"*** $bareImportFilename ***"} .= "Error parsing line $rownum\nIncorrect number of columns: ". scalar @rowarray . "\r\n";
        }
        else
        {
            $valid = 1;
            my $thisLineInsert = '';
            my $thisLineInsertByHand = '';
            my @thisLineVals = ();
            
            foreach(@order)
            {
                my $colpos = $_;
                # print "reading $colpos\n";
                $thisLineInsert .= '$'.$parameterCount.',';
                $parameterCount++;
                # Trim whitespace off the data
                @rowarray[$colpos] =~ s/^\s+//;
                @rowarray[$colpos] =~ s/^\t+//;
                @rowarray[$colpos] =~ s/\s+$//;
                @rowarray[$colpos] =~ s/\t+$//;
                # Some bad characters can mess with some processes later. Excel loves these \xA0
                @rowarray[$colpos] =~ s/\x{A0}//g;
                                    
                $thisLineInsertByHand.="\$data\$".@rowarray[$colpos]."\$data\$,";
                push (@thisLineVals, @rowarray[$colpos]);
                # log_write(Dumper(\@thisLineVals));
            }
            
            if($valid)
            {
                $thisLineInsert = substr($thisLineInsert,0,-1);
                $thisLineInsertByHand = substr($thisLineInsertByHand,0,-1);
                $queryInserts .= '(' . $thisLineInsert . "),\n";
                $queryByHand .= '(' . $thisLineInsertByHand . "),\n";
                foreach(@thisLineVals)
                {
                    # print "pushing $_\n";
                    push (@queryValues, $_);
                }
                $success++;
            }
            undef @thisLineVals;
        }
        $rownum++;
        
        if( ($success % 500 == 0) && ($success != 0) )
        {
            $accumulatedTotal+=$success;
            $queryInserts = substr($queryInserts,0,-2);
            $queryByHand = substr($queryByHand,0,-2);
            log_write($queryByHand);
            # print ("Importing $success\n");
            $fileParsingReport{"*** $bareImportFilename ***"} .= "Importing $accumulatedTotal / $rownum\r\n";
            log_write("Importing $accumulatedTotal / $rownum");
            dbhandler_update($queryInserts,\@queryValues);
            $success = 0;
            $parameterCount = 1;
            @queryValues = ();
            $queryInserts = $queryInsertsHead;
            $queryByHand = $queryByHandHead;
        }
    }

    $queryInserts = substr($queryInserts,0,-2) if $success;
    $queryByHand = substr($queryByHand,0,-2) if $success;
    
    # Handle the case when there is only one row inserted
    if($success == 1)
    {
        $queryInserts =~ s/VALUES \(/VALUES /;            
        $queryInserts = substr($queryInserts,0,-1);
    }

    # log_write($queryInserts);
    log_write($queryByHand);
    # log_write(Dumper(\@queryValues));

    close $fh;
    $accumulatedTotal+=$success;
    $fileParsingReport{"*** $bareImportFilename ***"} .= "\r\nImporting $accumulatedTotal / $rownum"  if $success;
    log_write("Importing $accumulatedTotal / $rownum") if $success;
    
    dbhandler_update($queryInserts,\@queryValues) if $success;
    
    my $query = "UPDATE $dbtable SET file_name = \$data\$$bareImportFilename\$data\$ WHERE file_name IS NULL";
    log_write($query) if $accumulatedTotal;
    dbhandler_update($query) if $accumulatedTotal;

    # remove the header row from the file, if it was loaded into the database
    my $query = "DELETE FROM $dbtable WHERE address_id = 'id' or street1 = 'street1'";
    log_write($query) if $accumulatedTotal;
    dbhandler_update($query) if $accumulatedTotal;

    # convert "NULL" to real database null
    if($accumulatedTotal)
    {
        my $queryBase = "UPDATE $dbtable SET ";
        while ( (my $key, my $value) = each(%importFileColMap) )
        {
            # the word "null" needs to be real DB null
            my $query = $queryBase . $value . " = null WHERE BTRIM(UPPER( $value )) = 'NULL' AND NOT dealt_with";
            log_write($query);
            dbhandler_update($query);
            # empty strings need to be null
            my $query = $queryBase . $value . " = null WHERE BTRIM( $value ) = '' AND NOT dealt_with";
            log_write($query);
            dbhandler_update($query);
        }
    }
    unlink $file;
}

sub filterProductionChanged
{
    my $queryBase = "
    UPDATE $dbtable synctable
    SET
    error_message = \$\$Production address !!column!! changed\$\$
    FROM
    actor.usr_address aua
    WHERE
    NOT dealt_with AND
    aua.id=synctable.address_id::bigint AND
    ";
    foreach(@ogColMapOrder)
    {
        my $query = $queryBase;
        if( ($_ ne 'address_id') && ($_ ne 'valid') )
        {
            $query =~ s/!!column!!/$_/g;
            $query .= "BTRIM(COALESCE(synctable.$_, '')) != BTRIM(COALESCE(aua.$_, ''))";
            log_write($query);
            dbhandler_update($query);
        }
    }
}

sub importUpgradedAddresses
{
    while ( (my $originalCol, my $correctedCol) = each(%addressComparison) )
    {
        my $query = "
        SELECT id FROM $dbtable WHERE NOT dealt_with AND error_message IS NULL
        AND
        BTRIM(COALESCE($originalCol, '')) != BTRIM(COALESCE($correctedCol, ''))
        ORDER BY 1";
        log_write($query);
        my @ids = @{dbhandler_query($query)};
        updateProductionAddressColumn($_->[0], $originalCol, $correctedCol) foreach(@ids);
    }
}

sub updateProductionAddressColumn
{
    my $stagingTableID = shift;
    my $productionCol = shift;
    my $localCol = shift;
    my $query = "
    UPDATE actor.usr_address aua
    SET $productionCol = synctable.$localCol
    FROM
    $dbtable synctable
    WHERE
    synctable.id = \$1 AND
    aua.id=synctable.address_id::bigint
    ";
    my @vars = ($stagingTableID);
    log_write($query);
    if( dbhandler_update($query, \@vars) )
    {
       $query = "
       UPDATE $dbtable synctable
       SET imported = true
       WHERE
       id = \$1";
       dbhandler_update($query, \@vars);
    }
    else
    {
        $query = "
        UPDATE $dbtable synctable
        SET
        imported = false,
        error_message = \$\$error updating production $productionCol\$\$
        WHERE
        id = \$1";
        dbhandler_update($query, \@vars);
        return 0;
    }
    return 1;
}

sub reportImportResults
{
    my %reporting = ();
    $query = "select count(*) from $dbtable where not dealt_with";
    @results = @{dbhandler_query($query)};
    $reporting{"Total Lines"} = $results[0][0] || 0;

    $query = "SELECT (CASE WHEN imported THEN 'Success' ELSE 'Failed' END) , count(*) FROM $dbtable WHERE NOT dealt_with GROUP BY 1 ORDER BY 1";
    @results = @{dbhandler_query($query)};
    $reporting{"*** Success Breakdown ***"} = "\r\n";
    foreach(@results)
    {
        my @row = @{$_};
        $reporting{"*** Success Breakdown ***"} .= @row[0]."   ".@row[1]."\r\n";
    }

    my $errored = "";
    $reporting{"Total lines with errors"} = 0;
    $query = "SELECT address_id, error_message FROM $dbtable WHERE NOT dealt_with AND error_message IS NOT NULL AND error_message!=\$\$\$\$";
    @results = @{dbhandler_query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        $errored.="( " . @row[0] . " ) ERROR = '".@row[1]."\n";
        $reporting{"Total with errors"}++;
    }
    my $processedFileNames = "";
    my $fileCount = 0;
    $query = "SELECT DISTINCT file_name FROM $dbtable WHERE NOT dealt_with";
    @results = @{dbhandler_query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        $processedFileNames .= @row[0] . "\r\n";
        $fileCount++;
    }

    if($fileCount > 0)
    {
        # Finally, mark all of the rows dealt_with for next execution to ignore
        # Closing these rows off before* the email gets sent, so that if the email
        # doesn't work for some reason, these rows have been marked off
        markFinished();

        my $body = "
Dear staff,

Your address file(s) has been processed.

Filename(s):
$processedFileNames\r\n\r\nSummary:\r\n\r\n";

        while ( (my $key, my $value) = each(%fileParsingReport) )
        {
            $body.=$key.": ".$value;
        }
        $body.="\r\n\r\n";

        my $lastReport = "";
        while ( (my $key, my $value) = each(%reporting) )
        {
            $body.=$key.": ".$value."\n" if !($key =~ m/\*/g);
            $lastReport .=$key.": ".$value."\n" if ($key =~ m/\*/g);
        }
        $body.="\r\n$lastReport";

        $body.="\r\n\r\nErrored records:\r\n$errored" if $reporting{"Total with errors"} > 0;

        $body.="\r\n\r\n-MOBIUS Perl Squad-";

        my $subject = trim( ($conf{'importemailsubjectline'} ? $conf{'importemailsubjectline'} : 'address import results') );
        my @tolist = ( $conf{"alwaysemail"} );
        my $email;
        $email = email_setup( $conf{"fromemail"}, \@tolist, 0, 1);
        email_send( $email,  "Evergreen Utility - " . $subject . " - $fileCount file(s)", $body );
    }
}

sub markFinished
{
    my $query = "UPDATE $dbtable SET dealt_with = true WHERE NOT dealt_with";
    log_write($query);
    dbhandler_update($query);
}

sub getFiles
{
	my $path = shift;
    my @ret = ();
	opendir(DIR, $path) or die $!;
	while (my $file = readdir(DIR)) 
	{
		if ( (-e "$path/$file") && !($file =~ m/^\./) )
		{
            # print "pushing $path/$file\n";
			push @ret, "$path/$file"
		}
	}
	return \@ret;
}

sub checkFileReady
{
    my $file = shift;
    my $worked = open (inputfile, '< '. $file);
    my $trys=0;
    if(!$worked)
    {
        print "******************$file not ready *************\n";
    }
    while (!(open (inputfile, '< '. $file)) && $trys<100)
    {
        print "Trying again, attempt $trys\n";
        $trys++;
        sleep(1);
    }
    close(inputfile);
}

sub setupSchema
{
    $dbtable = $conf{'dbtable'};
    my @schema_table = split(/\./,$dbtable);
    if($#schema_table != 1)
    {
        print "Please specify a valid schema and table via --db-table argument. AKA: mymig.address_sync\n";
        exit;
    }
    my %dbconf = %{ getDBconnects($xmlconf) };
    log_write( "got XML db connections", 1 );
    dbhandler_setupConnection( $dbconf{"db"}, $dbconf{"dbhost"}, $dbconf{"dbuser"}, $dbconf{"dbpass"}, $dbconf{"port"} );

    my $schema = @schema_table[0];
    my $tableName = @schema_table[1];
	my $query = "SELECT * FROM information_schema.columns WHERE table_name = '$tableName' AND table_schema='$schema'";
	my @results = @{dbhandler_query($query)};
	if($#results==-1)
	{
        $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name='$schema'";
        my @results = @{dbhandler_query($query)};
        if($#results <  0)
        {
            $query = "CREATE SCHEMA $schema";
            dbhandler_update($query);
        }

		$query = "CREATE TABLE $dbtable
		(
		id bigserial NOT NULL,
        ";
        foreach(@ogColMapOrder) { $query.= $_." text,"; }
        foreach(@correctedColMapOrder) { $query.= $_." text,"; }
        $query.="
        file_name text,
        imported boolean default false,
        error_message text,
        dealt_with boolean default false,
        insert_date timestamp with time zone NOT NULL DEFAULT now(),
        CONSTRAINT $schema"."_$tableName"."_id_pkey PRIMARY KEY (id)
        )";

        log_write($query);
		dbhandler_update($query);

        $query = "CREATE INDEX $dbtable"."_dealt_with_idx ON $schema.$dbtable (dealt_with);";

        log_write($query);
		dbhandler_update($query);

	}
}

sub cleanDatabase
{
    $query = "
    DELETE FROM $dbtable
    WHERE
    insert_date < now() - '1 year'::INTERVAL";
    my @vars = ();
    log_write($query);
    dbhandler_update($query, \@vars);
    undef @vars;
    return 0;
}

sub printHelp
{
    my $help = "Usage: ./address_sync.pl [OPTION]...

    This program is designed to export patron addresses, and allow a third party to make corrections,
    then re-import the same filt with the third party corrections. Keep in mind that the third party
    cannot alter the original columns. Those are used to match back. They need to append* columns to
    the file. This program has two modes: import and export. And export is assumed unless \"import\"
    flag is present.

    --config     path to config file
    --xmlconfig  pathto_opensrf.xml (optional)
";

    print $help;
    exit 0;
}


sub checkCMDArgs
{
    print "Checking command line arguments...\n" if $debug;

    if ( !-e $xmlconf )
    {
        print "$xmlconf does not exist.\nEvergreen database xml configuration "
          . "file does not exist. Please provide a path to the Evergreen opensrf.xml "
          . "database conneciton details. --xmlconf\n";
        exit 1;
    }

    if ( !-e $configFile )
    {
        print "$configFile does not exist. Please provide a path to your configuration file: " . " --config\n";
        exit 1;
    }

    # Init config
    my $conf = readConfFile($configFile);
    %conf = %{$conf};

    my @reqs =
    (
        "logfile", "scopedlibs", "importpath", "ftplogin",
        "ftppass", "ftphost", "remote_directory", "importemailsubjectline", "exportemailsubjectline",
        "archive", "transfermethod"
    );
    my @missing = ();
    for my $i ( 0 .. $#reqs )
    {
        push( @missing, @reqs[$i] ) if ( !$conf{ @reqs[$i] } );
    }

    if ( $#missing > -1 )
    {
        print "Please specify the required configuration options:\n";
        print "$_\n" foreach (@missing);
        exit 1;
    }
    if ( !-e $conf{"archive"} )
    {
        print "Archive folder: " . $conf{"archive"} . " does not exist.\n";
        exit 1;
    }

    if ( lc $conf{"transfermethod"} ne 'sftp' )
    {
        print "Transfer method: " . $conf{"transfermethod"} . " is not supported\n";
        exit 1;
    }

    # Init logfile
    log_init();

    log_write( "Valid Config", 1 );

    undef @missing;
    undef @reqs;

}

sub makeEvenWidth    #line, width
{
    my $ret   = shift;
    my $width = shift;

    $ret = substr( $ret, 0, $width ) if ( length($ret) > $width );

    $ret .= " " while ( length($ret) < $width );
    return $ret;
}

sub log_init
{
    open( $log, '> ' . $conf{"logfile"} )
      or die "Cannot write to: " . $conf{"logfile"};
    binmode( $log, ":utf8" );
}

sub log_addLogLine
{
    my $line = shift;

    my $dt   = DateTime->now( time_zone => "local" );
    my $date = $dt->ymd;
    my $time = $dt->hms;

    my $datetime = makeEvenWidth( "$date $time", 20 );
    print $log $datetime, ": $line\n";
}

sub log_write
{
    my $line             = shift;
    my $includeTimestamp = shift;
    $logWrites++;
    log_addLogLine($line) if $includeTimestamp;
    print $log "$line\n"  if !$includeTimestamp;

    # flush logs to disk every 100 lines, speed issues if we flush with each write
    if ( $logWrites % 100 == 0 )
    {
        close($log);
        open( $log, '>> ' . $conf{"logfile"} );
        binmode( $log, ":utf8" );
    }
}

sub readConfFile
{
    my $file = shift;

    my %ret = ();
    my $ret = \%ret;

    my @lines = @{ readFile($file) };

    foreach my $line (@lines)
    {
        $line =~ s/\n//;    #remove newline characters
        $line =~ s/\r//;    #remove newline characters
        my $cur = trim($line);
        if ( length($cur) > 0 )
        {
            if ( substr( $cur, 0, 1 ) ne "#" )
            {
                my @s     = split( /=/, $cur );
                my $Name  = shift @s;
                my $Value = join( '=', @s );
                $$ret{ trim($Name) } = trim($Value);
            }
        }
    }

    return \%ret;
}

sub dbhandler_query
{
    my $querystring = shift;
    my $valuesRef   = shift;
    my $includeTableHeader   = shift;
    my @values      = $valuesRef ? @{$valuesRef} : ();
    my @ret;

    my $query;
    $query = $dbHandler->prepare($querystring);
    my $i = 1;
    foreach (@values)
    {
        $query->bind_param( $i, $_ );
        $i++;
    }
    $query->execute();

    while ( my $row = $query->fetchrow_arrayref() )
    {
        push( @ret, [ @{$row} ] );
    }
    undef($querystring);
    push( @ret, $query->{NAME} ) if $includeTableHeader;

    return \@ret;
}

sub dbhandler_update
{
    my $querystring = shift;
    my $valRef      = shift;
    my @values      = ();
    @values = @{$valRef} if $valRef;
    my $q = $dbHandler->prepare($querystring);
    my $i = 1;
    foreach (@values)
    {
        my $param = $_;
        if ( lc( $param eq 'null' ) )
        {
            $param = undef;
        }
        $q->bind_param( $i, $param );
        $i++;
    }
    my $ret = $q->execute();
    return $ret;
}

sub dbhandler_setupConnection
{
    my $dbname = shift;
    my $host   = shift;
    my $login  = shift;
    my $pass   = shift;
    my $port   = shift;

    $dbHandler = DBI->connect(
        "DBI:Pg:dbname=$dbname;host=$host;port=$port",
        $login, $pass,
        {
            AutoCommit       => 1,
            post_connect_sql => "SET CLIENT_ENCODING TO 'UTF8'",
            pg_utf8_strings  => 1
        }
    );

}

sub getDBconnects
{
    my $openilsfile = shift;
    my $xml         = new XML::Simple;
    my $data        = $xml->XMLin($openilsfile);
    my %dbconf;
    $dbconf{"dbhost"} = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
    $dbconf{"db"}     = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
    $dbconf{"dbuser"} = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
    $dbconf{"dbpass"} = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
    $dbconf{"port"}   = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
    ##print Dumper(\%dbconf);
    return \%dbconf;

}

sub send_sftp
{
    my $hostname  = shift;
    my $login     = shift;
    my $pass      = shift;
    my $remotedir = shift;
    my $fileRef   = shift;
    my @files     = @{$fileRef} if $fileRef;

    log_write( "**********SFTP starting -> $hostname with $login and $pass -> $remotedir", 1 );
    my $sftp = Net::SFTP->new(
        $hostname,
        debug    => 0,
        user     => $login,
        password => $pass
    ) or return "Cannot connect to " . $hostname;

    foreach my $file (@files)
    {
        my $dest = $remotedir . "/" . getBareFilename($file);
        log_write( "Sending file $file -> $dest", 1 );
        $sftp->put( $file, $dest )
          or return "Sending file $file failed";
    }
    log_write( "**********SFTP session closed ***************", 1 );
    return 0;
}

sub email_setup
{
    my ( $from, $emailRecipientArrayRef, $errorFlag, $successFlag ) = @_;

    my $email =
    {
        fromEmailAddress    => $from,
        emailRecipientArray => $emailRecipientArrayRef,
        notifyError         => $errorFlag,                #true/false
        notifySuccess       => $successFlag,              #true/false
    };

    return email_setupFinalToList($email);
}

sub email_send
{
    my ( $email, $subject, $body ) = @_;

    my $message = Email::MIME->create(
        header_str => [
            From    => $email->{fromEmailAddress},
            To      => [ @{ $email->{finalToEmailList} } ],
            Subject => $subject
        ],
        attributes => {
            encoding => 'quoted-printable',
            charset  => 'ISO-8859-1',
        },
        body_str => "$body\n"
    );

    use Email::Sender::Simple qw(sendmail);

    email_reportSummary( $email, $subject, $body );

    sendmail($message);

    print "Sent\n" if $debug;
}

sub email_reportSummary
{
    my ( $email, $subject, $body, $attachmentRef ) = @_;
    my @attachments = ();
    @attachments = @{$attachmentRef} if ( ref($attachmentRef) eq 'ARRAY' );

    my $characters = length($body);
    my @lines      = split( /\n/, $body );
    my $bodySize   = $characters / 1024 / 1024;

    print "\n";
    print "From: " . $email->{fromEmailAddress} . "\n";
    print "To: ";
    print "$_, " foreach ( @{ $email->{finalToEmailList} } );
    print "\n";
    print "Subject: $subject\n";
    print "== BODY ==\n";
    print "$characters characters\n";
    print scalar(@lines) . " lines\n";
    print $bodySize . "MB\n";
    print "== BODY ==\n";

    my $fileSizeTotal = 0;
    if ( $#attachments > -1 )
    {
        print "== ATTACHMENT SUMMARY == \n";

        foreach (@attachments)
        {
            $fileSizeTotal += -s $_;
            my $thisFileSize = ( -s $_ ) / 1024 / 1024;
            print "$_: ";
            printf( "%.3f", $thisFileSize );
            print "MB\n";

        }
        $fileSizeTotal = $fileSizeTotal / 1024 / 1024;

        print "Total Attachment size: ";
        printf( "%.3f", $fileSizeTotal );
        print "MB\n";
        print "== ATTACHMENT SUMMARY == \n";
    }

    $fileSizeTotal += $bodySize;
    print "!!!WARNING!!! Email (w/attachments) Exceeds Standard 25MB\n"
      if ( $fileSizeTotal > 25 );
    print "\n";

}

sub email_deDupeEmailArray
{
    my $email         = shift;
    my $emailArrayRef = shift;
    my @emailArray    = @{$emailArrayRef};
    my %posTracker    = ();
    my %bareEmails    = ();
    my $pos           = 0;
    my @ret           = ();

    foreach (@emailArray)
    {
        my $thisEmail = $_;

        print "processing: '$thisEmail'\n" if $debug;

        # if the email address is expressed with a display name,
        # strip it to just the email address
        $thisEmail =~ s/^[^<]*<([^>]*)>$/$1/g if ( $thisEmail =~ m/</ );

        # lowercase it
        $thisEmail = lc $thisEmail;

        # Trim the spaces
        $thisEmail = trim($thisEmail);

        print "normalized: '$thisEmail'\n" if $debug;

        $bareEmails{$thisEmail} = 1;
        if ( !$posTracker{$thisEmail} )
        {
            my @a = ();
            $posTracker{$thisEmail} = \@a;
            print "adding: '$thisEmail'\n" if $debug;
        }
        else
        {
            print "deduped: '$thisEmail'\n" if $debug;
        }
        push( @{ $posTracker{$thisEmail} }, $pos );
        $pos++;
    }
    while ( ( my $email, my $value ) = each(%bareEmails) )
    {
        my @a = @{ $posTracker{$email} };

        # just take the first occurance of the duplicate email
        push( @ret, @emailArray[ @a[0] ] );
    }

    return \@ret;
}

sub email_setupFinalToList
{
    my $email = shift;
    my @ret   = ();

    my @varMap = ( "successemaillist", "erroremaillist" );

    foreach (@varMap) {
        my @emailList = split( /,/, $conf{$_} );
        for my $y ( 0 .. $#emailList ) {
            @emailList[$y] = trim( @emailList[$y] );
        }
        $email->{$_} = \@emailList;
        print "$_:\n" . Dumper( \@emailList ) if $debug;
    }

    undef @varMap;

    push( @ret, @{ $email->{emailRecipientArray} } )
      if ( $email->{emailRecipientArray}->[0] );

    push( @ret, @{ $email->{successemaillist} } )
      if ( $email->{'notifySuccess'} );

    push( @ret, @{ $email->{erroremaillist} } ) if ( $email->{'notifyError'} );

    print "pre dedupe:\n" . Dumper( \@ret ) if $debug;

    # Dedupe
    @ret = @{ email_deDupeEmailArray( $email, \@ret ) };

    print "post dedupe:\n" . Dumper( \@ret ) if $debug;

    $email->{finalToEmailList} = \@ret;

    return $email;
}

sub getBareFilename
{
    my $fullFile = shift;
    my @s        = split( /\//, $fullFile );
    return pop @s;
}

sub readFile
{
    my $file   = shift;
    my $trys   = 0;
    my $failed = 0;
    my @lines;

    #print "Attempting open\n";
    if ( -e $file ) {
        my $worked = open( inputfile, '< ' . $file );
        if ( !$worked ) {
            print "******************Failed to read file*************\n";
        }
        binmode( inputfile, ":utf8" );
        while ( !( open( inputfile, '< ' . $file ) ) && $trys < 100 ) {
            print "Trying again attempt $trys\n";
            $trys++;
            sleep(1);
        }
        if ( $trys < 100 ) {

            #print "Finally worked... now reading\n";
            @lines = <inputfile>;
            close(inputfile);
        }
        else {
            print "Attempted $trys times. COULD NOT READ FILE: $file\n";
        }
        close(inputfile);
    }
    else {
        print "File does not exist: $file\n";
    }
    return \@lines;
}

sub trim
{
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}

sub chooseNewFileName
{
    my $path = shift;
    my $seed = shift;
    my $ext  = shift;
    my $ret  = "";

    $path = $path . '/' if ( substr( $path, length($path) - 1, 1 ) ne '/' );

    if ( -d $path )
    {
        my $num = "";
        $ret = $path . $seed . $num . '.' . $ext;
        while ( -e $ret )
        {
            if ( $num eq "" )
            {
                $num = -1;
            }
            $num++;
            $ret = $path . $seed . $num . '.' . $ext;
        }
    }
    else
    {
        $ret = 0;
    }

    return $ret;
}

sub fillColMaps
{
    my $i = 0;
    foreach(@ogColMapOrder)
    {
        $importFileColMap{$i} = $_;
        $i++;
    }

    foreach(@correctedColMapOrder)
    {
        $importFileColMap{$i} = $_;
        $i++;
    }
    undef $i;
    
    $baseArchiveFolder = $conf{"archive"};
    $baseArchiveFolder =~ s/\/*$//;
    $baseArchiveFolder .= '/';
}

sub getOrgUnits
{
    my $libnames = lc( @_[0] );
    my @ret      = ();

    # spaces don't belong here
    $libnames =~ s/\s//g;

    my @sp = split( /,/, $libnames );

    my $libs = join( '$$,$$', @sp );
    $libs = '$$' . $libs . '$$';

    my $query = "
    select id
    from
    actor.org_unit
    where lower(shortname) in ($libs)
    order by 1";
    log_write($query) if $debug;
    my @results = @{ dbhandler_query($query) };
    foreach (@results)
    {
        my @row = @{$_};
        push( @ret, @row[0] );
        if ( $conf{"include_org_descendants"} )
        {
            my @des = @{ getOrgDescendants( @row[0] ) };
            push( @ret, @des );
        }
    }
    return dedupeArray( \@ret );
}

sub dedupeArray
{
    my $arrRef  = shift;
    my @arr     = $arrRef ? @{$arrRef} : ();
    my %deduper = ();
    $deduper{$_} = 1 foreach (@arr);
    my @ret = ();
    while ( ( my $key, my $val ) = each(%deduper) )
    {
        push( @ret, $key );
    }
    @ret = sort @ret;
    return \@ret;
}

sub getOrgDescendants
{
    my $thisOrg = shift;
    my $query   = "select id from actor.org_unit_descendants($thisOrg)";
    my @ret     = ();
    log_write($query) if $debug;

    my @results = @{ dbhandler_query($query) };
    foreach (@results)
    {
        my @row = @{$_};
        push( @ret, @row[0] );
    }

    return \@ret;
}


sub startFile
{
    my $filename = shift;
    my $handle;
    open( $handle, '> ' . $filename );
    binmode( $log, ":utf8" );
    return $handle;
}

exit;
