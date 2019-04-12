#!/usr/bin/perl -w

# PMS_ImportPMSData.pm 
# This module contains support the PMS RSIDN database, the PMS Teams, and the Merged Members

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMS_ImportPMSData;

use diagnostics;
use strict;
use Spreadsheet::Read;
use Text::CSV_XS;
use File::Basename;
#use lib 'PMSPerlModules';
use PMS_MySqlSupport;
use PMSConstants;
use PMSLogging;
require PMSUtil;

my $debug = 1;

# Forward declaration....
sub ReadPMS_RSIDNData( $$ );
sub GetRSINDRow( $$$$$ );
sub GetPMSTeams( $ );
sub RSINDFileIsNew( $$ );

# we allow 2 digit years but complain about it.  The generator of these data should know better!
my $foundIllegalBirthdate = 0;		# set to 1 if an illegal birthdate (year only 2 digits) supplied

####====================================================================================================================================
####====================================================================================================================================

# ReadPMS_RSIDNData( $filename, $yearBeingProcessed )
#
# PASSED:
#   $filename - full path name of file containing the data, an Excel file (.csv, .xls, or .xlsx) in this format:
#			ClubAbbr	SwimmerID	FirstName	MI	LastName	Address1	City	StateAbbr	Zip	Country	BirthDate	Sex	RegDate	EMailAddress	RegNumber
#		We will only read the first sheet of the workbook.
#	$yearBeingProcessed - in the form '2016'
#
# Logging must be enabled.
# 
#
sub ReadPMS_RSIDNData( $$ ) {
	my $filename = $_[0];
	my $yearBeingProcessed = $_[1];
	my $tableName = "RSIDN_$yearBeingProcessed";
	my( $simpleName, $dirs, $suffix ) = fileparse( $filename );		# get last simple name in filename
	my $numRSIDNSwimmerRows = 0;  # number of swimmers in RSIDN table before any possible update
	my $lastRSIDNFileName = "?"; 	# name of the previous file used to load RSIDN data
	my $query;

    # get some info about this spreadsheet (e.g. # sheets, # rows and columns in first sheet, etc)
    my $g_ref = ReadData( $filename );
    # $g_ref is an array reference
    # $g_ref->[0] is a reference to a hashtable:  the "control hash"
    my $numSheets = $g_ref->[0]{sheets};        # number of sheets, including empty sheets
    print "\nfile $filename:\n  Number of sheets:  $numSheets.\n  Names of non-empty sheets:\n" 
    	if( $debug > 0);
    my $sheetNames_ref = $g_ref->[0]{sheet};  # reference to a hashtable containing names of non-empty sheets.  key = sheet
                                              # name, value = monotonically increasing integer starting at 1 
    my %tmp = % { $sheetNames_ref } ;         # hashtable of sheet names (above)
    my ($sheetName);
    foreach $sheetName( sort { $tmp{$a} <=> $tmp{$b} } keys %tmp ) {
        print "    $sheetName\n" if( $debug > 0 );
    }
    
    # get the first sheet
    my $g_sheet1_ref = $g_ref->[1];         # reference to the hashtable representing the sheet
    my $numRowsInSpreadsheet = $g_sheet1_ref->{maxrow};	# number of rows in RSIDN file
    my $numColumnsInSpreadsheet = $g_sheet1_ref->{maxcol};
    print "numRows=$numRowsInSpreadsheet, numCols=$numColumnsInSpreadsheet\n" if( $debug > 0 );

	# do we already have this data in the database?
	PMSLogging::PrintLogNoNL( "", "", "->Get pms ($tableName) data?...", 1 );
    my $dbh = PMS_MySqlSupport::GetMySqlHandle();
    my $refreshRSIDNFile = 0;		# set to 1 if we need to read the RSIDN file
    $query = "SELECT COUNT(*) as Count FROM $tableName";
    ( my $sth, my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
	if( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		$numRSIDNSwimmerRows = $resultHash->{'Count'};		# number of swimmers in RSIDN table
	} else {
		die "Error returned by fetchrow_hashref after SELECT COUNT(*) FROM $tableName";
	}
	if( $numRSIDNSwimmerRows > 0 ) {
		# see if the passed RSIND file name is different from the one we last used to populate our
		# RSIDN table:
		($refreshRSIDNFile, $lastRSIDNFileName) = RSINDFileIsNew( $simpleName, $yearBeingProcessed);
		# 1 means that it's different, 0 means that it is not
	} else {
		# we have no RSIDN data - read the RSIDN file
		print "  (We found no data in the $tableName table)\n" if( $debug > 0 );
		$refreshRSIDNFile = 1;
	}
	
	if( $refreshRSIDNFile ) {
		# We've decided to read the spreadsheet because our RSIDN table is either empty or 
		# out of date - DROP it and then populate it.
		# First, one simple check...
		my $numSwimmerRowsInSpreadsheet = $numRowsInSpreadsheet-1;		# ignore header row
		if( ($numRSIDNSwimmerRows > 0) && ($numRSIDNSwimmerRows > $numSwimmerRowsInSpreadsheet) ) {
			# Hmmm - this is interesting.  The spreadsheet is SMALLER than the last one we processed
			# with this RSIDN data.  This isn't a good sign, but we'll just print a warning and
			# go on:
			PMSLogging::DumpWarning( "", "", "PMS_ImportPMSData::ReadPMS_RSIDNData(): The RSIDN file " .
				"that we're about to read contains LESS swimmers ($numSwimmerRowsInSpreadsheet)\n" .
				"    than the current RSIDN table ($numRSIDNSwimmerRows).    This is a WARNING only, " .
				"but it looks like the spreadsheet\n    ($filename) might be truncated.", 1 );
		}
		( my $sth, my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "TRUNCATE TABLE $tableName" );
		
		PMSLogging::PrintLog( "", "", "Yes! reading '$filename'...", 1 );
		# next, clear the RSIDN data in our meta data so if we don't complete the reading of this RSIDN file
		# we will force the read of the RSIDN file the next time we run:
		$query = "UPDATE Meta SET RSIDNFileName = '(No RSIDN File)' WHERE Year='$yearBeingProcessed'";
		my $rowsAffected = $dbh->do( $query );
		if( $rowsAffected == 0 ) {
			# update failed - must not be any rows for this year to update.  INSERT instead
			print "(Pre-read): UPDATE of Meta failed (query='$query') - try INSERT instead.\n" if( $debug > 0 );
			$query = "INSERT INTO Meta (RSIDNFileName,Year) VALUE ('(No RSIDN File)','$yearBeingProcessed')";
			$rowsAffected = $dbh->do( $query );
			if( $rowsAffected == 0 ) {
				# oops - Update failed
				PMSLogging::DumpError( 0, 0, "PMS_ImportPMSData.pm::ReadPMS_RSIDNData(): " .
					"(Pre-read): Unable to perform this INSERT: '$query'", 1 );
			} else {
				print "(Pre-read): Insert succeeded:  '$query'\n" if( $debug > 0 );
			}
		} else {
			print "(Pre-read): Update of $rowsAffected rows succeeded:  '$query'\n" if( $debug > 0 );
		}
		
		# Next, confirm the order of the columns:
		if( my $msg = InvalidRSIND( $g_sheet1_ref ) ) {
			PMSLogging::DumpError( 0, 0, "PMS_ImportPMSData.pm::ReadPMS_RSIDNData(): " .
				"Invalid RSIND file: $msg", 1 );
			die( "Failed to read the new RSIND file - ABORT!" );
		}
		
		
	    # Finally, pass through the sheet collecting initial data on all swimmers:
	    # (skip first row because we assume it has row titles)
	    my $rowNum;
	    my $rowRef = {};
	    for( $rowNum = 2; $rowNum <= $numRowsInSpreadsheet; $rowNum++ ) {
	    	if( ($rowNum % 1000) == 0 ) {
	    		print "...working on row $rowNum...\n";
	    	}
    		
	    	GetRSINDRow( $rowRef, $rowNum, $g_sheet1_ref, $yearBeingProcessed, $filename );
	
    		# now, get to work...
    		($sth,  $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
    			"INSERT INTO $tableName " .
    			"(FirstName, MiddleInitial, LastName, RegNum, USMSSwimmerId, " .
    			"RegisteredTeamInitialsStr, Gender, DateOfBirth, " .
    			"RegDate, Email, Address1, City, State, Zip, Country) " .
    			"VALUES (\"$rowRef->{'first'}\", \"$rowRef->{'middle'}\", \"$rowRef->{'last'}\", " .
    			"\"$rowRef->{'reg'}\", \"$rowRef->{'swimmerId'}\", " .
    			"\"$rowRef->{'club'}\", \"$rowRef->{'gender'}\", \"$rowRef->{'dob'}\", " .
    			"\"$rowRef->{'regDate'}\", \"$rowRef->{'email'}\", \"$rowRef->{'address1'}\", " .
    			"\"$rowRef->{'city'}\", \"$rowRef->{'state'}\", " .
    			"\"$rowRef->{'zip'}\", \"$rowRef->{'country'}\" " .
    			")" );
	    }
	    # we're done - updata our meta data
		$query = "UPDATE Meta SET RSIDNFileName = '$simpleName' WHERE Year='$yearBeingProcessed'";
		$rowsAffected = $dbh->do( $query );
		if( $rowsAffected == 0 ) {
			# update failed - must not be any rows for this year to update.  INSERT instead
			print "UPDATE of Meta failed (query='$query') - try INSERT instead.\n" if( $debug > 0 );
			$query = "INSERT INTO Meta (RSIDNFileName,Year) VALUE ('$simpleName','$yearBeingProcessed')";
			$rowsAffected = $dbh->do( $query );
			if( $rowsAffected == 0 ) {
				# oops - Update failed
				PMSLogging::DumpError( 0, 0, "PMS_ImportPMSData.pm::ReadPMS_RSIDNData(): " .
					"Unable to perform this INSERT: '$query'", 1 );
			} else {
				print "Insert succeeded:  '$query'\n" if( $debug > 0 );
			}
		} else {
			print "Update of $rowsAffected rows succeeded:  '$query'\n" if( $debug > 0 );
		}
		$rowNum -= 2;		# this is the real number of rows we read from the new RSIND file (not counting header row)
		PMSLogging::PrintLog( "", "", "->Done reading $rowNum rows from our RSIDN file (replacing " .
			"$numRSIDNSwimmerRows rows from previous RSIDN file.)", 1 );
	} else {
		PMSLogging::PrintLog( "", "", "NO (we already have the data from: '$lastRSIDNFileName'; $numRSIDNSwimmerRows swimmers.)", 1 );
	}
    
} # end of ReadPMS_RSIDNData()



#	    	GetRSINDRow( $rowRef, $rowNum, $g_sheet1_ref, $yearBeingProcessed, $filename );
# GetRSINDRow -  get a row from the spreadsheet and validate each field.
#
# PASSED:
#	$rowRef - reference to a hash into which the fields of the spreadsheet are stored
#	$rowNum - the row number in the spreadsheet
#	$g_sheet1_ref - reference to the spreadsheet
#	$yearBeingProcessed
#
# RETURNED:
#	n/a
#
# NOTES:
#	Any problems discovered with the data will result in a log message.
#
sub GetRSINDRow( $$$$$ ) {
	my ($rowRef, $rowNum, $g_sheet1_ref, $yearBeingProcessed, $filename) = @_;
	# empty our hash of data:
	for (keys %$rowRef) {
		delete $rowRef->{$_};
	}
	# extract data from the spreadsheet:
	$rowRef->{'club'} = $g_sheet1_ref->{"A$rowNum"};
	if( ! defined $rowRef->{'club'} ) {
			PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined club.", 1 );
	} else {
		$rowRef->{'club'} = uc($rowRef->{'club'});
	}	
	$rowRef->{'swimmerId'} = PMSUtil::GenerateCanonicalUSMSSwimmerId( $g_sheet1_ref->{"B$rowNum"} );
	$rowRef->{'first'} = $g_sheet1_ref->{"C$rowNum"};
	$rowRef->{'middle'} = $g_sheet1_ref->{"D$rowNum"};
	if( !defined( $rowRef->{'middle'} ) ) {
		$rowRef->{'middle'} = "";
	}
	$rowRef->{'last'} = $g_sheet1_ref->{"E$rowNum"};
	$rowRef->{'address1'} = $g_sheet1_ref->{"F$rowNum"};
	$rowRef->{'city'} = $g_sheet1_ref->{"G$rowNum"};
	$rowRef->{'state'} = $g_sheet1_ref->{"H$rowNum"};
	$rowRef->{'zip'} = $g_sheet1_ref->{"I$rowNum"};
	$rowRef->{'country'} = $g_sheet1_ref->{"J$rowNum"};
	$rowRef->{'dob'} = $g_sheet1_ref->{"K$rowNum"};
	$rowRef->{'gender'} = $g_sheet1_ref->{"L$rowNum"};
	$rowRef->{'regDate'} = $g_sheet1_ref->{"M$rowNum"};
	$rowRef->{'email'} = $g_sheet1_ref->{"N$rowNum"};
	$rowRef->{'reg'} = $g_sheet1_ref->{"O$rowNum"};
	if( ! defined $rowRef->{'reg'} ) {
			PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined reg.", 1 );
	} else {
		$rowRef->{'reg'} = uc($rowRef->{'reg'});
	}	
	
	# NOW for validation and correction:
	if( ($rowRef->{'club'} eq "") || (length( $rowRef->{'club'} ) > 10) ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid club.", 1 );
	}
	if( ! defined $rowRef->{'swimmerId'} ) {
			PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined swimmerId.", 1 );
	}	
	if( ! defined $rowRef->{'first'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined first name.", 1 );
	} elsif( ($rowRef->{'first'} eq "") ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid (empty) first name.", 1 );
	}
	if( ! defined $rowRef->{'last'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined last name.", 1 );
	} elsif( ($rowRef->{'last'} eq "") ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid (empty) last name.", 1 );
	}
	if( ! defined $rowRef->{'address1'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined address.", 1 );
	} elsif( ($rowRef->{'address1'} eq "") ) {
		PMSLogging::DumpWarning( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid (empty) address.", 1 );
	}
	if( ! defined $rowRef->{'city'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined city.", 1 );
	} elsif( ($rowRef->{'city'} eq "") ) {
		PMSLogging::DumpWarning( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid (empty) city.", 1 );
	}
	if( ! defined $rowRef->{'state'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined state.", 1 );
	} elsif( ($rowRef->{'state'} eq "") ) {
		PMSLogging::DumpWarning( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid (empty) state.", 1 );
	}
	if( ! defined $rowRef->{'zip'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined zip.", 1 );
	} elsif( ($rowRef->{'zip'} eq "") ) {
		PMSLogging::DumpWarning( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid (empty) zip.", 1 );
	}
	if( ! defined $rowRef->{'country'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined country.", 1 );
	} elsif( ($rowRef->{'country'} eq "") ) {
		PMSLogging::DumpWarning( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid (empty) country.", 1 );
	}
	if( ! defined $rowRef->{'email'} ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): undefined email.", 1 );
	} elsif( $rowRef->{'email'} !~ m/^[^@]+@.+\..+/ ) {
		# an email adderss is not really required but we'll check to make sure it's sane anyway...
		PMSLogging::DumpWarning( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid email " .
		"address (" .  $rowRef->{'email'} . ")" );
	}
	
	# full reg number is in the spreadsheet
	$rowRef->{'reg'} = PMSUtil::GenerateCanonicalRegNum($rowRef->{'reg'}); 
	# if the reg number is missing (older RSIDN files sometimes has a missing number) then construct it
	# the best we can
	if( $rowRef->{'reg'} eq $PMSConstants::INVALID_REGNUM ) {
		my $yearDigit = $yearBeingProcessed;
		$yearDigit =~ s/^...//;
		$rowRef->{'reg'} = "38${yearDigit}x-" . $rowRef->{'swimmerId'};		# e.g. 387x-12345
	}
	
	# sanity check:  $swimmerId must be last part of reg number.  If the swimmerId isn't
	# consistent with the regnum then we have a choice:  ignore one of them and construct
	# it from the other.
	my $temp = $rowRef->{'reg'};
	$temp =~ s/^.*-//;
	
	if( $temp ne $rowRef->{'swimmerId'} ) {
		# WARNING:  the supplied regnum and supplied swimmerid are not consistent!
		if(0) {
			# execute this code if we want to ignore the supplied regnum and instead construct it from the
			# supplied swimmerId
			my $newReg = $rowRef->{'reg'};
			$newReg =~ s/-.*$/-$rowRef->{'swimmerId'}/;
			PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): swimmerId (" .
				$rowRef->{'swimmerId'} . ") " .
				"isn't consistent with their reg number (" . $rowRef->{'reg'} . ").\n" .
				"  Changing reg number to $newReg.  last=$rowRef->{'last'}, " .
				"first=$rowRef->{'first'}, club=$rowRef->{'club'}");
			$rowRef->{'reg'} = $newReg;
		} else {
			# execute this code if we want to ignore the supplied swimmerId and instead construct it from the
			# supplied regnum
			my $newSwimmerId = $rowRef->{'reg'};
			$newSwimmerId =~ s/^.*-//;
			PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): swimmerId " .
				($rowRef->{'swimmerId'}) .
				" isn't consistent with their reg number ($rowRef->{'reg'}).\n" .
				"  Changing swimmerId to $newSwimmerId.  last=$rowRef->{'last'}, " .
				"first=$rowRef->{'first'}, club=$rowRef->{'club'}");
			$rowRef->{'swimmerId'} = $newSwimmerId;
		}
	}
	
	# validate and correct (if necessary) the swimmerId and regnum
	$rowRef->{'swimmerId'} = PMSUtil::ValidateAndCorrectSwimmerId( $rowRef->{'swimmerId'},
		"PMS_ImportPMSData::GetRSINDRow()", $yearBeingProcessed );
	$rowRef->{'reg'} = PMSUtil::ValidateAndCorrectSwimmerId( $rowRef->{'reg'}, 
		"PMS_ImportPMSData::GetRSINDRow()", $yearBeingProcessed );
	
	# convert the $gender into our own canonical form:
	$rowRef->{'gender'} = PMSUtil::GenerateCanonicalGender( $filename, $rowNum, $rowRef->{'gender'} );
print "gender is now " . $rowRef->{'gender'} . "\n";
	if( $rowRef->{'gender'} eq "?" ) {
		PMSLogging::DumpError( "", $rowNum, "PMS_ImportPMSData::GetRSINDRow(): Invalid gender." );
	}
	
	# convert birthdate into mysql format
	#  mm/dd/yyyy -> yyyy-mm-dd
	$rowRef->{'dob'} = PMSUtil::GenerateCanonicalDOB($rowRef->{'dob'});
	# same for regdate
	$rowRef->{'regDate'} = PMSUtil::GenerateCanonicalDOB($rowRef->{'regDate'});
	my $year = $rowRef->{'dob'};
	my $month = $rowRef->{'dob'};
	my $day = $rowRef->{'dob'};
	$month =~ s,[-/].*$,,;
	$day =~ s,^[^/-]+[-/],,;
	$day =~ s,[-/].*$,,;
	$year =~ s,^.*[-/],,;
	# handle 2 digit years:
	if( $year < 100 ) {
		$year += 1900;
		if( ! $foundIllegalBirthdate ) {
			# we need to display an error, but only once
			$foundIllegalBirthdate++;
			my $errStr = "Bad birthdate found in RSIDN file in row # " .
				"$rowNum: dob='$rowRef->{'dob'}'.  Should be a 4 digit year; we'll assume 1900+";
			PMSLogging::DumpWarning( "", "", $errStr );
			print "\nPMS_ImportPMSData::GetRSINDRow(): $errStr\n";	    				
		}
	}
	$rowRef->{'dob'} = $year . "-" . $month . "-" . $day;
	# be careful of any value containing characters that can confuse the mySql parser
	$rowRef->{'club'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'club'} );
	$rowRef->{'first'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'first'} );
	$rowRef->{'middle'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'middle'} );
	$rowRef->{'last'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'last'} );
	$rowRef->{'address1'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'address1'} );
	$rowRef->{'city'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'city'} );
	$rowRef->{'state'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'state'} );
	$rowRef->{'zip'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'zip'} );
	$rowRef->{'country'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'country'} );
	$rowRef->{'email'} = PMS_MySqlSupport::MySqlEscape( $rowRef->{'email'} );

} # end of GetRSINDRow()
    		


# 		if( my $msg = InvalidRSIND( $g_sheet1_ref ) ) {
# InvalidRSIND - analyze the passed RSIND file to confirm that it looks reasonable
#
# PASSED:
#	$g_sheet1_ref - reference to the hashtable representing the RSIND sheet
#
# RETURNED:
#	$msg - empty string if the RSIND file looks OK, a non-empty error string if it appears invalid
#
sub InvalidRSIND( $ ) {
	my $g_sheet1_ref = $_[0];
	my $msg = "";
	my @columnHeadings = (
		"ClubAbbr",
		"SwimmerID",
		"FirstName",
		"MI",
		"LastName",
		"Address1",
		"City",
		"StateAbbr",
		"Zip",
		"Country",
		"BirthDate",
		"Sex",
		"RegDate",
		"EMailAddress",
		"RegNumber"
	);
	
	for( my $colNum = ord("A"); $colNum <= ord("O"); $colNum++ ) {
		if( $g_sheet1_ref->{chr($colNum)."1"} ne $columnHeadings[$colNum-ord("A")] ) {
			$msg = "'" . chr($colNum) . "' is '" . $g_sheet1_ref->{chr($colNum)."1"} .
				"' but expected '" . $columnHeadings[$colNum-ord("A")] . "'";
		}
	}
	
	return $msg;
		
} # end of InvalidRSIND()


#		$refreshRSIDNFile = RSINDFileIsNew( $simpleName, $yearBeingProcessed);
#
# RSINDFileIsNew - determine whether or not the passed simple name of a RSIND file is different from the
#	simple name of the last RSIND file used to populate our RSIND table.
#
# PASSED:
#	$simpleName -
#	$yearBeingProcessed -
#
# RETURNED:
#	$refreshRSIDNFile - 1 means that the passed RSIND file is different from the last one we used, 0 otherwise.
#	$yearBeingProcessed -
#
sub RSINDFileIsNew( $$ ) {
	my( $simpleName, $yearBeingProcessed ) = @_;
    my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $refreshRSIDNFile = 0;
	my $lastRSIDNFileName;
	
	# we have RSIDN data - is it the data from the requested RSIDN file?
	my $query = "SELECT RSIDNFileName FROM Meta  WHERE Year = '$yearBeingProcessed'";
	(my $sth2, my $rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
	my $arr_ref = $sth2->fetchrow_arrayref();
	$lastRSIDNFileName = $arr_ref->[0];		# default is "(none)" - set when table created
	if( !defined $lastRSIDNFileName ) {
		print "The RSIDNFileName in Meta is undefined, so it didn't match '$simpleName'\n" if( $debug > 0 );
		# last RSIDN file read is different from what we're asked to read
		$refreshRSIDNFile = 1;
	} elsif( $lastRSIDNFileName ne $simpleName ) {
		print "The RSIDNFileName in Meta ($lastRSIDNFileName) didn't match '$simpleName'\n" if( $debug > 0 );
		# last RSIDN file read is different from what we're asked to read
		$refreshRSIDNFile = 1;
	}
	
	return ($refreshRSIDNFile, $lastRSIDNFileName);
	
} # end of RSINDFileIsNew()




####====================================================================================================================================
####====================================================================================================================================
#
# GetPMSTeams - get a list of legal PMS teams
#
# PASSED:
#	$clubDataFile - full path name of file containing the data in the form (tab-separated fields):
#			Club_Abbr	Club_Name	NumTeamMemberts 	Year	Reg._Date
#		The extension of the file tells us the exact format of the file.  One of:
#			.txt - tab separated fields
#			.csv - comma separated fields
#			(non-empty extension) - some kind of excel spreadsheet
#			(no extension) - error
#
sub GetPMSTeams( $ ) {
	my $clubDataFile = $_[0];
	my( $simpleName, $dirs, $suffix ) = fileparse( $clubDataFile );		# get last simple name in filename
	my $numTeamRows = 0;  # number of rows in PMSTeams table before any possible update

	# do we already have this data in the database?
	PMSLogging::PrintLogNoNL( "", "", "->Get pms team names?...", 1 );
    my $dbh = PMS_MySqlSupport::GetMySqlHandle();
    my $refreshTeamsFile = 0;		# set to 1 if we need to read the RSIDN file
    (my $sth, my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "SELECT COUNT(*) as Count FROM PMSTeams" );
	my $arr_ref = $sth->fetchrow_arrayref();
	if( !defined($arr_ref) ) {
		die "Error returned by fetch_arrayref() after SELECT COUNT(*) FROM PMSTeams";
	} else {
		$numTeamRows = $arr_ref->[0];
		if( $numTeamRows > 0 ) {
			# we have team data - is it the data from the requested Team file?
    		(my $sth2, my $rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "SELECT TeamsFileName FROM Meta" );
    		$arr_ref = $sth2->fetchrow_arrayref();
    		my $lastTeamsFileName = $arr_ref->[0];
    		if( !defined $lastTeamsFileName ) {
				print "The TeamsFileName in Meta is undefined, so it didn't match '$simpleName'\n" if( $debug > 0 );
    			# last Teams file read is different from what we're asked to read
    			$refreshTeamsFile = 1;
    		} elsif( $lastTeamsFileName ne $simpleName ) {
				print "The TeamsFileName in Meta ($lastTeamsFileName) didn't match '$simpleName'\n" if( $debug > 0 );
    			# last Teams file read is different from what we're asked to read
    			$refreshTeamsFile = 1;
    		}
		} else {
			# we have no Teams data - read the Teams file
			print "We found no data in the PMSTeams table\n" if( $debug > 0 );
    		$refreshTeamsFile = 1;
		}
	}
	
	if( $refreshTeamsFile ) {
		# our Teams table is either empty or out of date - DROP it and then populate it
		( my $sth, my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "TRUNCATE TABLE PMSTeams" );
		
		# empty table - populate it
		PMSLogging::PrintLog( "", "", "Yes! reading '$clubDataFile'...", 1 );

	    # what kind of file is this?  Use the file extension to tell us:
	    my $ext = $clubDataFile;
	    $ext =~ s/^.*\.//;
	    $ext = lc( $ext );
		my $numTeams = 0;
	    if( ! $ext ) {
	    	# no extension?  give up
	    	die "PMS_ImportPMSData::GetPMSTeams(): Missing file extension ($clubDataFile)."
	    } elsif( $ext eq "txt" ) {
	    	$numTeams = GetPMSTeams_txt( $clubDataFile );
	    } elsif( $ext eq "csv" ) {
	    	GetPMSTeams_csv( $clubDataFile );
	    } else {
	    	# assume a spreadsheet
	    	GetPMSTeams_xls( $clubDataFile );
	    }

	    # we're done - updata our meta data
		my $query = "UPDATE Meta SET TeamsFileName = '$simpleName'";
		my $rowsAffected = $dbh->do( $query );
		if( $rowsAffected == 0 ) {
			# update failed - must not be any rows to update.  INSERT instead
			$query = "INSERT INTO Meta (TeamsFileName) VALUE ('$simpleName')";
			$rowsAffected = $dbh->do( $query );
			if( $rowsAffected == 0 ) {
				# oops - Update failed
				PMSLogging::DumpError( 0, 0, "PMS_ImportPMSData.pm::GetPMSTeams(): " .
					"Unable to perform this INSERT: '$query'", 1 );
			} else {
				print "Insert succeeded:  '$query'\n" if( $debug > 0 );
			}
		} else {
			print "Update succeeded:  '$query'\n" if( $debug > 0 );
		}

	    print( "->Done reading $numTeams rows from our TeamsFileName file.\n" );
	} else {
		PMSLogging::PrintLog( "", "", "NO (we already have the data from: '$arr_ref->[0]'; $numTeamRows rows.)", 1 );
	}
} # end of GetPMSTeams()

sub GetPMSTeams_xls( $ ) {
	die "PMS_ImportPMSData::GetPMSTeams_xls(): Not implemented.";
} # end of GetPMSTeams_xls()


sub GetPMSTeams_csv( $ ) {
	my $seperator = ",";
	die "PMS_ImportPMSData::GetPMSTeams_csv(): Not implemented.";
} # end of GetPMSTeams_csv()


# format:
# Club Abbr	\t	Club Name	\t	#	\t	Year	\t	Reg. Date
sub GetPMSTeams_txt( $ ) {
	my $clubDataFile = $_[0];
	my $seperator = "\t";
	my $lineNum = 0;
	my $numTeams = 0;
	local $/ = "\r";
	open( PROPERTYFILE, "< $clubDataFile" ) || die( "PMS_ImportPMSData::GetPMSTeams_txt():  Can't open $clubDataFile: $!" );
	while( my $line = <PROPERTYFILE> ) {
		my $value = "";
		$lineNum++;
		chomp( $line );
		$line =~ s/\s*#.*$//;
		next if( $line eq "" );
		# split on tabs
		my @fields = split( "\t", $line );
		my ($abbr, $fullName) = @fields;
		if( ($fullName eq $line) || ($abbr eq $line) ) {
			PMSLogging::printLog( "PMS_ImportPMSData::GetPMSTeams_txt():  !!! ERROR: Illegal PMS team (missing tab on line $lineNum): '$line'\n");
			next;
		}
		if( ($abbr eq "") || ($fullName eq "") ) {
			PMSLogging::printLog( "PMS_ImportPMSData::GetPMSTeams_txt():  !!! ERROR: Illegal PMS team (missing abbr on line $lineNum): '$line'\n");
			next;
		}
		$abbr = uc( $abbr);
		# look for heading line
		next if( $abbr eq "CLUB ABBR");
		$numTeams++;
		#print "team #$numTeams:  '$abbr' = '$fullName' [line='$line']\n";
		
		# be careful of any value containing characters that can confuse the mySql parser
		$abbr = PMS_MySqlSupport::MySqlEscape( $abbr );
		$fullName = PMS_MySqlSupport::MySqlEscape( $fullName );
		# now, get to work...
		my $dbh = PMS_MySqlSupport::GetMySqlHandle();
		# IGNORE errors because this team may already be in the table.  If it is the insert
		# won't do anything.
		(my $sth,my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, 
			"INSERT INTO PMSTeams (TeamAbbr, FullTeamName) " .
			"VALUES (\"$abbr\", \"$fullName\")" );
    } # end of while( my $line = <PROPERTYFILE....
		
	# add 'unat' to our list
	my $abbr = "UC38";
	my $fullName = "Unattached";
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	(my $sth,my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "INSERT IGNORE INTO PMSTeams " .
		"(TeamAbbr, FullTeamName) " .
		"VALUES (\"$abbr\", \"$fullName\")" );
	
	# add "non-PMS team" to our list
	$abbr = "nonpms";
	$fullName = "Non-PMS Team";
	$dbh = PMS_MySqlSupport::GetMySqlHandle();
	($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,  "INSERT IGNORE INTO PMSTeams " .
		"(TeamAbbr, FullTeamName) " .
		"VALUES (\"$abbr\", \"$fullName\")" );

	return $numTeams;
} # end of GetPMSTeams




# GetMergedMembers - get our list of PMS swimmers who have changed their USMS swimmerid's (usually by
#	purchasing a vanity ID, but there are other ways, too.)
#
# This routine will read the passed CSV file which contains lines of the form:
# 	Old Perm. ID,New Perm. ID,Member Name,Club Name,Birth Date
# 	000CU,00C1P,Eller  Patti,,1953-05-04
# 	000R5,03G4T,Sheeper  Lisa M,Menlo Masters Aka Team Sheeper,1964-05-10
# where the columns represent:
#	Old Perm. ID - a previous USMS swimmer id for a specific swimmer
#	New Perm. ID - their new USMS swimmer id
#	Member Name - the swimmer's name
#	Club Name - the swimmer's team
#	Birth Date - the swimmer's birthdate
#
# The use of this file is similar to using the property ">regnum" or ">regnumName" except instead of
# the data being generated by this program and then put into the property file it is supplied by 
# a USMS report named "View Merged Members" (as of July, 2016).  In addition we are using swimmer id's,
# not full reg nums.
#
# PASSED:
#   $filename - full path name of file containing the data, an Excel file (.csv, .xls, or .xlsx) in this format:
#			ClubAbbr	SwimmerID	FirstName	MI	LastName	BirthDate	Sex	RegDate	EMailAddress	RegNumber
#		We will only read the first sheet of the workbook.
#	$yearBeingProcessed - in the form '2016'
#
sub GetMergedMembers($$) {
	my $filename = $_[0];
	my $yearBeingProcessed = $_[1];
	my( $simpleName, $dirs, $suffix ) = fileparse( $filename );		# get last simple name in filename
	my $numMMRows;
	my $unknownUSMSSwimmerIds = 0;
	my $unmatchedUSMSSwimmerIds = 0;
	
	# do we already have this data in the database?
	PMSLogging::PrintLogNoNL( "", "", "->Get USMS Merged Member data?...", 1 );
    my $dbh = PMS_MySqlSupport::GetMySqlHandle();
    my $refreshMMFile = 0;		# set to 1 if we need to read the MergedMember file
    ( my $sth, my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "SELECT COUNT(*) as Count FROM MergedMembers" );
	my $arr_ref = $sth->fetchrow_arrayref();
	if( !defined($arr_ref) ) {
		die "Error returned by fetch_arrayref() after SELECT COUNT(*) FROM MergedMembers";
	} else {
		$numMMRows = $arr_ref->[0];
		if( $numMMRows > 0 ) {
			# we have Merged Member data - is it the data from the requested Merged Member file?
    		(my $sth2, my $rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "SELECT MergedMemberFileName FROM Meta" );
    		$arr_ref = $sth2->fetchrow_arrayref();
    		my $lastMMFileName = $arr_ref->[0];
    		if( !defined $lastMMFileName ) {
				print "The MergedMemberFileName in Meta is undefined, so it didn't match '$simpleName'\n" if( $debug > 0 );
    			# last Merged Member file read is different from what we're asked to read
    			$refreshMMFile = 1;
    		} elsif( $lastMMFileName ne $simpleName ) {
				print "The MergedMemberFileName in Meta ($lastMMFileName) didn't match '$simpleName'\n" if( $debug > 0 );
    			# last RSIDN file read is different from what we're asked to read
    			$refreshMMFile = 1;
    		}
		} else {
			# we have no Merged Member data - read the Merged Member file
			print "We found no data in the MergedMembers table\n" if( $debug > 0 );
    		$refreshMMFile = 1;
		}
	}
	
	if( $refreshMMFile ) {
		# our MergedMembers table is either empty or out of date - DROP it and then populate it
		( my $sth, my $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, "TRUNCATE TABLE MergedMembers" );
		
#		my $foundIllegalBirthdate = 0;		# set to 1 if an illegal birthdate (year only 2 digits) supplied
		PMSLogging::PrintLog( "", "", "Yes! reading '$filename'...", 1 );
	    # read the spreadsheet
	    my $g_ref = ReadData( $filename );
	    # $g_ref is an array reference
	    # $g_ref->[0] is a reference to a hashtable:  the "control hash"
	    my $numSheets = $g_ref->[0]{sheets};        # number of sheets, including empty sheets
	    print "\nfile $filename:\n  Number of sheets:  $numSheets.\n  Names of non-empty sheets:\n" if( $debug > 0);
	    
	    my $sheetNames_ref = $g_ref->[0]{sheet};  # reference to a hashtable containing names of non-empty sheets.  key = sheet
	                                              # name, value = monotonically increasing integer starting at 1 
	    my %tmp = % { $sheetNames_ref } ;         # hashtable of sheet names (above)
	    my ($sheetName);
	    foreach $sheetName( sort { $tmp{$a} <=> $tmp{$b} } keys %tmp ) {
	        print "    $sheetName\n" if( $debug > 0 );
	    }
	    
	    # get the first sheet
	    my $g_sheet1_ref = $g_ref->[1];         # reference to the hashtable representing the sheet
	    my $numRows = $g_sheet1_ref->{maxrow};
	    my $numColumns = $g_sheet1_ref->{maxcol};
	    print "numRows=$numRows, numCols=$numColumns\n" if( $debug > 0 );
	    # pass through the sheet collecting old and new regnum data on all swimmers:
	    # (skip first row because we assume it has row titles)
	    my $rowNum;
	    for( $rowNum = 2; $rowNum <= $numRows; $rowNum++ ) {
	    	if( ($rowNum % 1000) == 0 ) {
	    		print "...working on row $rowNum...\n";
	    	}
	        my $oldUSMSSwimId = $g_sheet1_ref->{"A$rowNum"};
	        my $newUSMSSwimId = $g_sheet1_ref->{"B$rowNum"};
	        
	        my $fullName = $g_sheet1_ref->{"C$rowNum"};
	        
	        #### VVVVVVVVVVVVVVVVVVVVVVVVV  ALL OF THIS BECAUSE THE NAME COLUMN IS BADLY DESIGNED! VVVVVVVVVVVVVVVVVV ####
	        my ($firstName, $middleInitial, $lastName) = ("", "", "");
	        # unfortunately we only get the full name.  We'll do our best to break it into its correct pieces.
			# break the $fullName into first, middle, and last names
			my $USMSSwimmerId = "";		# just in case we can't deduce the swimmer's names
			my @arrOfBrokenNames = PMSUtil::BreakFullNameIntoBrokenNames( $filename, $rowNum, $fullName );
			for( my $nameIndex = 0; $nameIndex < scalar @arrOfBrokenNames; $nameIndex++ ) {
				my $hashRef = $arrOfBrokenNames[$nameIndex];
				# see if this set of first/middle/last names matches a name in our rsind table
				$USMSSwimmerId = PMS_MySqlSupport::GetUSMSSwimmerIdFromName( $filename, $rowNum, 
					$hashRef->{'first'}, $hashRef->{'middle'}, $hashRef->{'last'}, 1 );
				if( $USMSSwimmerId ne "" ) {
					# we've found a PMS name!
					$firstName = $hashRef->{'first'};
					$middleInitial = $hashRef->{'middle'};
					$lastName = $hashRef->{'last'};
					last;
				}
			}
			
			# done trying to convert the $fullName into it's pieces - were we successful?
			if( $USMSSwimmerId eq "" ) {
				PMSLogging::DumpWarning( "", "", "PMS_ImportPMSData::GetMergedMembers(): " .
					"Can't find the swimmer '$fullName' in the RSIDN file; " .
					"\n    MergedMember File: '$filename', line: '$rowNum'" .
					"\n    This is non-fatal, which means we will still consider the USMSSwimmerIds '$oldUSMSSwimId' " .
					"\n    and '$newUSMSSwimId' to belong to the same swimmer.", $debug );
				# we're going to go with this swimmer even tho we didn't find them in the RSIDN file using their
				# name.  We're going to have to guess as to their correct name.
				my $hashRef = $arrOfBrokenNames[0];
				# use this as their PMS name since we have nothing else to go on
				$firstName = $hashRef->{'first'};
				$middleInitial = $hashRef->{'middle'};
				$lastName = $hashRef->{'last'};
				$unknownUSMSSwimmerIds++
			} elsif( ($USMSSwimmerId ne $oldUSMSSwimId) && ($USMSSwimmerId ne $newUSMSSwimId) ) {
				PMSLogging::DumpWarning( "", "", "PMS_ImportPMSData::GetMergedMembers(): " .
					"We found the swimmer '$fullName' in the RSIDN file; " .
					"\n    MergedMember File: '$filename', line: '$rowNum'" .
					"\n    This is non-fatal, but in the RSIDN file we have their USMSSwimmerId as '$USMSSwimmerId', which is not " .
					"\n    consistent with the MergedMember file which has the old id as '$oldUSMSSwimId' " .
					"\n    and the new id as '$newUSMSSwimId'.  We are assuming that the swimmer's name is " .
					"\n    '$firstName' '$middleInitial' '$lastName'", $debug );
				$unmatchedUSMSSwimmerIds++;
			}
	        #### ^^^^^^^^^^^^^^^^^^^^^^^^^^^  ALL OF THE ABOVE BECAUSE THE NAME COLUMN IS BADLY DESIGNED! ^^^^^^^^^^^^^^^^^^^^^^ ####

	        my $fullTeamName = $g_sheet1_ref->{"D$rowNum"};
	        my $dob = $g_sheet1_ref->{"E$rowNum"};
	        
	        # add an entry into our MergedMembers table
    		($sth,  $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
    			"INSERT INTO MergedMembers " .
    			"(FirstName, MiddleInitial, LastName, OldUSMSSwimmerId, NewUSMSSwimmerId, " .
    			"TeamFullName, DateOfBirth) " .
    			"VALUES (\"$firstName\", \"$middleInitial\", \"$lastName\", \"$oldUSMSSwimId\", \"$newUSMSSwimId\", " .
    			"\"$fullTeamName\", \"$dob\")" );
	        #print "old: '$oldUSMSSwimId', new='$newUSMSSwimId', name='$fullName' ('$firstName' '$middleInitial' '$lastName'), team='$fullTeamName', dob='$dob'\n";
	    } # end of for( my $rowNum = 2; $rowNum <= $numRows...
	    
	    # display problems...
	    if( $unknownUSMSSwimmerIds ) {
			PMSLogging::DumpWarning( "", "", "PMS_ImportPMSData::GetMergedMembers(): " .
				"We found $unknownUSMSSwimmerIds instances where a swimmer in the Merged Members file was not found " .
				"in the RSIDN file.", 1 );
	    }
	    if( $unmatchedUSMSSwimmerIds ) {
			PMSLogging::DumpWarning( "", "", "PMS_ImportPMSData::GetMergedMembers(): " .
				"We found $unmatchedUSMSSwimmerIds instances where a swimmer in the Merged Members file was found " .
				"in the RSIDN file with a different USMSSwimmerId.", 1 );
	    }
	    
	    # we're done - updata our meta data
		my $query = "UPDATE Meta SET MergedMemberFileName = '$simpleName'";
		my $rowsAffected = $dbh->do( $query );
		if( $rowsAffected == 0 ) {
			# update failed - must not be any rows to update.  INSERT instead
			$query = "INSERT INTO Meta (MergedMemberFileName) VALUE ('$simpleName')";
			$rowsAffected = $dbh->do( $query );
			if( $rowsAffected == 0 ) {
				# oops - Update failed
				PMSLogging::DumpError( 0, 0, "PMS_ImportPMSData::GetMergedMembers(): " .
					"Unable to perform this INSERT: '$query'", 1 );
			} else {
				print "Insert succeeded:  '$query'\n" if( $debug > 0 );
			}
		} else {
			print "Update succeeded:  '$query'\n" if( $debug > 0 );
		}

	    print( "->Done reading $rowNum rows from our MergedMembers file.\n" );
	} # end of 	if( $refreshMMFile )...
	else {
		PMSLogging::PrintLog( "", "", "NO (we already have the data from: '$arr_ref->[0]'; $numMMRows rows.)", 1 );
	}
} # end of GetMergedMembers()


1;  # end of module
