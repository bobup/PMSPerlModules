#!/usr/bin/perl -w

# PMSProcess2SingleFile.pm - contains the code that will process a single result file.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMSProcess2SingleFile;
use strict;
use Spreadsheet::Read;
use Text::CSV_XS;
require PMSUtil;
require PMSConstants;
require PMSStoreSingleRow;
require PMS_MySqlSupport;


#use lib '../PerlCpan/Graphics-ColorUtils-0.17/lib';
#use lib '../PerlCpan/Spreadsheet-ParseXLSX-0.27/lib';
#use Spreadsheet::ParseXLSX;


#use Spreadsheet::XLSX;


### CONSTANTS that control the logic
my $MIN_FIELDS = 4;     # a row with more than this many non-empty fields is considered a result line.
						# Less than this many fields implies a blank line, a gender/age group line, a
						# wetsuit designation line, or a header line.  The last two generate warnings
						# and handled as expected.  If a header line has $MIN_FIELDS or more fields
						# then we'll try to use it as a result line and get errors, which is bothersome
						# but OK.


### GLOBALS used to maintain state across rows of a single result file.  These values are
### initialized and returned by ProcessRace, incremented by ProcessRow
my @swimsInThisRace;    # number of CAT1 and CAT2 splashes that we recorded for this race
my @dqsInThisRace;      # number of CAT1 and CAT2 DQs that we recorded for this race
my @ignoredInThisRace;	# number of CAT1 and CAT2 records that appear to be non-DQ results
						#   that we IGNORED for this race (due to data problems)


# ProcessRace - process the results of one open water race.  (May actually contain both cat1 and cat2 races)
# PASSED:
#   fileName - the (full path) name of the file holding the results of the race we're processing
#   swimName - the name of the swim corresponding to the passed fileName.  Used for logging
#   raceFileName - the simple file name of the file holding the results of the race we're processing (the 
#		last simple name in the fileName path)
#   numSwims - the number of this swim (in the order of result files we are processing)
#   $category - category of the race we're processing.  If CAT1 this file may ALSO contain some CAT2 results.
#	$calendarRef - reference to our calendar used to determine the date for each event.  (See the PMSMacros.pm 
#		for the complete definition of our calendar hash.)
#
# RETURNED:
#   - swimsInThisRace (-1 if error)
#   - dqsInThisRace 
#
# SIDE-EFFECTS:
#   The database is populated by this function, containing data gathered while processing the race passed to this function.
#   
sub ProcessRace( $$$$$ ) {
    my( $fileName, $eventName, $raceFileName, $numSwims, $category, $calendarRef ) = @_;
    my $result = -1;
	my $resultsGender = "";
	my $resultsAgeGrp = "";
    my $rowNum = 0;
	
	@swimsInThisRace = (0,0,0);		# number of CAT1 and CAT2 splashes in this race. [0] is unused
    @dqsInThisRace = (0,0,0);		# number of CAT1 and CAT2 DQs in this race.  [0] is unused
    @ignoredInThisRace = (0,0,0);	# number of CAT1 and CAT2 records ignored.  [0] is unused
    
    # get the number of non-pms swimmers we've found so far (when processing other result files)
    my $currentCountOfLoggedSwimmers = PMS_MySqlSupport::GetNumLoggedInvalidSwimmers();
	
    PMSLogging::printLog( "\nProcessing '$raceFileName' (aka '$eventName')\n" );
    
    # what kind of file is this?  Use the file extension to tell us:
    my $ext = $fileName;
    $ext =~ s/^.*\.//;
    $ext = lc( $ext );

    # Store the detauls that we know about this event into our DB:
    my $distance = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "Distance" );
    my $eventDate = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "Date" );
 	my $eventUniqueID = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "UniqueID" );
    my $eventId = PMS_MySqlSupport::InitialRecordThisEvent( $eventName, $fileName, $raceFileName, $ext, $category,
    	$eventDate, $distance, $eventUniqueID, -1, -1 );
    
    # Now, get to work!
    if( ! $ext ) {
    	# no extension?  give up
    	@swimsInThisRace = (0,-1,-1);
    } elsif( ($ext eq "txt") || ($ext eq "csv") ) {
    	# csv or tab-seperated file
    	my $seperator = "\t";
    	$seperator = "," if( $ext eq "csv" );
         my @rows;
         my $csv = Text::CSV_XS->new ({ binary => 1, sep_char => $seperator }) or
             die "Cannot use CSV: ".Text::CSV_XS->error_diag ();
         open my $fh, "<:encoding(utf8)", "$fileName" or do {
            PMSLogging::DumpError( "", "", "PMSProcess2SingleFile::ProcessRace():  Unable to open '$fileName' - ABORT!", 1 );
         	die "PMSProcess2SingleFile::ProcessRace(): ABORT: Can't open '$fileName': $!";
         };
        print "file $fileName: Number of sheets:  1 (it's a " .
        	( $seperator eq "," ? "comma-separated" : "tab-separated" ) . " .$ext file).\n" if( $PMSConstants::debug >= 1);
         while (my $row = $csv->getline ($fh)) {
            $rowNum++;
            my $previousCategory = $category;
            ProcessRow( $eventName, $raceFileName, $rowNum, $row, \$resultsGender, \$resultsAgeGrp,
                \$category, $numSwims, $eventId );
            if( ($previousCategory == 1) && ($category == 2) ) {
            	# the results file contains both cat1 and cat2 results - we're switching to cat2, so
            	# we need to record this as a separate event.
				# Now that we know the stats on the previous cat1 event we'll update the DB
				PMS_MySqlSupport::UpdateThisEvent( $eventId, $swimsInThisRace[1], $dqsInThisRace[1] );
				
				# initialize this new event:
			    $eventId = PMS_MySqlSupport::InitialRecordThisEvent( $eventName, $fileName, $raceFileName, $ext, $category,
			    	$eventDate, $distance, $eventUniqueID, -1, -1 );
            }
         }
         $csv->eof or $csv->error_diag ();
         close $fh;
         $result = $rowNum;
    } else {
    	# .xlsx file
    	$result = 0;
	    # read the spreadsheet
	    my $g_ref = ReadData( $fileName );
	    
	    # NOTE:  if the file doesn't exist the above returns a null or empty (?) ref which causes errors below
	    
	    # $g_ref is an array reference
	    # $g_ref->[0] is a reference to a hashtable:  the "control hash"
	    my $numSheets = $g_ref->[0]{sheets};        # number of sheets, including empty sheets
	    print "file $fileName: Number of sheets:  $numSheets.  Non-empty sheets:\n" if( $PMSConstants::debug > 0);
	    
	    my $sheetNames_ref = $g_ref->[0]{sheet};  # reference to a hashtable containing names of non-empty sheets.  key = sheet
	                                              # name, value = monotonically increasing integer starting at 1 
	    my %tmp = % { $sheetNames_ref } ;         # hashtable of sheet names (above)
	    my ($sheetName);
	    foreach $sheetName( sort { $tmp{$a} <=> $tmp{$b} } keys %tmp ) {
	        print "  $sheetName\n" if( $PMSConstants::debug > 0 );
	    }
	    
	    # get the first sheet
	    my $g_sheet1_ref = $g_ref->[1];         # reference to the hashtable representing the sheet
	    my $numRows = $g_sheet1_ref->{maxrow};
	    my $numColumns = $g_sheet1_ref->{maxcol};
	    my $rowArrayRef = $g_sheet1_ref->{cell};
        my $rowArrayRef_0 = $rowArrayRef->[0];
        my $rowArrayRef_1 = $rowArrayRef->[1];
	    print "numRows=$numRows, numCols=$numColumns\n" if( $PMSConstants::debug > 0 );
	    # pass through the sheet collecting initial data on all swimmers:
	    # (skip first row because we assume it has row titles)
	    
	    $result = $numRows;
	    for( $rowNum = 1; $rowNum <= $numRows; $rowNum++ ) {
	    	my @row = ();
	    	for( my $colNum = 1; $colNum <= $numColumns; $colNum++ ) {
                $row[$colNum-1] = $g_sheet1_ref->{cell}[$colNum][$rowNum];
	    	}
            my $previousCategory = $category;
	    	ProcessRow( $eventName, $raceFileName, $rowNum, \@row, \$resultsGender, \$resultsAgeGrp,
	    	    \$category, $numSwims, $eventId );
            if( ($previousCategory == 1) && ($category == 2) ) {
            	# the results file contains both cat1 and cat2 results - we're switching to cat2, so
            	# we need to record this as a separate event.
				# Now that we know the stats on the previous cat1 event we'll update the DB
				PMS_MySqlSupport::UpdateThisEvent( $eventId, $swimsInThisRace[1], $dqsInThisRace[1] );
				
				# initialize this new event:
			    $eventId = PMS_MySqlSupport::InitialRecordThisEvent( $eventName, $fileName, $raceFileName, $ext, $category,
			    	$eventDate, $distance, $eventUniqueID, -1, -1 );
            }
	    }
    } # done with .xlsx file
    
    # At this point we've finished processing the file (regardless of its type)
    my $swimsInThisRace = $swimsInThisRace[1] + $swimsInThisRace[2];
    my $dqsInThisRace = $dqsInThisRace[1] + $dqsInThisRace[2];
    my $ignoredInThisRace = $ignoredInThisRace[1] + $ignoredInThisRace[2];
    my $numSwimmers = $swimsInThisRace + $dqsInThisRace + $ignoredInThisRace;
    my $countOfLoggedSwimmersForThisFile = PMS_MySqlSupport::GetNumLoggedInvalidSwimmers() - $currentCountOfLoggedSwimmers;
    PMSLogging::printLog( "(There were $numSwimmers swimmers competing in this race:  $swimsInThisRace placed " .
    	"($swimsInThisRace[1] CAT1 and $swimsInThisRace[2] CAT2), " .
    	"$dqsInThisRace were DQed ($dqsInThisRace[1] CAT1 and $dqsInThisRace[2] CAT2),\n  " .  
    	"and we had $ignoredInThisRace results ignored for data inconsistency problems " .
    	"($ignoredInThisRace[1] CAT1 and $ignoredInThisRace[2] CAT2.)  " .
    	"\n  A total of $result rows were read, $countOfLoggedSwimmersForThisFile of which were " .
    	"logged with some kind of problem.)\n" );
	
	# now that we know the stats on this event we'll update the DB
	PMS_MySqlSupport::UpdateThisEvent( $eventId, $swimsInThisRace[$category], $dqsInThisRace[$category] );

    return ($swimsInThisRace,$dqsInThisRace);
        
} # end of ProcessRace()




# ProcessRow - process a single row of a results file.  This row may or may not contain actual results (it
#   might, instead, contain column headings, a blank line, etc.)
#
# PASSED:
#   $eventName - the name of the swim
#   $raceFileName - the name of the file representing the results of this swim
#   $rowNum - the number of the row in the file (starting at 1)
#   $row - a reference to the row to be processed (an array of fields)
#   $resultsGenderRef - a reference to the gender of this row.  This value can be changed
#       by this function.
#   $resultsAgeGrpRef - a reference to the age group of this row.  This value can be changed
#       by this function.
#   $categoryRef - a reference to the category we're currently processing. This value can be changed
#       by this function.
#   $numSwims - the number of this swim
#	$eventId - Id of this event in our database.
#
# RETURNED:
# 	return 0 if OK, or # errors if errors
#
# NOTES:
# Assume a row is 0 or more non-empty fields.  If > 0 then it's:
# - one field indicating beginning of cat 2 entries, or
# - a bunch of fields containing the column headings, or
# - one field giving the gender and/or age group of the following results (DEPRECATED), or
# - a bunch of fields that looks like real results, or
# - a bunch of fields that looks like a DQ, or
# - 1 or more fields that don't look like any of the above
#
# Example result row:
# genderAgeGrp   place   lastname   firstname   MI   team   age   reg#   DOB   time|distance
#	or
# place   lastname   firstname   MI   team   age   reg#   DOB   time|distance  (DEPRECATED)
#   or
# 		  place   lastname   firstname   MI   team   age   reg#   DOB   time|distance  (DEPRECATED)
#
sub ProcessRow( $$$$$$$$ ) {
    my( $eventName, $raceFileName, $rowNum, $row, $resultsGenderRef, $resultsAgeGrpRef,
        $categoryRef, $numSwims, $eventId ) = @_;

	my $errors = 0;
	my $rowAsString = "";
	
	# Use the passed $row (reference to an array of fields) to construct a single string, which is
	# a comma separated string of fields.  First, define the character that separates fields in the 
	# string we're about to construct:
	my $fieldSeperator = ",";
	# Now, produce the string, and while we do it we'll clean each field of leading and trailing whitespace:
    ($rowAsString, my $numNonEmptyFields) = PMSUtil::CleanAndConvertRowIntoString( $row );
    # We'll use this string for easly printing the row we're processing, and also doing some
    # context-sensitive pattern matching.

    # Use the number of non-empty fields to help decide what kind of row we have
    if( $numNonEmptyFields == 0 ) {
    	# empty row - ignore it.
    } elsif( $numNonEmptyFields == 1 ) {
    	#  Look for those situations where a row with a single field contains something useful:
    	if( BeginWetSuit( $row ) ) {
    		# found a row starting cat 2 results
            PMSLogging::DumpRowWarning( $row, $rowNum, "Found wetsuit division - processing as category 2" );
            $$categoryRef = 2;
    	} else {
    		# did we find a row containing only gender and/or age group?  (DEPRECATED, but we're still allowing it)
	        my ($genAgegrp, $newGender, $newAgeGrp) = GenderAgeGrpRow( $row, $rowNum, $row->[0] );
	        if( $genAgegrp == 3 ) {
	            # this row contains a gender and/or age group - need to remember it for following rows (if they don't have one)
	            $$resultsGenderRef = $newGender if( $newGender ne "" );
	            $$resultsAgeGrpRef = $newAgeGrp if( $newAgeGrp ne "" );
	        } else {
                PMSLogging::DumpRowWarning( $row, $rowNum, "Found a row with only 1 unrecognized field - ignored." );
	        }
    	}
    } # end of $numNonEmptyFields == 1...
    # else we must have 2 or more fields in this row:    
    elsif(	$numNonEmptyFields < $MIN_FIELDS ) {
            # then assume this is a header field
            PMSLogging::DumpRowWarning( $row, $rowNum, "PMSProcess2SingleFile::ProcessRow: Found heading - ignored." );
    } else {
    	# This row has at least $MIN_FIELDS non-empty fields.
    	# Look for a DQ, which is a normal result line except the time or place field contains one of:
    	#	DQ, DNF, DNS
    	# We actually allow any field to contain any of the above strings to recognize a DQ.  Many of the timers
    	# like to use "***" surrounding the DQ so we'll allow that, too.
        # Specifically, we'll look for this:
        #	,***dq***,
        # where ',' is $fieldSeperator, *** is 0 or more *'s or other special chars, 
        #   'dq' is one of the "dq strings" we're looking for,
        #	and the left ',' can be a beginning of line; right ',' is a end of line
        if( ( ($rowAsString =~ m/($fieldSeperator|^)[\*)(]*dq[\*)(]*($fieldSeperator|$)/i) ||
              ($rowAsString =~ m/($fieldSeperator|^)[\*)(]*dnf[\*)(]*($fieldSeperator|$)/i) ||
              ($rowAsString =~ m/($fieldSeperator|^)[\*)(]*dns[\*)(]*($fieldSeperator|$)/i) ||
              (0) ) ) {
            ###
            ###  We've found a DQ!!!
            ###
            PMSLogging::DumpRowWarning( $row, $rowNum, "Ignoring possible DQ line with multiple fields." );
            $dqsInThisRace[$$categoryRef]++;
        } else {
        	# FINALLY!  This is starting to look like a real result!  First, extract the gender/age group from it:
	        my ($genAgegrp, $newGender, $newAgeGrp) = GenderAgeGrpRow( $row, $rowNum, $row->[0] );
	        if( $genAgegrp == 3 ) {
	        	# this row contains a gender and/or age group - need to remember it for following rows (if they don't have one)
				$$resultsGenderRef = $newGender if( $newGender ne "" );
				$$resultsAgeGrpRef = $newAgeGrp if( $newAgeGrp ne "" );
	        }
			# regardless of whether or not the line contains a gender and/or age group, if it contains 
			# multiple fields AND we have seen a gender and age group THEN we'll treat this as a 
			# results row:
	        if( $$resultsGenderRef ne "" && 
	            $$resultsAgeGrpRef ne ""  ) {
	            ###
	            ###  We've MAY HAVE found a results row!!!  (well, at least it has multiple fields)
	            ###
	            $errors += ProcessResultRow( $eventName, $raceFileName, $rowNum, $row, 
	                $$resultsGenderRef, $$resultsAgeGrpRef, $$categoryRef, $numSwims, $eventId, 
	                $genAgegrp, $newGender, $newAgeGrp );
            } else {
                PMSLogging::DumpRowWarning( $row, $rowNum, "PMSProcess2SingleFile::ProcessRow: Found what looks like a result row, but we've " .
                	"found no gender/age group indication.\n    We'll ignore this row." );
            }
        } # end of "starting to look like a real result..."
    } # end of "This row has at least $MIN_FIELDS..."

	return $errors;
} # end of ProcessRow()




# ProcessResultRow - process a single result row of a results file.
#
# PASSED:
#   $eventName - the name of the swim
#   $raceFileName - the name of the file representing the results of this swim
#   $rowNum - the number of the row in the file (starting at 1)
#   $rowRef - a reference to the row to be processed (an array of fields)
#   $resultsGender - the gender of this row.  Used only if the row doesn't contain this info
#   $resultsAgeGrp - the age group of this row.  Used only if the row doesn't contain this info
#   $category - the category we're currently processing.
#   $numSwims - the number of this swim
#	$eventId -
#	$genAgegrp, $newGender, $newAgeGrp - values returned by calling GenderAgeGrpRow() on this row
#
# RETURNED:
# 	return 0 if OK, or # errors if errors
#
# NOTES:
#	All we really know is that we have a gender and age group (but not necessarily from the passed row), and
#	we have a row of "stuff" that occupies multiple fields.  If the fields don't "look right" we'll reject the
#	row.
#	We also know that this is NOT a DQ result since we already filtered for that.
#
#	Assume a row is of one of these forms:
# 		genderAgeGrp   place   lastname   firstname   MI   team   age   reg#   DOB   time|distance
#   or (this is DEPRECATED)
# 		place   lastname   firstname   MI   team   age   reg#   DOB   time|distance  (DEPRECATED)
#		(where 'place' is in column 1 and the above line is preceded with some kind 
#		of gender/age group designation.)
#   or (this is DEPRECATED)
# 		""	place   lastname   firstname   MI   team   age   reg#   DOB   time|distance  (DEPRECATED)
#		(where "" means column 1 is empty and 'place' is in column 2 and the above line is preceded with some kind 
#		of gender/age group designation.)
#
sub ProcessResultRow( $$$$$$$$$$$$ ) {
    my( $eventName, $raceFileName, $rowNum, $rowRef, $resultsGender, $resultsAgeGrp, 
        $category, $numSwims, $eventId, $genAgegrp, $newGender, $newAgeGrp ) = @_;
    my $errors = 0;
    
    my $debugLastName = "xxxzzz";
    
    # does this row have its own value for a gender:age group?
    if( $genAgegrp != 3 ) {
    	# nope - we have a row that looks like one of these:
		# place   lastname   firstname   MI   team   age   reg#   DOB   time|distance  (DEPRECATED)
    	#	or
    	# ""	place   lastname   firstname   MI   team   age   reg#   DOB   time|distance  (DEPRECATED)
    	# Handle the second case first:
    	if( $rowRef->[0] eq "" ) {
    		shift( @{$rowRef} );		# remove the leading empty field
    	}
    	# now the row looks like this:
		# place   lastname   firstname   MI   team   age   reg#   DOB   time|distance  (DEPRECATED)

    	# now add the appropriate gender:age group to the front of the row
    	# (This is so we can support the DEPRECATED form of input)
    	unshift(  @{$rowRef}, "$resultsGender:$resultsAgeGrp" );
    } else {
    	# both gender and age group are in the first field of the result row.  But here we'll put 
    	# them in canonical form.
    	$rowRef->[0] = "$newGender:$newAgeGrp";
    }

    # This is a summary of the data that we should have now:
    # $rowRef->[0] - gender:age group
    # $rowRef->[1] - place - must be non-empty
    # $rowRef->[2] - lastname - can be anything but must be non-empty
    # $rowRef->[3] - firstname - can be anything but must be non-empty
    # $rowRef->[4] - MI - can be anything and can be empty
    # $rowRef->[5] - team - can be anything
    # $rowRef->[6] - age - must be non-empty and all digits
    # $rowRef->[7] - reg # - can be anything but must be non-empty
    # $rowRef->[8] - DOB - can be anything but needed if no age ([6]) supplied
    # $rowRef->[9] - time|distance - can be anything 

	# Use the passed $rowRef (reference to an array of fields) to construct a single string, which is
	# a comma separated string of fields.
	# And while we do it we'll clean each field of leading and trailing whitespace:
    (my $rowAsString, my $numNonEmptyFields) = PMSUtil::CleanAndConvertRowIntoString( $rowRef );
    # We'll use this string for easly printing the row we're processing, and also doing some
    # context-sensitive pattern matching.

    # perform sanity check and cleanup of each field:
    # remove trailing '.' from place
    $rowRef->[1] =~ s/\.$//;
    
    # place - must be all digits
    if( $rowRef->[1] !~ m/^[0-9]+$/ ) {
        # place is not all digits
        $errors += PMSLogging::DumpRowWarning( $rowRef, $rowNum, "PMSProcess2SingleFile::ProcessResultRow:  " .
        	"Non-numeric place ('$rowRef->[1]') - LINE IGNORED!" );
    } elsif( $rowRef->[9] !~ m/^[\d:.]+$/ ) {
    	# time of swim doesn't look reasonable
        $errors += PMSLogging::DumpRowWarning( $rowRef, $rowNum, "PMSProcess2SingleFile::ProcessResultRow:  " .
        	"Non-valid time ('$rowRef->[9]') - LINE IGNORED!" );
    } elsif( (!defined( $rowRef->[2] )) || (length( $rowRef->[2] ) == 0) ) {
        $errors += PMSLogging::DumpRowError( $rowRef, $rowNum, "Undefined lastname - LINE IGNORED!" );
    } elsif( (!defined( $rowRef->[3] )) || (length( $rowRef->[3] ) == 0) ) {
        $errors += PMSLogging::DumpRowError( $rowRef, $rowNum, "Undefined firstname - LINE IGNORED!" );
    } elsif( $PMSConstants::RegNumRequired && ((!defined( $rowRef->[7] )) || (length( $rowRef->[7] ) == 0)) ) {
        # Either regnum is required for all races being processed by this program, or it's not. If it is required it must be present and non-empty
        $errors += PMSLogging::DumpRowError( $rowRef, $rowNum, "Undefined Registration number - LINE IGNORED!" );

    } elsif( (!defined( $rowRef->[6] )) || (length( $rowRef->[6] ) == 0) ) {
    	# compute their age using the supplied DOB (if supplied)
    	my $dateOfBirth = $rowRef->[8];		# mm/dd/yyyy
        PMSLogging::DumpRowWarning( $rowRef, $rowNum, "Undefined age - FIX THIS - Warning only..." .
        	" We will try to compute their age from their Date Of Birth ($dateOfBirth)." );
    	$dateOfBirth =~ s/\s*//g;		# remove all whitespace
		my $dateOfBirthDef = PMSUtil::GenerateCanonicalDOB($dateOfBirth);		# yyyy-mm-dd
		my $computedAge = PMSUtil::AgeAtEndOfYear( $dateOfBirthDef );
		$rowRef->[6] = $computedAge;
    } elsif( $rowRef->[6] !~ m/^[0-9]+$/ ) {
        $errors += PMSLogging::DumpRowError( $rowRef, $rowNum, "Non-numeric age ('$rowRef->[6]') - LINE IGNORED!" );
	}

    if( !$errors && PMSUtil::IsValidAge( $rowRef->[6], $resultsAgeGrp ) == $PMSConstants::INVALIDAGE ) {
    	# we're going to log this as an error, but not really count it as an error.  If this is the only problem
    	# with this row we're going to give the swimmer their points, but the error needs to be fixed.
        PMSLogging::DumpRowWarning( $rowRef, $rowNum, "PMSProcess2SingleFile::ProcessResultRow(): " .
        	"Invalid age ($rowRef->[6] is either not a legal age for this group of swimmers\n" .
        	"    or is not in the assigned age group [$resultsAgeGrp]) for this swimmer.  This swimmer will " .
        	"still get points\n    in the assigned age group WHICH MIGHT BE WRONG!\n    " );
    }

	my $temp_avoid_warning = $PMSConstants::INVALIDAGE;		# my compiler is stupid...
	$temp_avoid_warning = $PMSConstants::RegNumRequired;		# my compiler is stupid...

    PMSLogging::printLog( "ProcessResultRow: row passed: '$rowAsString'\n" ) if( $rowRef->[2] =~ m/$debugLastName/i );

    if( $errors == 0 ) {
	    PMSStoreSingleRow::StoreResult( $rowRef, $rowNum, $numSwims, $category, 
	        $raceFileName, $eventId );
	    $swimsInThisRace[$category]++;
    } else {
    	$ignoredInThisRace[$category]++;
    }

    return $errors;
} # end of ProcessResultRow()






# BeginWetSuit - analyze the passed row to see if it is the designation of the beginning of a wetsuit
#	section in the results.
#
# PASSED:
#   $row - a reference to the row to be processed (an array of fields)
#
# RETURNED:
#	1 if this row is the start of a wetsuit section, 0 otherwise.
#
sub BeginWetSuit( $ ) {
    my $row = $_[0];
    my $field0 = lc($row->[0]);
    my $result = 0;
    
    if( ($field0 =~ m/wetsuit|wet.*suit/) ||
        ($field0 =~ m/cat\s*2/) ) {
        $result = 1 if( $field0 !~ m/non/ );
    }
    
    return $result;
} # end of BeginWetSuit




# GenderAgeGrpRow - analyze the passed row to see if it specifying a gender/age group
#
# PASSED:
#   $rowRef - a reference to the row to be processed (an array of fields)
#   $rowNum - the number of the row in the file (starting at 1)
#   $field - a text string that might contain the gender or age group
#
# RETURNED:
#   $fieldFound:
#       Return 1 if a Gender is found in the passed field, (must be legal gender)
#       Return 2 if an age group is found in the passed field,  (must be legal age group)
#       Return 3 if both a gender and age group are found in the passed field, 
#       Return 0 if neither found (or illegal values )
#   $gender - "" if no gender found, or one of "W", "M".
#   $ageGroup - "" if no ageGroup found, or "xx-yy", e.g. "18-24"
#
# NOTES:
#   e.g. "M/25-29" or "Women 25-29" or "Men 18 to 24" or "M:18-24" returns 3
#   "women" or "men" or "M" returns 1
#   "18-24" or "18 to 24" returns 2
#
sub GenderAgeGrpRow {
	my $rowRef = $_[0];
	my $rowNum = $_[1];
    my $field = lc($_[2]);
    my $fieldFound = 0;
    my $gender = "";
    my $ageGroup = "";
    
    # look for gender
    if( $field =~ m/(^m)|(^w)|(^f)|^b|^g/ ) {
        $fieldFound = 1;                # found a gender
	    # get the GENDER since we have one:
	    if( $field =~ m/female|women|^w|^f|girl|^g/ ) {
	        $gender = "W";
	    } elsif( $field =~ m/male|men|m|boy|b/ ) {
	        $gender = "M";
        }
    } # end of gender
    
    # look for age group
    if( $field =~ m/\d\d.*\d\d\s*$/ ) {           # must be dd ... dd followed by optional whitespace
	    # get the AGE GROUP since we have one:
	    $field =~ s/^[^\d]*//;        # remove all leading non-digits (e.g. gender)
	    $field =~ s/[^\d]*$//;        # remove all trailing non-digits (e.g. spaces)
	    $ageGroup = $field;	    
	    if( $ageGroup =~ m/\d/ ) {
	        # assume $ageGroup contains the age group in some format:
	        if( $ageGroup =~ m/-/ ) {
	            # convert xx  -------...--  xx into xx-xx
	            $ageGroup =~ s/\s//g;
	            $ageGroup =~ s/--*/-/;      # to handle 25 -- 29 whiskytown 2011
	        } elsif( $ageGroup =~ m/to/ ) {
	            # convert xx to xx into xx-xx
	            $ageGroup =~ s/\s*to\s*/-/g;
	            # --- 2008 Berryessa
	        } else {
	            # convert 2529 into 25-29
	            $ageGroup =~ s/(^..)/$1-/;
	            # --- 2008 SC Roughwater
	        }
	        # if ageGroup contains a '19', change it to '18' (DelValle used 19-24 instead of 18-24.)
	        # 9oct2010:  but DON'T do this for USA results - in this case it's an error (19 is not a valid age group) but we don't want to change the
	        # age group so the error message is correct.
	        if( $ageGroup =~ m/19/ ) {
	        	PMSLogging::DumpRowWarning( $rowRef, $rowNum, "GenderAgeGrpRow: change age group from 19- to 18-" );
                $ageGroup =~ s/19/18/;
	        }
	        # assume we now have an age group of the form "ddd-ddd" where 'ddd' is 1 or more digits.
	        # break it apart, remove leading zeros, then put it back.
	        my $age1 = my $age2 = $ageGroup;
	        $age1 =~ s/-.*$//;
	        $age1 =~ s/^0*//;
	        $age2 =~ s/^.*-//;
	        $age2 =~ s/^0*//;
	        $ageGroup = "$age1-$age2";
	        
	        # now, make sure we have a legal age group.  If not, try to fix it:
		    if( PMSUtil::IsValidAgeGroup( $ageGroup ) ) {
                $fieldFound += 2;               # found an age group - return 2 or 3 depending on whether or not we found a gender
		    } elsif( $gender eq "" ) {
		    	# The passed $field didn't have a gender, thus we think we're looking for an age group.
		    	# BUT, in this case if it's not a legal age group we're NOT going to try it because it's
		    	# probably not really an age group.  Example: Whiskeytown, 2017:  The put a date in this 
		    	# field, which excel said was "42988" which we then tried to use.  That's a mistake, and
		    	# instead we need to ignore such data.
		    	$fieldFound = 0;
		    } else {
		        # we saw a gender, so this is likely to be an age group.  See if we can fix the obvious errors:
		        my $ageGroupFixed = PMSUtil::FixInvalidAgeGroup( $ageGroup, $age1, $age2 );
	        	PMSLogging::DumpRowError( $rowRef, $rowNum, "PMSProcess2SingleFile::GenderAgeGrpRow(): " .
	        		"Convert bad ageGroup ('$ageGroup') to this ageGroup: $ageGroupFixed" );
	        	$ageGroup = $ageGroupFixed;
		        if( PMSUtil::IsValidAgeGroup( $ageGroup ) ) {
                    $fieldFound += 2;               # found an age group - return 2 or 3 depending on whether or not we found a gender
		        } else {
		        	$ageGroup = "";
		        }
		    }
	    } else {
	    	$ageGroup = "";
	    }
    } # end of age group
    
    if( $gender eq "" || $ageGroup eq "" ) {
    	$fieldFound = 0;           # illegal values found
    }
    
    return ($fieldFound, $gender, $ageGroup);
}  # end of GenderAgeGrpRow






1;  # end of module

