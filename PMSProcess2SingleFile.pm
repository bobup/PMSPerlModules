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
use Data::Dumper qw(Dumper);

sub BeginGenHTMLRaceResults( $$ );
sub EndGenHTMLRaceResults();

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
sub ProcessRace( $$$$$$ ) {
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

    # begin our own generation of human readable results if we're not processing a single file:
	my $hrResults = "";
    if( !PMSStruct::GetMacrosRef()->{"SingleFile"} ) {
		$hrResults = BeginGenHTMLRaceResults( $raceFileName, $calendarRef );
	}
    
    # Store the detauls that we know about this event into our DB:
    my $distance = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "Distance" );
    my $eventDate = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "Date" );
 	my $eventUniqueID = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "UniqueID" );
    my $eventId = PMS_MySqlSupport::InitialRecordThisEvent( $eventName, $fileName, $raceFileName, $ext, $category,
    	$eventDate, $distance, $eventUniqueID, $hrResults, -1, -1 );
    
    
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
        print  "PMSProcess2SingleFile::ProcessRace(): file $fileName: Number of sheets:  1 (it's a " .
        	( $seperator eq "," ? "comma-separated" : "tab-separated" ) . " .$ext file).\n" if( $PMSConstants::debug >= 1);
		while( my $row = $csv->getline( $fh ) ) {
            $rowNum++;
            if( ($PMSConstants::debug >= 100) || 0 ) {
    			(my $rowAsString, my $numNonEmptyFields) = PMSUtil::CleanAndConvertRowIntoString( $row );
            	print  "Row #$rowNum: '$rowAsString'\n";
            }
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
			    	$eventDate, $distance, $eventUniqueID, "", -1, -1 );
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
			    	$eventDate, $distance, $eventUniqueID, "", -1, -1 );
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

	# we're finished generating the human readable results:
    if( !PMSStruct::GetMacrosRef()->{"SingleFile"} ) {
		EndGenHTMLRaceResults();
	}
	
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
# removed this wetsuit hack 7sep2023.
    	if( BeginWetSuit( $row ) && 0 ) {
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
### show dqs in human readable results?
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
    my $errorSummary = "";
    
    my $debugLastName = "xxxxx";
    
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
        $errors++;
        $errorSummary = "The value in the PLACE field ('$rowRef->[1]') contains non-digits.";
    } elsif( $rowRef->[9] !~ m/^[\d:.]+$/ ) {
    	# time of swim doesn't look reasonable
        $errors++;
        $errorSummary = "The value representing the duration of the swim ('$rowRef->[9]') doesn't match the specification.";
    } elsif( (!defined( $rowRef->[2] )) || (length( $rowRef->[2] ) == 0) ) {
        $errors++;
        $errorSummary = "The LASTNAME field is empty.";
    } elsif( (!defined( $rowRef->[3] )) || (length( $rowRef->[3] ) == 0) ) {
        $errors++;
        $errorSummary = "The FIRSTNAME field is empty.";
    } elsif( $PMSConstants::RegNumRequired && ((!defined( $rowRef->[7] )) || (length( $rowRef->[7] ) == 0)) ) {
        # Either regnum is required for all races being processed by this program, or it's not. If it is required it must be present and non-empty
        $errors++;
        $errorSummary = "Missing USMS REGISTRATION NUMBER field.";
    } elsif( (!defined( $rowRef->[6] )) || (length( $rowRef->[6] ) == 0) ) {
    	# compute their age using the supplied DOB (if supplied)
    	my $dateOfBirth = $rowRef->[8];		# mm/dd/yyyy
        PMSLogging::DumpRowWarning( $rowRef, $rowNum, "Undefined age - FIX THIS - Warning only..." .
        	" We will try to compute their age from their Date Of Birth ($dateOfBirth)." );
    	$dateOfBirth =~ s/\s*//g;		# remove all whitespace
		my $dateOfBirthDef = PMSUtil::GenerateCanonicalDOB($dateOfBirth);		# yyyy-mm-dd
		my $computedAge = PMSUtil::AgeAtEndOfYear( $dateOfBirthDef );
		$rowRef->[6] = $computedAge;
        $errorSummary = "Note that the age of this swimmer was not supplied so it was computed from their birthdate.";
    } elsif( $rowRef->[6] !~ m/^[0-9]+$/ ) {
        $errors++;
        $errorSummary = "The AGE field ('$rowRef->[6]') for this swimmer is non-empty but contains non-digits."
	}

    if( !$errors && PMSUtil::IsValidAge( $rowRef->[6], $resultsAgeGrp ) == $PMSConstants::INVALIDAGE ) {
    	# we're going to log this as an error, but not really count it as an error.  If this is the only problem
    	# with this row we're going to give the swimmer their points, but the error needs to be fixed.
    	### we need to handle an empty $errorSummary special: if left empty a line with only spaces in it
    	### will be truncated to an empty line by the php write library.
    	my $details = "PMSProcess2SingleFile::ProcessResultRow()\n  $errorSummary";
    	if( $errorSummary eq "" ) {
    		$details = "PMSProcess2SingleFile::ProcessResultRow()";
    	}
    	
    	
        PMSLogging::DumpFatalRowError( $rowRef, $rowNum,
        	"The age of the swimmer ($rowRef->[6]) in row $rowNum is not in their assigned age group " .
        	"($resultsAgeGrp).\n  " . $details, 1 );
    } elsif( $errors ) {
        PMSLogging::DumpFatalRowError( $rowRef, $rowNum,
        	"There is something wrong with this row, so it will be ignored.\n  $errorSummary\n  " .
        	"Are you sure you have all of the required columns in the correct order?\n  " .
        	"PMSProcess2SingleFile::ProcessResultRow()\n  " .
        	"It should look like this:\n  " .
        	"Gender:Age Group , Place, Last, First, Middle, Team, Age, Reg #, DOB, Time", 1 );
    }

	my $temp_avoid_warning = $PMSConstants::INVALIDAGE;		# my compiler is stupid...
	$temp_avoid_warning = $PMSConstants::RegNumRequired;		# my compiler is stupid...

	if( $rowRef->[2] =~ m/$debugLastName/i ) {
    	PMSLogging::printLog( "ProcessResultRow: row passed: '$rowAsString'\n" ) ;
    	PMSLogging::printLog( "ProcessResultRow: Call StoreResult with rowNum='$rowNum'\n" ) ;
	}

    if( $errors == 0 ) {
	    PMSStoreSingleRow::StoreResult( $rowRef, $rowNum, $numSwims, $category, 
	        $raceFileName, $eventId );
	    $swimsInThisRace[$category]++;
		# generate a human readable result row for our human readable results html file:
		if( !PMSStruct::GetMacrosRef()->{"SingleFile"} ) {
			GenHTMLRaceResultRow( $rowRef, $category );
		}
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
#	        	PMSLogging::DumpFatalRowError( $rowRef, $rowNum, "PMSProcess2SingleFile::GenderAgeGrpRow(): " .
#	        		"Convert bad ageGroup ('$ageGroup') to this ageGroup: $ageGroupFixed" );
	        	PMSLogging::DumpFatalRowError( $rowRef, $rowNum, "Found an invalid age group: '$ageGroup' on row " .
	        		"$rowNum. It's likely that the age group should be fixed to be '$ageGroupFixed' but please " .
	        		"confirm (and adjust as necessary) and then fix the file." .
	        		"\n  Details: PMSProcess2SingleFile::GenderAgeGrpRow()" );
	        	
	        	
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


#==================================================
#======= GENERATE HUMAN READABLE RESULTS===========
#==================================================
#==================================================


use Time::Piece;
# the file handle for the HTML human readable age group results file we're generating:
my $generatedAGFileHandle;
# the file handle for the HTML human readable overall results file we're generating:
my $generatedORFileHandle;

# 	my $hrResults = BeginGenHTMLRaceResults( $raceFileName, $calendarRef );
#
# BeginGenHTMLRaceResults - Initialize the creation  of human readable results and overall results
#	for this event.
#
# PASSED:
#	$raceFileName - the partial path name of the file we're processing for this event.
#	$calendarRef - a reference to the calendar hash, part of which describes this event.
#
# RETURNED:
#	$result - the link to the human readable results initialized by this routine.
#
# SIDE EFFECTS:
#	Two files are created and partially written:
#		- human readable age group results in .html form
#		- human readable overall results in .html form
#
sub BeginGenHTMLRaceResults( $$ ) {
    my($raceFileName, $calendarRef ) = @_;
    my $eventDate = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "Date" );		# yyyy-m-d
    my $eventName = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "EventName" );
    my $cat = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "CAT" );
    my $link = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "Link" );
    if( ! defined $link ) {
    	$link = "https://www.pacificmasters.org/page.cfm?pagetitle=Open+Water+Competition";
    }
	# get the date in a more human friendly format:
	my $date = Time::Piece->strptime( $eventDate, "%F" );
	my $mydate = $date->strftime( "%A, %B %e, %Y" );

	# get some values we're using in our template files:
	PMSStruct::GetMacrosRef()->{"EventName"} = $eventName;
	PMSStruct::GetMacrosRef()->{"Date"} = $mydate;
	PMSStruct::GetMacrosRef()->{"CAT"} = $cat;
	PMSStruct::GetMacrosRef()->{"catmeaning"} = "No Wetsuit";
	if( $cat == 2 ) {
		PMSStruct::GetMacrosRef()->{"catmeaning"} = "Wetsuit";
	}
	PMSStruct::GetMacrosRef()->{"Link"} = $link;

	###############################################################################
	########################## AGE GROUP RESULTS ##################################
	###############################################################################
	# compute a background picture
	my $backPictureAG = ComputeBackgroundImage( $raceFileName, $calendarRef );
	# get the full path name of the template file we're going to use:
	my $templateGenResRoot = PMSStruct::GetMacrosRef()->{"templateRootRoot"} . "TemplatesGenRes/";
	my $templateGenResHeadPathName = $templateGenResRoot . "ReadableResultHead.html";
	# get the full path name of the HTML file we're going to generate and open it for writing
	my $genSimpleFileName = "$eventName-cat$cat-AG.html";
	# modify the file name:
	#	replace spaces with underscores
	$genSimpleFileName =~ s/\s+/_/g;
	#	replace '/' with dash
	$genSimpleFileName =~ s;/+;-;g;
	# remember this file name so we can link to it in the OW points page
#	PMSLogging::DumpNote( "", "", "Begin generation of human readable results: file generated: '$genSimpleFileName'", 1);
	my $generatedFileName = PMSStruct::GetMacrosRef()->{"hrResultsFullDir"} . "$genSimpleFileName";
	open( $generatedAGFileHandle, "> $generatedFileName" ) || die( "PMSProcess2SingleFile::BeginGenHTMLRaceResults(): " .
		"  Can't open/create $generatedFileName: $!" );

	###############################################################################
	########################## OVERALL RESULTS ####################################
	###############################################################################
	# compute a background picture
	my $backPictureOR = ComputeBackgroundImage( $raceFileName, $calendarRef );
	PMSStruct::GetMacrosRef()->{"BackgroundPicture"} = $backPictureOR;
	# get the full path name of the template file we're going to use:
	my $templateGenORRoot = PMSStruct::GetMacrosRef()->{"templateRootRoot"} . "TemplatesGenRes/";
	my $templateGenORHeadPathName = $templateGenResRoot . "OverallResultHead.html";
	# get the full path name of the HTML file we're going to generate and open it for writing
	my $genORSimpleFileName = "$eventName-cat$cat-OR.html";
	# modify the file name:
	#	replace spaces with underscores
	$genORSimpleFileName =~ s/\s+/_/g;
	#	replace '/' with dash
	$genORSimpleFileName =~ s;/+;-;g;
	# remember this file name so we can link to it in the OW points page
#	PMSLogging::DumpNote( "", "", "Begin generation of overall results: file generated: '$genORSimpleFileName'", 1);
	my $generatedORFileName = PMSStruct::GetMacrosRef()->{"hrResultsFullDir"} . "$genORSimpleFileName";
	open( $generatedORFileHandle, "> $generatedORFileName" ) || die( "PMSProcess2SingleFile::BeginGenHTMLRaceResults(): " .
		"  Can't open/create $generatedORFileName: $!" );
	PMSStruct::GetMacrosRef()->{"AGFileName"} = $genSimpleFileName;
	PMSTemplate::ProcessHTMLTemplate( $templateGenORHeadPathName, $generatedORFileHandle );

	###############################################################################
	########################## AGE GROUP RESULTS ##################################
	###############################################################################
	PMSStruct::GetMacrosRef()->{"BackgroundPicture"} = $backPictureAG;
	PMSStruct::GetMacrosRef()->{"ORFileName"} = $genORSimpleFileName;
	PMSTemplate::ProcessHTMLTemplate( $templateGenResHeadPathName, $generatedAGFileHandle );

	my $result = PMSStruct::GetMacrosRef()->{"hrResultsSimpleDir"} . $genSimpleFileName;
	#print "BeginGenHTMLRaceResults(): backPictureAG='$backPictureAG', backPictureOR='$backPictureOR', " .
	#	"return '$result'\n";
	return $result;
	
} # end of BeginGenHTMLRaceResults()





#
# 		GenHTMLRaceResultRow( $rowRef, $category );
#
# GenHTMLRaceResultRow - Given a single row of our race results describing the swim by
#	one swimmer generate a single row of human readable results, and also store information
#	to later be used to generate overall results.
#
# PASSED:
#	$rowRef - a reference to an array holding details of one specific swim in this event.
#	$category - the suit category of the event.
#
# RETURNED:
#	n/a
#

# Static variables used by this routine and other related routines:
my $currentGenderAge = "";
my $colorClass = "";			# will be used to color some rows
my $numberWithThisGenderAge = "";

# CONSTRUCTING OVERALL RESULTS:
# %overall{id} = timeInCS   -  id is a unique id (numeric) used to identify this swimmer
#		in this event. The timeInCS is the time of a swim, in hundredths of a second
# %overallDetails{id} = TheDetails - id is the same as above. TheDetails is the following string:
#		Name:::age:::gender:age group:::club:::time:::humanTime
#	where:
#		Name - is the name of the swimmer:  First Last
#		age - is the age of  the swimmer (as of Dec 31 of race year)
#		gender:age group - gender is 'M' for men; age group is xx-yy
#		club - initials of Club
#		time  - the time in hundredths of a second
#		humanTime - the time in the form 1:03:33.09 (1 hour, 3 minutes, 33 seconds, 9 hundredths
my %overall = ();
my %overallDetails = ();
my $overallId = 0;

sub GenHTMLRaceResultRow( $$ ) {
	my ($rowRef, $category) = @_;
	
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

	###############################################################################
	########################## AGE GROUP RESULTS ##################################
	###############################################################################
	# this is used to get the full path name of the template file we're going to use:
	my $templateGenResRoot = PMSStruct::GetMacrosRef()->{"templateRootRoot"} . "TemplatesGenRes/";

	# First, is this a new gender/age group?
	if( $currentGenderAge ne $rowRef->[0] ) {
		# YES! begin a new section
		# get the full path name of the template files we're going to use:
		my $templateGenResSectionStartPathName = $templateGenResRoot . "ReadableResultBeginSection.html";
		my $templateGenResSectionEndPathName = $templateGenResRoot . "ReadableResultEndSection.html";
	
		# end the current section (unless this is the beginning of the first section)
		if( $currentGenderAge ne "" ) {
			PMSTemplate::ProcessHTMLTemplate( $templateGenResSectionEndPathName, $generatedAGFileHandle );
		}
		
		# start the new section
		$rowRef->[0] =~ m/^(.):(.+)$/;
		my $gender = $1;
		my $ageGroup = $2;
		if( $gender eq "M" ) {
			$gender = "Men";
			$colorClass = "resultRowClassMen";
		} else {
			$gender = "Women";
			$colorClass = "resultRowClassWomen";
		}
		PMSStruct::GetMacrosRef()->{"Gender"} = $gender;
		PMSStruct::GetMacrosRef()->{"AgeGroup"} = $ageGroup;
		PMSTemplate::ProcessHTMLTemplate( $templateGenResSectionStartPathName, $generatedAGFileHandle );
		$currentGenderAge = $rowRef->[0];
		# keep track of the odd/even rows:
		$numberWithThisGenderAge = 0;
	}

	$numberWithThisGenderAge++;
	
	# Now we're ready to generate the single result row for the passed swimmer:
	# get the full path name of the template file we're going to use:
	my $templateGenResRowPathName = $templateGenResRoot . "ReadableResultRow.html";

	# generate one row of the human readable results:
	PMSStruct::GetMacrosRef()->{"Place"} = $rowRef->[1];
	PMSStruct::GetMacrosRef()->{"Name"} = $rowRef->[3] . " " . $rowRef->[2];
	PMSStruct::GetMacrosRef()->{"Age"} = $rowRef->[6];
	
	
############################club
	PMSStruct::GetMacrosRef()->{"Club"} = $rowRef->[5];
	PMSStruct::GetMacrosRef()->{"Time"} = $rowRef->[9];
	# how do we color this row?
	my $colorForThisRow = "";
	if( $numberWithThisGenderAge % 2 ) {
		$colorForThisRow = $colorClass;
	} else {
		$colorForThisRow = "resultRowClassNoColor";
	}
	PMSStruct::GetMacrosRef()->{"resultrowclass"} = $colorForThisRow;
	PMSTemplate::ProcessHTMLTemplate( $templateGenResRowPathName, $generatedAGFileHandle );

	###############################################################################
	########################## OVERALL RESULTS ####################################
	###############################################################################
	# NOW, we're going to record details on this swimmer and race so we can later generate
	# overall results:
	my $timeInCS = PMSUtil::GenerateCanonicalDurationForDB_v2( PMSStruct::GetMacrosRef()->{"Time"},
		0, "", "", "called by GenHTMLRaceResultRow()" );
	$overallId++;
	$overall{$overallId} = $timeInCS;
	$overallDetails{$overallId} = PMSStruct::GetMacrosRef()->{"Name"} . ":::" .
		PMSStruct::GetMacrosRef()->{"Age"} . ":::" .
		$rowRef->[0] . ":::" .
		PMSStruct::GetMacrosRef()->{"Club"} . ":::" .
		$timeInCS . ":::" .
		PMSStruct::GetMacrosRef()->{"Time"};
		
} # end of GenHTMLRaceResultRow()



#
# AscendingTimeInCS - used to sort the %overall hash for overall results display
#	This function uses the Perl sort facility that allows traversing a hash in a sort
#	order of the VALUE of the hash, not the key.  Used like this:
#			foreach my $key( sort AscendingTimeInCS( keys %overall ) ) {...
#
sub AscendingTimeInCS {
	$overall{$a} <=> $overall{$b};
} # end of AscendingTimeInCS()





# EndGenHTMLRaceResults - finish generating the human readable results for a single
#	event. In addition, construct the overall results for this event.
#
sub EndGenHTMLRaceResults() {

	###############################################################################
	########################## AGE GROUP RESULTS ##################################
	###############################################################################
	# get the full path name of the template files we're going to use:
	my $templateGenResRoot = PMSStruct::GetMacrosRef()->{"templateRootRoot"} . "TemplatesGenRes/";

	# if a section is in progress then we need to end it:
	if( $currentGenderAge ne "" ) {
		my $templateGenResSectionEndPathName = $templateGenResRoot . "ReadableResultEndSection.html";
		PMSTemplate::ProcessHTMLTemplate( $templateGenResSectionEndPathName, $generatedAGFileHandle );
	}

	# now end the readable results html file:
	my $templateGenResTailPathName = $templateGenResRoot . "ReadableResultTail.html";
	# ...all done with human readable results!
	PMSTemplate::ProcessHTMLTemplate( $templateGenResTailPathName, $generatedAGFileHandle );

	close( $generatedAGFileHandle );
	undef $generatedAGFileHandle;

	###############################################################################
	########################## OVERALL RESULTS ####################################
	###############################################################################
	my $oaPlace = 0;
	my $previousTime = 0;
	my $placeCatchUp = 0;
	# get the full path name of the template file we're going to use:
	my $templateGenORRoot = PMSStruct::GetMacrosRef()->{"templateRootRoot"} . "TemplatesGenRes/";
	my $templateGenORRowPathName = $templateGenResRoot . "OverallResultRow.html";
	# pass through our list of swimmers, fastest to slowest, and keep track of every
	# swimmer's overall place
	my $rowColor = 0;
	foreach my $key( sort AscendingTimeInCS( keys %overall ) ) {
		my $detailString = $overallDetails{$key};			# details of this swimmer
		# This detail string looks like this:
		#	Name:::Age:::Gender:Age Group:::Club:::Time in CS:::human readable time
		my @details = split(  ":::", $detailString );
		my $time = $details[4];						# time in hundredths
		if( $time == $previousTime ) {
			# this swimmer  tied the previous swimmer, so they are the same overall place.
			# but we need to keep track of the overall places...
			$placeCatchUp++;
		} else {
			$oaPlace += ($placeCatchUp + 1);
			$placeCatchUp = 0;
			$previousTime = $time;
		}
		PMSStruct::GetMacrosRef()->{"ORPlace"} = $oaPlace;
		PMSStruct::GetMacrosRef()->{"ORName"} = $details[0];
		PMSStruct::GetMacrosRef()->{"ORAge"} = $details[1];
		PMSStruct::GetMacrosRef()->{"ORGenAge"} = $details[2];
		PMSStruct::GetMacrosRef()->{"ORClub"} = $details[3];
		PMSStruct::GetMacrosRef()->{"ORTime"} = $details[5];
		# set our row color:
		$rowColor++;
		$rowColor = 1 if( $rowColor > 2 );
		PMSStruct::GetMacrosRef()->{"RowSpanOptions"} = "background-color:Black; color:White"
			if( $rowColor == 1 );
		PMSStruct::GetMacrosRef()->{"RowSpanOptions"} = "background-color:White; color:Black"
			if( $rowColor == 2 );
		# generate the row
		PMSTemplate::ProcessHTMLTemplate( $templateGenORRowPathName, $generatedORFileHandle );
	} # end of foreach...
	# done with overall results - finish it up:
	my $templateGenORTailPathName = $templateGenResRoot . "OverallResultTail.html";
	PMSTemplate::ProcessHTMLTemplate( $templateGenORTailPathName, $generatedORFileHandle );
	
	close( $generatedORFileHandle );
	undef( $generatedORFileHandle );
	%overall = ();
	
} # end of EndGenHTMLRaceResults()



# 	my $backPicture = ComputeBackgroundImage( $raceFileName, $calendarRef );
#
# ComputeBackgroundImage - figure out what background image (if any) we will show with
#	the passed OW event.
#
# PASSED:
#	$raceFileName - the partial path name of the file we're processing for this event.
#	$calendarRef - a reference to the calendar hash, part of which describes this event.
#
# RETURNED:
#	$result - path name to the background image to use, or "" if none. The path is relative
#		to the human readable result file(s) that the image will be used in.
#
sub ComputeBackgroundImage( $$ ) {
	my ($raceFileName, $calendarRef) = @_;
	my $result = "";

    my $keyword = PMSUtil::GetEventDetail( $raceFileName, $calendarRef, "Keywords" );
    # use the first keyword as the name of the directory holding background images for this event
    $keyword =~ s/,.*$//;
	# modify the file name:
	#	replace spaces with underscores
	$keyword =~ s/\s+/_/g;
	#	replace '/' with dash
	$keyword =~ s;/+;-;g;
	# compute the full path name of the directory holding background images for this event:
	my $fullPath = PMSStruct::GetMacrosRef()->{"AppDirName"} . "/Background/" . 
		PMSStruct::GetMacrosRef()->{"YearBeingProcessed"} . "/$keyword/";
	# get a list of .jpg and .jpeg files in the above directory:
	my $dirHandle;
	my $numBackgrounds = 0;
	my @listOfFiles;
	if( opendir( $dirHandle, $fullPath ) ) {
		# we have a directory of 0 or more background images to be used
		@listOfFiles = grep( /(.jpg)|(.jpeg)$/i, readdir $dirHandle ); 
		closedir( $dirHandle );
		$numBackgrounds = scalar( @listOfFiles );
	}
	if( $numBackgrounds > 0 ) {
		my $index = int( rand( $numBackgrounds ) );
		$result = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"} . "/$keyword/" . $listOfFiles[$index];
	} else {
		# no specific background  images - use some default one
		$fullPath = PMSStruct::GetMacrosRef()->{"AppDirName"} . "/Background/misc/";
		if( opendir( $dirHandle, $fullPath ) ) {
			# we have a directory of 0 or more background images to be used
			my @listOfFiles = grep( /(.jpg)|(.jpeg)$/i, readdir $dirHandle ); 
			my $numBackgrounds = scalar( @listOfFiles );
			my $index = int( rand( $numBackgrounds ) );
			$result = "misc/" . $listOfFiles[$index];
			closedir( $dirHandle );
		}
	}
#	PMSLogging::DumpNote( "", "", "ComputeBackgroundImage(): return '$result'", 1);

	return $result;
} # end of ComputeBackgroundImage()


1;  # end of module

