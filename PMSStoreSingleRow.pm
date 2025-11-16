#!/usr/bin/perl -w
# PMSStoreSingleLine.pm - Store a single result row into our database.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMSStoreSingleRow;
#use lib 'PMSPerlModules';
use PMSConstants;
require PMSStruct;
require PMSUtil;

use strict;
use sigtrap;
use warnings;

## Only used in this file:
my %nextExpectedResultPlace;    # $nextExpectedResultPlace{gender:ageGrp/numSwim/category} - While processing results this is the next place 
								# we expect to see for the given gender,
                            	# age group, and race IN THE RESULTS.  If undefined then we haven't started processing that gender/age group/race.  
                            	# If -1 it means we found a place in the results
                            	# out of order and already reported it for that gender/age group/race.  Used in StoreResult() to 
                            	# catch cases where the results show places
                            	# out of order or (worse) missing places.
my $numNonPMSSwimmersInThisGroup;	# $numNonPMSSwimmersInThisGroup is the number of non-PMS
									# swimmers we've seen SO FAR in the current gender/age group / swim / category.  Used to
									# help us assign the correct place to PMS swimmers.
my $numNonBinarySwimmersInThisGroup;	# number of non-binary swimmers seen so far in the current eventGender/age group / swim / category.
my $numTiesSeenInResults;			# $numTiesSeenInResults is the number of consecutive ties we've seen in the results for
									# the current gender/age group / swim / category immediatly prior to the row
									# we are processing.  Used to
									# help us assign the correct place to PMS swimmers.
my $lastRecordedPlaceSeen;			# The recorded place of the previous row seen, 0 if we're looking at the first row
									# of a specific gender/age group / swim / category.
my $lastRecordedTimeSeen;			# The time, an integer representing hundredths of a second, of the previous swim duration seen
									# for this gender/age group. If we're looking at the first row of a specific 
									# gender/age group / swim / category this value will be 0 (since there is no previous swim).
									# NOTE: THIS IS ONLY USED FOR EVENTS FOR WHICH THE EVENT IS TIMED! If the distance is measured instead
									# then this is not used.
my $computedPMSPlace;				# we also need to track the next PMS swimmer's place. Normally this is the same as their place
									# in the results ($recordedPlace below if no errors in the results) but it won't be if non-PMS 
									# swimmers are placed faster than the PMS swimmer.


# StoreResult - store one result line (place, name, team, etc) along with gender/age
#
# PASSED:
#   $rowRef - reference to a row of results, containing the following:
    # $rowRef->[0] - gender:age group. Note that this is the "event gender", not necessarily the swimmer's gender.
    # $rowRef->[1] - place - must be non-empty
    # $rowRef->[2] - lastname - can be anything but must be non-empty
    # $rowRef->[3] - firstname - can be anything but must be non-empty
    # $rowRef->[4] - MI - can be anything and can be empty
    # $rowRef->[5] - team - can be anything
    # $rowRef->[6] - age - must be non-empty and all digits
    # $rowRef->[7] - reg # - can be anything but shouldn't be non-empty (but we'll handle it if it is)
    # $rowRef->[8] - DOB - can be anything 
    # $rowRef->[9] - time|distance - can be anything. If time, must be of the form 'hh:mm:ss[.tt]'.
#   $rowNum - row number of this row in the source file
#   $numSwims - the number of this swim for this category.  Swims are numbered starting at 1 throughout the
#		year. E.g. berryessa 1 mile cat 1 swim might be #4, 
#		and berryessa 2 mile cat 1 swim might be #5, and Del Valle 1k cat 2 swim might be #4 (if some
#		preceding events didn't have cat 2 results.)
#   $category - 1 or 2
#   $raceFileName - name of the file containing the results of this race
#	$eventId - Id of this event in our database.
#
# RETURNED:
#	n/a
#
sub StoreResult {
    my ($rowRef, $rowNum, $numSwims, $category, $raceFileName, $eventId) = @_;
   
    my $debugLastName = "xxxxxx";        # used to do isolated debugging of this function
    
    my $genAgeGrpRace = $rowRef->[0]  . "/" . $numSwims . "/" . $category;		# $genAgeGrpRace uniquely
    	#identifies a race (gender/age group + swim number + category)
    my $lastName = $rowRef->[2];
    my $firstName = $rowRef->[3];
    my $middleInitial = $rowRef->[4];
    my $age = $rowRef->[6];
    my $regNum = $rowRef->[7];		# regnum for swimmer on entry form
    my $dateOfBirth = $rowRef->[8];		# mm/dd/yyyy
    my $timeOrDistance = $rowRef->[9];
# assume $timeOrDistance is a time! fix this!
#xxx - if timeordistance has a ":" it's a time.  otherwise it's a distance.
	# is this a timed event (e.g. 200 free), or a distance event (e.g. 1 hour swim)?
	my $eventType = "timed";
	if( index( $timeOrDistance, ":" ) == -1 ) {
		$eventType = "distance";
	}
	my $timeInHundredths = 0;
	if( $eventType eq "timed" ) {
		$timeInHundredths = PMSUtil::GenerateCanonicalDurationForDB_v2( $timeOrDistance, 0, $rowRef, $rowNum, "" );
	}

    my $recordedPlace = $rowRef->[1];
    my $line = PMSUtil::ConvertArrayIntoString( $rowRef );
	my $sth, my $rv;
	my $foundSynomousPerson;

	# we track the "recordedPlace" (above $rowRef->[1] = the place of a row found in the results) and the 
	# "computedResultsPlace" (what we THINK the next place in the results should be). They should be
	# the same but sometimes the results have an error and they won't be. This is how we find that error.
	my $computedResultsPlace;
	# is this the first time we've seen a result in this gender/age group + swim number + category?
	if( !defined( $nextExpectedResultPlace{$genAgeGrpRace} ) ) {
        # this is the first swimmer in this race/gender/age group -  initialize:
        $nextExpectedResultPlace{$genAgeGrpRace} = 1;
        $numNonPMSSwimmersInThisGroup = 0;
        $numNonBinarySwimmersInThisGroup = 0;
        $numTiesSeenInResults = 0;
        $lastRecordedPlaceSeen = 0;
        $lastRecordedTimeSeen = 0;		# not used if $eventType == "distance"
        $computedPMSPlace = 1;
	}
#	PMSLogging::PrintLog( "", "", "PMSStoreSingleRow.pm: StoreResult(): rowNum=$rowNum, genAgeGrpRace='$genAgeGrpRace', " .
#		"nextExpectedResultPlace='$nextExpectedResultPlace{$genAgeGrpRace}'\n" .
#		"    row='$line'" );

	# get ready to use our database:
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();

	# get the age group (e.g. 25-29) from the gender:age group
    my $ageGrp = $rowRef->[0];
    $ageGrp =~ s/^.*://;
    
    # get the gender
    my $eventGender = $rowRef->[0];
    $eventGender =~ s/:.*//;

    if( ($lastName =~ m/$debugLastName/i)  ) {
    	print "PMSStoreSingleRow::StoreResult(): got $debugLastName: genAgeGrpRace='$genAgeGrpRace', regnum=$regNum\n";
    }
    
    # convert team to UPPERCASE so users of this value downstream don't have to worry about it
    my $team = uc( $rowRef->[5] );
    # remove trailing '-PC' (or '-whatever')
    $team =~ s/\s*-.*$//;
    
    # Generate "cononocal" names for a person: remove quotes, commas, etc; remove leading/trailing space; make the 
    # initial one letter.
    my ($lastNamePreSynonym, $firstNamePreSynonym, $middleInitialPreSynonym) = 
    	PMSUtil::GenerateCanonicalNames( $lastName, $firstName, $middleInitial );

    if( ($lastName =~ m/$debugLastName/i)  ) {
    	print "PMSStoreSingleRow::StoreResult(): got Canonical of $debugLastName: last,first,middle = $lastName, $firstName, $middleInitial\n";
    }

    # Now see if the names we just computed (above) should actually be replaced with different names.
    # This uses the ">last,first" property
    ($lastName, $firstName, $middleInitial, $foundSynomousPerson) = 
    	FindSynonomousPerson( $lastNamePreSynonym, $firstNamePreSynonym, $middleInitialPreSynonym );

    if( ($lastName =~ m/$debugLastName/i)  ) {
    	print "PMSStoreSingleRow::StoreResult(): after FindSynonomousPerson(): " .
    	"last,first,middle,foundSynomousPerson = " .
    	"$lastName, $firstName, $middleInitial, $foundSynomousPerson\n";
    }

    # remember this swimmer's regNum and dateOfBirth (if either is empty or all spaces we'll set to 
	# a default, invalid value):
    $regNum =~ s/\s*//g;
	$regNum = PMSUtil::GenerateCanonicalRegNum( $regNum );
	# now that we're happy with the regnum we're going to validate it:
	$regNum = PMSUtil::ValidateAndCorrectSwimmerId( $regNum, "PMSStoreSingleRow::StoreResult(): ($firstName " .
		"$middleInitial $lastName): ", PMSStruct::GetMacrosRef()->{"YearBeingProcessed"} );
    $dateOfBirth =~ s/\s*//g;		# remove all whitespace

	# Now see if the regnum we just got (above) should actually be replaced with a different regnum.
	# This uses the ">regnumName" property
    if( ($lastName =~ m/$debugLastName/i)  ) {
    	print "PMSStoreSingleRow::StoreResult(): for $debugLastName: GenerateCanonicalRegNum() returned '$regNum'\n";
    }
    my ($newRegNum, $foundSynomousRegNum) = FindSynonomousRegNumName( $regNum, $firstName, $lastName, $middleInitial );
    if( ($lastName =~ m/$debugLastName/i)  ) {
    	print "PMSStoreSingleRow::StoreResult(): for $debugLastName: after FindSynonomousRegNumName() returned $newRegNum, $foundSynomousRegNum\n";
    }
	if( $foundSynomousRegNum ) {
		PMSLogging::DumpNote( "", 0, "PMSStoreSingleRow::StoreResult(): " .
			"Found synomous RegNumName: $regNum replaced with $newRegNum for '$firstName', " .
			"'$lastName', '$middleInitial'.", 0 );
		$regNum = $newRegNum;
	} else {
		# regnum for a specific person didn't have a synonym; see of a general regnum synonym exists
	    ($newRegNum, $foundSynomousRegNum) = FindSynonomousRegNum( $regNum );
		if( $foundSynomousRegNum ) {
			PMSLogging::DumpNote( "", 0, "PMSStoreSingleRow::StoreResult(): " .
				"Found synomous regNum: $regNum replaced with $newRegNum", 0 );
			$regNum = $newRegNum;
		}
	}

	# we're ready to store this swimmer into our results database. Everyone gets put into the DB, 
	# even if they are not a PMS swimmer.
    if( ($lastName =~ m/$debugLastName/i)  ) {
    	print "PMSStoreSingleRow::StoreResult(): before InsertSwimmerIntoMySqlDB: regnum=$regNum, team='$team'\n";
    }
	(my $swimmerId, my $correctedRegNum, my $isPMS, my $correctedTeam, my $correctedGender) = PMS_MySqlSupport::InsertSwimmerIntoMySqlDB( 
		$dateOfBirth, $regNum, $firstName, $middleInitial,
		$lastName, $eventGender, $age, $ageGrp, $genAgeGrpRace, $raceFileName, $team, $eventId,
		$recordedPlace, $rowNum );

	# 12jun2024: we need to remember this swimmer's team IF the results show no team but we actually know the team from the
	# USMS database (which we query in InsertSwimmerIntoMySqlDB() above.) We want this for generated human readable results,
	# so even though this is tacky we're going to update the passed $rowRef[] with the correct team if necessary:
	if( $team eq "" ) {
		$rowRef->[5] = $correctedTeam;
		$team = $rowRef->[5];
	}
    if( ($lastName =~ m/$debugLastName/i)  ) {
    	print "PMSStoreSingleRow::StoreResult(): after InsertSwimmerIntoMySqlDB: swimmerid=$swimmerId, correctedRegNum=$correctedRegNum, " .
    		"ispms=$isPMS, team='$team'\n";
    }

	# This is a bit tricky:  the results will sometimes:
	# - list the results in the incorrect order (2nd place listed before 1st place.)  THIS IS AN ERROR
	# - Or perhaps there is a tie so there will be two 1st places and no 2nd place.  This is OK
	# - Or two 1st places and a 2nd place!  THIS IS AN ERROR
	# - Or two 1st places with different times/distances! THIS IS AN ERROR
	# - Or a non-pms swimmer finished faster than this swimmer, so this swimmer (and those following) should
	# 	slide up in the results. This is OK
	# We're going to catch all of these cases EXCEPT the non-PMS case (that's handled later below) and
	# do our best, reporting all these situations so we can confirm that they are right.
	
	# We will keep track of the place we expect the next swimmer to take (sometimes we're wrong which
	# is OK [e.g. there was a valid tie] and sometimes we're wrong due to an error in the results.)

    # we will compute the place ourselves, temporarily ignoring the place in the results
    $computedResultsPlace = $nextExpectedResultPlace{$genAgeGrpRace};
    if( $computedResultsPlace > 0 ) {
    	# so far all the places are in order, so we're going to keep checking for this gender/age group.
    	# Compute the next place for the swimmer following the swimmer we're processing now:
    	$nextExpectedResultPlace{$genAgeGrpRace}++;
    
    	# is the place of the previous swimmer seen the same as the place for the swimmer we're processing now?
		if( $lastRecordedPlaceSeen == $recordedPlace ) {
			# YES!  That means that there was a tie in the results.
			# Since there could be multiple ties in a row we need to consider that when computing what we think
			# the current place should be in the results:
			$computedResultsPlace = $computedResultsPlace - 1 - $numTiesSeenInResults;
			PMSLogging::DumpNote( $line, $rowNum, "PMSStoreSingleRow::StoreResult(): " .
				"Found a tie (swimmer $swimmerId) in file $raceFileName." . 
				"computedResultsPlace=$computedResultsPlace, lastRecordedPlaceSeen=$lastRecordedPlaceSeen", 1 );
			$numTiesSeenInResults++;
		} elsif( $recordedPlace == $computedResultsPlace ) {
			# the place we see on this row is what we expect
			$numTiesSeenInResults = 0;
		} else {
			# something not right...
			PMSLogging::DumpFatalError( $line, $rowNum, "Found a major discrepancy between recordedPlace (" .
				$recordedPlace . ") and computedResultsPlace ($computedResultsPlace) for $firstName $lastName.\n" .
				"  genAgeGrpRace='$genAgeGrpRace' (unique race identifier: Gender:age group/Event number/Category),\n" .
				"    numNonPMSSwimmersInThisGroup (so far)=$numNonPMSSwimmersInThisGroup, " .
				"	 numNonBinarySwimmersInThisGroup (so far)=$numNonBinarySwimmersInThisGroup, " .
				"    numTiesSeenInResults (so far)=$numTiesSeenInResults,\n" .
				"    lastRecordedPlaceSeen='$lastRecordedPlaceSeen', Swimmer's internal ID=$swimmerId,\n" .
				"    filename='$raceFileName'.\n" .
				"    {PMSStoreSingleRow.pm::StoreResult()}", 
				1 );
			$numTiesSeenInResults = 0;
			# 13jun2024: don't continue checking places for the remaining swims in this gender/age group. We're already
			# off so all the following swimmers will be off, too (most likely...)
			$nextExpectedResultPlace{$genAgeGrpRace} = -1;
		}
		$lastRecordedPlaceSeen = $recordedPlace;
	} # end of if( $computedResultsPlace > 0...
    
    if( $eventType eq "timed" ) {
		# now we're going to look for a reasonable swim time, in the sense that it must be slower than the previous
		# swimmer's time, or the same if it was a tie.  Del Valle 10k, cat1 2024.
		if( $numTiesSeenInResults ) {
			# this swimmer tied the previous swimmer.
			if( $timeInHundredths != $lastRecordedTimeSeen ) {
				my $currentTime = PMSUtil::GenerateDurationStringFromHundredths( $timeInHundredths );
				my $previousTime = PMSUtil::GenerateDurationStringFromHundredths( $lastRecordedTimeSeen );
				PMSLogging::DumpFatalError( $line, $rowNum, "Found a major discrepancy with swimmer times: the results\n" .
					"    show that this swimmer and the previous swimmer tied\n" .
					"    (placed the same), but their times are DIFFERENT! The previous swimmer's time was $previousTime " .
					"    and this swimmer's time was $currentTime.\n" .
					"	genAgeGrpRace='$genAgeGrpRace' (unique race identifier: Gender:age group/Event number/Category),\n" .
					"	numNonPMSSwimmersInThisGroup (so far)=$numNonPMSSwimmersInThisGroup, " .
					"	 numNonBinarySwimmersInThisGroup (so far)=$numNonBinarySwimmersInThisGroup, " .
					"	numTiesSeenInResults (so far)=$numTiesSeenInResults,\n" .
					"	filename='$raceFileName'.\n" .
					"	{PMSStoreSingleRow.pm::StoreResult()}", 
					1 );
			}
		} else {
			# According to the results this swimmer didn't tie the previous Swimmer, but that might not be right based on
			# the swim durations!
			# is the time in the results < the time of previous swimmer?
			if( $timeInHundredths < $lastRecordedTimeSeen ) {
				# YES!
				# this swimmer placed slower than the previous swimmer, but their time is faster!
				my $currentTime = PMSUtil::GenerateDurationStringFromHundredths( $timeInHundredths );
				my $previousTime = PMSUtil::GenerateDurationStringFromHundredths( $lastRecordedTimeSeen );
				PMSLogging::DumpFatalError( $line, $rowNum, "Found a major discrepancy with swimmer times: the results\n" .
					"    show that this swimmer's time ($currentTime) was FASTER than the\n" .
					"    previous swimmer's time ($previousTime), but the faster swimmer was placed slower!\n" .
					"	genAgeGrpRace='$genAgeGrpRace' (unique race identifier: Gender:age group/Event number/Category),\n" .
					"	numNonPMSSwimmersInThisGroup (so far)=$numNonPMSSwimmersInThisGroup, " .
					"	 numNonBinarySwimmersInThisGroup (so far)=$numNonBinarySwimmersInThisGroup, " .
					"	numTiesSeenInResults (so far)=$numTiesSeenInResults,\n" .
					"	filename='$raceFileName'.\n" .
					"	{PMSStoreSingleRow.pm::StoreResult()}", 
					1 );
			} elsif( $timeInHundredths == $lastRecordedTimeSeen ) {
				# the time in the results = the time of previous swimmer, but they were not placed as ties!
				my $currentTime = PMSUtil::GenerateDurationStringFromHundredths( $timeInHundredths );
				my $previousTime = PMSUtil::GenerateDurationStringFromHundredths( $lastRecordedTimeSeen );
				PMSLogging::DumpFatalError( $line, $rowNum, "Found a major discrepancy with swimmer times: The previous\n" .
					"    swimmer's time ($previousTime) and this swimmer's time ($currentTime)\n" .
					"    ARE THE SAME, yet they are not placed in a tie.\n" .
					"	genAgeGrpRace='$genAgeGrpRace' (unique race identifier: Gender:age group/Event number/Category),\n" .
					"	numNonPMSSwimmersInThisGroup (so far)=$numNonPMSSwimmersInThisGroup, " .
					"	 numNonBinarySwimmersInThisGroup (so far)=$numNonBinarySwimmersInThisGroup, " .
					"	numTiesSeenInResults (so far)=$numTiesSeenInResults,\n" .
					"	filename='$raceFileName'.\n" .
					"	{PMSStoreSingleRow.pm::StoreResult()}", 
					1 );    	
			}
	
		}
		# done checking for a reasonable swim time
		$lastRecordedTimeSeen = $timeInHundredths;
	} # end of if( $eventType eq "timed"....
    
    
    # Next, handle the case where this swimmer swam slower than 1 or more non-PMS swimmers, thus we 
    # need to adjust their place in the Swim table to slide them up in place, logically removing
    # the non-PMS swimmers.
    # NOTE: in the distance case we only see PMS swimmers here.
	if( $isPMS ) {
		# 14Nov2025: We will NOT award points to those with a non-binary gender.  See
		# https://www.usms.org/volunteer-central/policy-and-governance/usms-policies/interim-eligibility-policy
		# if this is the case we let others slide up in points.
		if( index( $PMSConstants::BinaryGenders, $correctedGender ) >= 0 ) {
			PMS_MySqlSupport::AddSwim( $eventId, $swimmerId, $timeOrDistance, $recordedPlace, $computedPMSPlace,
				$rowRef, $rowNum );
			$computedPMSPlace++;
		} else {
			# non-binary gender. Skip their points and assign to the next slowest swimmer.
			PMS_MySqlSupport::AddSwim( $eventId, $swimmerId, $timeOrDistance, $recordedPlace, -10,
				$rowRef, $rowNum );
			$numNonBinarySwimmersInThisGroup++;
		}
	} else {
		PMS_MySqlSupport::AddSwim( $eventId, $swimmerId, $timeOrDistance, $recordedPlace, -10,
			$rowRef, $rowNum );
	    $numNonPMSSwimmersInThisGroup++;
	}
   
} # end of StoreResult()





# FindSynonomousPerson - find a different name for the passed name
#
# PASSED:
#	$lastName
#	$firstName
#	$middleInitial
#
# RETURNED:
#	The new or same lastName, firstName, and middleInitial
#
# NOTES:
#   Using the passed last, first, and middle names, see if there is a ">last,first" property that specifies a 
#	different last, first, name names.
#   If such a property exists, return the synonym of this person.
#   Example:  pass in "Upshaw,Rob", and we have the property ">last,first   Upshaw,Rob >  Upshaw,Bob".  
#	This function will return "Upshaw,Bob" everytime.
#
# 	$middleInitial is always defined but will be an empty string if there is no middle initial.
#
# Returned:
#	The new or same lastName, firstName, and middleInitial.
#	$synonymFound:  true if the returned first, last, and/or middle names are different from the 
#		passed ones.
#   
sub FindSynonomousPerson {
	my ($lastName, $firstName, $middleInitial) = @_;
    my $lastFirst = lc("$lastName,$firstName");
    if( $middleInitial ne "" ) {
    	$lastFirst .= lc(",$middleInitial");
    }
    my $synonym = PMSStruct::GetSynonymFirstLastName($lastFirst);
    my $synonymFound = 0;
	if( defined( $synonym ) ) {
		# synonym:  Last,First[,Extra]
		$lastName = $firstName = $synonym;
		$lastName =~ s/,.*$//;
		$firstName =~ s/^[^,]*,//;
		$firstName =~ s/,.*$//;			# will fail if no Extra
		my $mid = $synonym;
		$mid =~ s/^.*,.*,//;
		if( $mid ne $synonym ) {
			$middleInitial = $mid;
		}
		$synonymFound = 1;
	}
    
    return ($lastName, $firstName, $middleInitial, $synonymFound);

} # end of FindSynonomousPerson




# FindSynonomousRegNum - find a different regnum for the passed one
#
# PASSED:
#	$regNum - the passed regNum.
#
# RETURNED:
#	The new or same regnum.
#	$synonymFound: true if the returned regnum is different from the 
#		passed one.
#
# NOTES:
#   Using the passed regnum, see if there is a ">regnum" property that specifies a 
#	different regnum.
#   If such a property exists, return the synonym regnum for this person.
#
sub FindSynonomousRegNum($) {
	my ($regNum) = @_;
    my $synonymFound = 0;
    my $synonym = PMSStruct::GetSynonymRegNum($regNum);

	if( defined( $synonym ) ) {
		$regNum = $synonym;
		$synonymFound = 1;
	}
    
    return ($regNum, $synonymFound);
} # end of FindSynonomousRegNum




# FindSynonomousRegNumName - find a different regnum for the passed one ONLY WHEN the passed name
#	matches the corresponding different regnum's name.
#
# PASSED:
#	$regNum
#	$firstName
#	$lastName
#	$middle
#
# RETURNED:
#	The new or same regnum.
#	$synonymFound: true if the returned regnum is different from the 
#		passed one.
#
# NOTES:
# Same idea as FindSynonomousRegNum except the passed first, last, and middle must match the
# >regnumName property along with the passed $regNum
#
sub FindSynonomousRegNumName($$$$) {
	my ($regNum, $first, $last, $middle) = @_;
    my $synonymFound = 0;
    my $synonym = PMSStruct::GetSynonymRegNum($regNum, $first, $last, $middle);

	if( defined( $synonym ) ) {
		$regNum = $synonym;
		$synonymFound = 1;
	}
    
    return ($regNum, $synonymFound);
} # end of FindSynonomousRegNumName





1;  # end of module
