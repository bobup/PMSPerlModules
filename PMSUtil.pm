#!/usr/bin/perl -w
# PMSUtil.pm - various utility routines used by the OW processing programs.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMSUtil;
use strict;

#use lib 'PMSPerlModules';
require PMSLogging;
require PMSConstants;
require Devel::StackTrace;



# trim - remove all leading and trailing white-space from the passed string.
#
sub trim($) {
	my $str = $_[0];
	$str = "" if( !defined $str );
	$str =~ s/^\s*//;
	$str =~ s/\s*$//;
	return $str;
} # end of trim()



# GenerateCanonicalDurationForDB_v2 - convert the passed text representation of a time duration into
#	an integer representing the duration in hundredths of a second.
#
# PASSED:
#	$passedDuration - the duration in text form, e.g. 1:03:33.09 (1 hour, 3 minutes, 33 seconds, 9 hundredths
#		of a second)
#	$passedDistance - the distance of the event that this duration is used for, in meters/yards, or 0.
#		If 0 then it, and the logic involved, is ignored.
#		We use this to get an idea of a "valid duration"
#		so we can do some error detection and error correction.  For example, if the event is a 1 mile
#		swim (1760 yards) and the duration is "23.33" we know that it's not 23 seconds and 33 hundredths!  
#		So we'll log an error and try to adjust the passed duration so that it makes sense.  Even if
#		we can't make sense of the passed duration we'll return a valid duration.
#		NOTE: if 0 or undefined then we will probably make false assumptions and generate
#		bogus errors which you can ignore if you dare...
#	$rowRef - reference to the result row.  Used in log messages only, so it can be "" if unknown.
#	$rowNum - the number of the result row in the result file.  Used in log messages only, 
#		so it can be "" if unknown.
#	$extraMsg - (optional) an extra string appended to log messages.  If not supplied then "" is assumed.
#
# RETURNED:
#	$returnedDuration - the equivalent duration as an integer in hundredths of a second.
#
# NOTES:
#	It's common for the duration in a result file to be formatted wrong, so we try to handle durations
#	that do not match the above specification.	
# 
# 	Possible formats:
#	- THE CORRECT FORMAT:  hh:mm:ss.tt (e.g. 0:19:51.50) - 19*60*100 + 51*100 + 50
# 	- . (dot) or comma or semicolons in place of colons - replace them with colons
#	- ss.tt - also easily confused with hh:mm and mm:ss, except if a '.' is used assume ss.tt
#	- hh:mm  mm:ss - ambiguious...assume mm:ss and convert to 00:mm:ss
#	- mm:ss.tt - assume 0:mm:ss.tt
#	- use the event distance to make an intelligent guess if we have to.
#
sub GenerateCanonicalDurationForDB_v2($$$$) {
	my ($passedDuration, $passedDistance, $rowRef, $rowNum, $extraMsg) = @_;
	if( !defined $extraMsg ) {
		$extraMsg = "";
	} else {
		$extraMsg = "\n    ($extraMsg)";
	}
	my $convertedTime = $passedDuration;
	
	if( !defined $convertedTime ) {
		PMSLogging::DumpRowError( $rowRef, $rowNum, 
			"PMSUtil::GenerateCanonicalDurationForDB_v2(): undefined time in GenerateCanonicalDurationForDB_v2 " .
			"- use \"9:59:59.00\"$extraMsg\n", 1 );
		$convertedTime = "9:59:59.00";
	}
	my $returnedDuration = 0;
	$convertedTime =~ s/^\s+//;
	$convertedTime =~ s/\s+$//;
	my( $hr, $min, $sec, $hundredths );
	if( $convertedTime =~ m/^\d+\.\d+$/ ) {
		$convertedTime = "0:0:$convertedTime";
	}elsif( $convertedTime =~ m/^\d+[.,;:]\d+[:]\d+$/ ) {
		# e.g. 1[:;,.]34:12 = 1:34:12 = 1 hour, 34 minutes, 12 seconds:  this is fine, but add the tenths
		$convertedTime =~ s/[.,;]/:/g;		# use only ':' for separaters
		$convertedTime = "$convertedTime.00";
	} elsif( $convertedTime =~ m/^\d+[,;:]\d+$/ ) {
		# e.g. 23[:;,]45 = 23:45 is ambiguious, assume 0:23:45, which is 0 hours, 23 minutes, 45 seconds
		$convertedTime =~ s/[.,;]/:/g;		# use only ':' for separaters
		$convertedTime = "00:$convertedTime.00";
	} elsif( $convertedTime =~ m/^(\d+)[.,;:](\d+).(\d+)$/ ) {
		# e.g. 34[;:,.]45.95 = 34:45.95 = mm:ss.tt - add the hour
        $hr = "00";
        $min = $1;
        $sec = $2;
        $hundredths = $3;
        # convert ".5" to ".50"
        $hundredths .= "0" if( length( $hundredths ) == 1 );
        $convertedTime = "$hr:$min:$sec.$hundredths";
	} elsif( $convertedTime =~ m/^(\d+)[.,;:](\d+)[.,;:](\d+)[.,;:](\d+)$/ ) {
        # e.g. 3[;:,.]40[;:,.]50[;:,.]45 = 3:40:50.45 (or hh:mm:ss.tt which is standard)
        $hr = $1;
        $min = $2;
        $sec = $3;
        $hundredths = $4;
        # be sure the .tt is 2 digits 
        $hundredths .= "0" if( length( $hundredths ) == 1 );
        $convertedTime = "$hr:$min:$sec.$hundredths";
	} elsif( $convertedTime =~ m/^[.,;:]*(\d+)[.,;:]*$/ ) {
		# the above pattern assumes  mm  (or .mm. or something like that)
		$hr = "00";
		$min = $1;
		$sec = "00";
		$hundredths = "00";
        $convertedTime = "$hr:$min:$sec.$hundredths";
	} else {
		PMSLogging::DumpRowError( $rowRef, $rowNum, "PMSUtil::GenerateCanonicalDurationForDB_v2():invalid time " .
			"in GenerateCanonicalDurationForDB_v2: '$passedDuration'$extraMsg", 1 );		
		$convertedTime = "";
	}
	if( $convertedTime ne "" ) {
		# convert the duration to an integer
		$convertedTime =~ m/^(\d+):(\d+):(\d+).(\d+)$/;
        $hr = $1;
        $min = $2;
        $sec = $3;
        $hundredths = $4;
		$returnedDuration = $hr*60*60*100 + $min*60*100 + $sec*100 + $hundredths;
	}
	my $strReturnedDuration = GenerateDurationStringFromHundredths( $returnedDuration ) .
		" ($returnedDuration)";
	
	if( $passedDistance > 0 ) {
		# now check to see if this duration makes sense
		# what are the maximum and minimum duration (hundredths) for the passed distance?
		my $minDuration = (100*$passedDistance*18)/50;		# 18 seconds per 50 - pretty fast!
		my $strMinDuration = GenerateDurationStringFromHundredths( $minDuration ) . " ($minDuration)";
		my $maxDuration = (100*$passedDistance*150)/50;	# 2.5 minutes per 50 -  pretty slow!
		my $strMaxDuration = GenerateDurationStringFromHundredths( $maxDuration ) . " ($maxDuration)";

		#### 13Dec2021: Wouldn't you know it...??? Today I found a valid time that was really slow (1/2 mile keller cove)
		# belonging to an "older woman" (bless her heart - I'm almost her age! Damn!) that got flagged below as "too slow", and the
		# code tried to convert it to a "reasonable" time, which was wrong and caught as TOO FAST!  So we're going
		# to adjust this heuristic to be a bit more "understanding".
		
		# If you get the same kind of problem again adjust this as necessary...
		my $superMaxDuration = (200*$passedDistance*150)/50;	# 5 minutes per 50 -  really slow!
		my $strSuperMaxDuration = GenerateDurationStringFromHundredths( $superMaxDuration ) . " ($superMaxDuration)";

		# we need to remember whether or not we change the returnedDuration in the following heuristic:
		my $weChangedReturnedDuration = 0;
		
		if( $returnedDuration < $minDuration ) {
			my $updatedReturnedDuration = $returnedDuration + 60*60*100;		# add an hour
			my $strUpdatedReturnedDuration = GenerateDurationStringFromHundredths( $updatedReturnedDuration );
			PMSLogging::DumpRowError( $rowRef, $rowNum, "PMSUtil::GenerateCanonicalDurationForDB_v2():computed " .
				"duration $strReturnedDuration is much too FAST!\n      (expected a time of at least $strMinDuration), " .
				"distance of swim='$passedDistance'\n      CHANGED TO $updatedReturnedDuration " .
				"($strUpdatedReturnedDuration)$extraMsg", 1 );
			$returnedDuration = $updatedReturnedDuration;
			$weChangedReturnedDuration = 1;
		} elsif( $returnedDuration > $maxDuration ) {
			if(  $returnedDuration < $superMaxDuration ) {
				PMSLogging::DumpRowWarning( $rowRef, $rowNum, "PMSUtil::GenerateCanonicalDurationForDB_v2():computed " .
					"duration $strReturnedDuration is pretty slow but we're going to go with it.\n" .
					"      (expected a time of no more than $strMaxDuration) " .
					"distance of swim='$passedDistance'\n       $extraMsg" );
			} else {
				my $updatedReturnedDuration = $hr*60*100 + $min*100 + $sec;		# hr becomes minutes, minutes become seconds, etc...
				my $strUpdatedReturnedDuration = GenerateDurationStringFromHundredths( $updatedReturnedDuration );
				PMSLogging::DumpRowError( $rowRef, $rowNum, "PMSUtil::GenerateCanonicalDurationForDB_v2():computed " .
					"duration $strReturnedDuration is much too SLOW!\n      (expected a time of no more than $strSuperMaxDuration) " .
					"distance of swim='$passedDistance'\n      CHANGED TO $updatedReturnedDuration " .
					"($strUpdatedReturnedDuration)$extraMsg", 1 );
				$returnedDuration = $updatedReturnedDuration;
				$weChangedReturnedDuration = 1;
			}
		}
		
		# now check to see if this duration makes sense
		if( $weChangedReturnedDuration ) {
			if( $returnedDuration < $minDuration ) {
				PMSLogging::DumpRowError( $rowRef, $rowNum, "PMSUtil::GenerateCanonicalDurationForDB_v2():computed " .
					"duration is much too FAST! AGAIN!\n      (expected a time of at least $strMinDuration) " .
					"  Duration=$strReturnedDuration, " .
					"distance of swim='$passedDistance'$extraMsg", 1 );
			} elsif( $returnedDuration > $maxDuration ) {
				PMSLogging::DumpRowError( $rowRef, $rowNum, "PMSUtil::GenerateCanonicalDurationForDB_v2():computed " .
					"duration is much too SLOW! AGAIN!\n      (expected a time of no more than $strMaxDuration) " .
					"  Duration=$strReturnedDuration, " .
					"distance of swim='$passedDistance'$extraMsg", 1 );
			}
		}
	}
	return $returnedDuration;
} # end of GenerateCanonicalDurationForDB_v2()



# GenerateDurationStringFromHundredths - basically the opposite of GenerateCanonicalDurationForDB_v2()
#
# PASSED:
#	$hundredths - a duration as an integer representing hundredths of seconds
#
# RETURNED:
#	$duration - a string of the form hh:mm:ss.tt
#
# NOTES:
#	Assume that the returned time will be at least 10 seconds, thus will always
#	be of the form [[hh:]mm:]ss.tt
#	where:
#		hh: is missing if hours is 0, and is a hh is a single digit if hours < 10
#		mm: is missing if hours is 0 and minutes is 0, and mm is a single digit if
#			hours is 0 and mm < 10
#
#	If the passed $hundredths is undefined (or equal to "(undef)") an error will be logged
#	and a bogus value will be assumed so the program can go on.
#
sub GenerateDurationStringFromHundredths( $ ) {
	my $hundredths = $_[0];
	my $duration;
	my( $hr, $min, $sec );
	
	if(  (!defined $hundredths) || ($hundredths eq "(undef)") ) {
		PMSLogging::PrintLog( "", "", "PMSUtil::GenerateDurationStringFromHundredths(): " .
			"undefined value.  Stack trace:\n" . GetStackTrace() );
		$hundredths = 9999;
	}
	
	$hr = int($hundredths / (60*60*100));
	$hundredths -= $hr*(60*60*100);
	$min = int($hundredths / (60*100));
	$hundredths -= $min*(60*100);
	$sec = int($hundredths / 100);
	$hundredths -= $sec*100;
	# don't show hours if there are 0 of them
	if( $hr == 0 ) {
		# don't show minutes if there are 0 of them
		if( $min == 0 ) {
			$duration = sprintf( "%02d.%02.2s", $sec, $hundredths );
		} else {
			# show minutes but not hours, with no leading '0' for minutes if < 10
			$duration = sprintf( "%d:%02d.%02.2s", $min, $sec, $hundredths );
		}
	} else {
		# show hours, minutes, seconds, and hundredths, with no leading '0' for hours if < 10
		$duration = sprintf( "%d:%02d:%02d.%02.2s", $hr, $min, $sec, $hundredths );
	}
	return $duration;	
} # end of GenerateDurationStringFromHundredths()



# GenerateCanonicalDOB - convert the passed birth date into a canonical form:
#
# PASSED:
#	$dateOfBirth - a string of the form m[m]/d[d]/[yy]yy or some recognizable variation, or an empty string or
#		undefined value.
#
# RETURNED:
#	$dateOfBirth - the date in a "canonical" form as defined by ConvertDateToISO(), which is "yyyy-mm-dd".
#		If the passed $dateOfBirth isn't something we can decipher then we'll return a properly
#		formed but invalid date (i.e. $PMSConstants::INVALID_DOB)
#
# NOTES:
#	IOW, convert m[m]/d[d]/yyyy into yyyy-mm-dd, etc.
#
sub GenerateCanonicalDOB($) {
	my $dateOfBirth = $_[0];
	if( (!defined($dateOfBirth)) || ($dateOfBirth eq "") ) {
		$dateOfBirth = $PMSConstants::INVALID_DOB;		# invalid date
		$dateOfBirth = $PMSConstants::INVALID_DOB;		# avoid compiler warning
	} else {
		$dateOfBirth = ConvertDateToISO( $dateOfBirth );		# convert into yyyy-mm-dd
	}
	return $dateOfBirth;
} # end of GenerateCanonicalDOB()




#!todo   handle passed date as empty string!

# ConvertDateToISO( $passedDate ) -  convert m[m]/d[d]/[yy]yy (or something similar) 
#	into yyyy-mm-dd IF NECESSARY.
# 
# PASSED:
#	$passedDate - a string of the form m[m]/d[d]/[yy]yy or some recognizable variation, OR
#		a string of the form yyyy/mm/dd (which is already ISO format)
#
# RETURNED:
#	a date in the form yyyy-mm-dd
#
# NOTES:
#	- if we are passed '-' instead of '/' then deal with it.
#	- if we can't parse the date assume a default after displaying an error message.
#
my $twoDigitYearSeen = 0;		# set to 1 if we see a 2 digit year so we don't report it a billion times
sub ConvertDateToISO( $ ) {
	my $passedDate = $_[0];
	my $isoDate;
	$passedDate =~ m,^([^-/]+)[-/]+([^-/]+)[-/]+(.*)$,;
	my $month = $1;
	my $day = $2;
	my $year = $3;
	# see if the passed date was already in ISO format:
	if( length( $month ) == 4 ) {
		# oops - bad assumption.  Swam the numbers around...
		my $tmp = $year;
		$year = $month;
		$month = $day;
		$day = $tmp;
		$isoDate = "$year-$month-$day";
	} else {
		# make sure the passed date is the correct format.  If not we'll us a default.
		if( !defined $month ) {
			my $xxx = $PMSConstants::debug;		# avoid compiler error below
			PMSLogging::DumpError( "", "", "PMSUtil::ConvertDateToISO(): invalid date ('$passedDate') so we couldn't " .
				"figure out the date's components.  Assume default date ('$PMSConstants::INVALID_DOB').",
				"" ) if( $PMSConstants::debug > 0 );
			$isoDate = $PMSConstants::INVALID_DOB;		# of the form 1900-01-01
		} else {
			$isoDate = ConvertToISOPrimary( $year, $month, $day, $passedDate );
		}
	}
	
	return $isoDate;
	} # end of ConvertDateToISO()
	
		
# ValidateISODate( $passedDate ) -  confirm that the passed date is of the form yyyy-mm-dd
# 
# PASSED:
#	$passedDate - a string of the form yyyy/m[m]/d[d]
#
# RETURNED:
#	1 if the passed date is an ISO date, 0 otherwise.
#
# NOTES:
#	- if we are passed '-' instead of '/' then deal with it.
#	- if we can't parse the date return 0
#
sub ValidateISODate( $ ) {
	my $passedDate = $_[0];
	my $result = 0;
	$passedDate =~ m,^([^-/]+)[-/]+([^-/]+)[-/]+(.*)$,;
	my $year = $1;
	my $month = $2;
	my $day = $3;

	# make sure the passed date is the correct format. 
	if( defined $month ) {
		my $isoDate = ConvertToISOPrimary( $year, $month, $day, $passedDate );
		if( $passedDate eq $isoDate ) {
			$result = 1;
		}
	}
	
	return $result;
	} # end of ValidateISODate()
	
		
		
		
sub ConvertToISOPrimary( $$$$ ) {
	my ($year, $month, $day, $passedDate ) = @_;
	my $yearBeingProcessed;

	$month = "0$month" if( length( $month ) < 2 );
	if( ($month > 12) || ($month < 1) ) {
		PMSLogging::DumpError( "", "", "PMSUtil::ConvertToISOPrimary(): invalid date ('$passedDate' - '$month' is an invalid month). " .
			"Changing to month '01'.  This needs to be corrected.", 1 );
my $trace = Devel::StackTrace->new;
print $trace->as_string; # like carp
		$month = "01";
	}
	$day = "0$day" if( length( $day ) < 2 );
	# validate the day
	my @mdays = (0,31,28,31,30,31,30,31,31,30,31,30,31);
	##### Leap year conditions
	if ($month == 2) {
		if ($year % 4 != 0) { $mdays[2] = 28; }
		elsif ($year % 400 == 0) { $mdays[2] = 29; }
		elsif ($year % 100 == 0) { $mdays[2] = 28; }
		else { $mdays[2] = 29; }
	}		
	if( ($day<1) || ($day>$mdays[$month]) ) {
		PMSLogging::DumpError( "", "", "PMSUtil::ConvertToISOPrimary(): invalid date ('$passedDate' - invalid day). " .
			"Changing to day '01'.  This needs to be corrected.", "" );
		$day = "01";
	}		
	if( length($year) < 3 ) {
		# a two digit year ... God, will they never learn???
		$yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};
		my $twoDigitYear = $year;
		my ($sec,$min,$hour,$mday,$mon,$currentYear,$wday,$yday,$isdst) = localtime();
		$year += 2000;		# convert '83' to '2083', or '02' to '2002'
		# Here is the problem:  Pretend the current year is 2019.  Above we convert the year
		# '53' to 2053 which is probably a bad assumption (especially if we're talking
		# about a birthdate.)  So since we have to assume something in order to come up with 
		# a reasonable date we'll assume a date is not too far in the past (e.g. '01' is 
		# probably not 1901 if it's a birthdate) and not in the future (i.e. not beyond
		# the current year, 2019 in this example.)  Note that most dates we're dealing with are
		# birthdates and dates of swims and registraton dates.  Trouble is:  what to do with 
		# '01'?  2001 is a reasonable birthdate of a masters swimmer, and 1901 is unlikely, so
		# the better assumption would be 2001.  What about '04'?  2004 is not a valid 
		# birthdate for a masters swimmer, so we have to assume 1904?  Not if it's the date
		# of a swim record.  So the code below just punts; the better solution is to
		# insist that data we process uses 4 digit years everywhere, and for our sake we
		# pass along the "meaning" of the date (e.g. birthdate or event date) to our date 
		# handling code so we can make more intelligent decisions.
		# ANOTHER PROBLEM:  depending on how this routine is called, it's possible we 
		# don't know the year being processed.  In that case we will leave it as a 2 year date.
		if( defined $yearBeingProcessed ) {
			if( $year > $yearBeingProcessed ) {
				# oops - this can't be right!  try again...
				$year = $twoDigitYear + 1900;		# convert '83' to '1983', or '02' to '1902'
				if( !$twoDigitYearSeen ) {
					PMSLogging::DumpWarning( "", "", "PMSUtil::ConvertToISOPrimary(): invalid date ('$passedDate' - invalid year). " .
						"Changing '$twoDigitYear' to '$year'.  This needs to be corrected (this message " .
						"will not be repeated.)", "" );
					$twoDigitYearSeen = 1;
				}
			}
		} else {
			$year -= 2000;		# back to a 2 digit year...
			if( !$twoDigitYearSeen ) {
				PMSLogging::DumpError( "", "", "PMSUtil::ConvertToISOPrimary(): invalid date ('$passedDate' - invalid year). " .
					"Not sure what it should be so not changed.  This needs to be corrected (this message " .
					"will not be repeated.)", "" );
				$twoDigitYearSeen = 1;
			}
		}
	} elsif( length($year) == 3 ) {
		PMSLogging::DumpError( "", "", "PMSUtil::ConvertToISOPrimary(): invalid date ('$passedDate' - invalid year). " .
			"Changing '$year' to '1$year'.  This needs to be corrected.", "" );
		$year = "1$year";
	}
	# our year is 4 or more digits...
	if( (length($year) >= 4) && ( ($year < 1900) || ($year > 2200) ) ) {
		# avoid sql error, but still allow possibly bogus year
		PMSLogging::DumpError( "", "", "PMSUtil::ConvertToISOPrimary(): invalid date ('$passedDate' - invalid year). " .
			"Changing to year '1900'.  This needs to be corrected.", "" );
		$year = 1900;
	}

	return "$year-$month-$day";
} # end of ConvertToISOPrimary()




# ConvertDateRangeToISO - convert a date of the form 'Feb 11, 2017' or 'Jun 11-12, 2016'  or 'Apr 28 - May 1, 2016' into 
#		MySql format 'yyyy-mm-dd' or 'yyyy-mm-dd - yyyy-mm-dd' (where 'dd' and 'mm' can be single digits)
#
# RETURNED:
#	$result - ''yyyy-mm-dd' or 'yyyy-mm-dd - yyyy-mm-dd' (where 'dd' and 'mm' can be single digits) or ""
#		if failure.
#
# NOTES:
#	Doesn't work for date ranges that span different years
#
sub ConvertDateRangeToISO( $ ) {
	my $passedDate = $_[0];
	my $result = "";
	my %mon2num = qw(
	  jan 1  feb 2  mar 3  apr 4  may 5  jun 6
	  jul 7  aug 8  sep 9  oct 10 nov 11 dec 12
	);
	
	# try a date of the form 'Feb 11, 2017' or 'Jun 11-12, 2016'
	$passedDate =~ m/^([a-zA-Z]+)\s+([^\s]+),\s+(\d+)$/;
	my $monthWord = $1;
	my $days = $2;
	my $year = $3;
	my ($day1, $day2);
	# check for bad date:
	if( (!defined $monthWord) || (!defined $days) || (!defined $year) ) {
		# OK, now try a date of the form 'Apr 28 - May 1, 2016'
		$passedDate =~ m/^([a-zA-Z]+)\s+(\d+)\s+-\s+([a-zA-Z]+)\s+(\d+),\s+(\d+)$/;
		my $month1Word = $1;
		$day1 = $2;
		my $month2Word = $3;
		$day2 = $4;
		$year = $5;
		# check for bad date:
		if( (!defined $month1Word) || (!defined $month2Word) || 
			(!defined $day1) || (!defined $day2) || 
			(!defined $year) ) {
			PMSLogging::DumpError( "", "", "TT_Util::ConvertDateRangeToISO: Unable to parse '$passedDate'", 1 );
		} else {
			# we have a date in the form 'Apr 28 - May 1, 2016'
			my $month1 = $mon2num{ lc( $month1Word ) };
			my $month2 = $mon2num{ lc( $month2Word ) };
			$result = "$year-$month1-$day1 - $year-$month2-$day2";
		}
	} else {
		# we have a date in the form 'Feb 11, 2017' or 'Jun 11-12, 2016'
		my $month = $mon2num{ lc( $monthWord ) };
		if( $days =~ m/-/ ) {
			# we have a range
			$days =~ m/^(\d+)-(\d+)$/;
			$day1 = $1;
			$day2 = $2;
			$result = "$year-$month-$day1 - $year-$month-$day2";
		} else {
			# single day meet
			$result = "$year-$month-$days";
		}
		# check for invalid date (did we generate what we expected?)
		if( ($result !~ m/^\d+-\d+-\d+ - \d+-\d+-\d+$/) &&
			($result !~ m/^\d+-\d+-\d+$/) ) {
			PMSLogging::DumpError( "", "", "TT_Util::ConvertDateRangeToISO: Conversion failed with '$passedDate'", 1 );
			$result = "";
		}
	}
	return $result;		
} # end of ConvertDateRangeToISO()






# PMSUtil::AgeAtEndOfYear( $dob )
# AgeAtEndOfYear - compute the age (in integer years) of a person as of the end of the year being processed.
#
# PASSED:
#	$dob - the date of birth of the person, in canonical form (yyyy-mm-dd)
#
# RETURNED:
#	$age - their age as of the end of the year being processed, in years.
#
sub AgeAtEndOfYear( $ ) {
	my $dob = $_[0];
	my $theYearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};
	my ($birthDay, $birthMonth, $birthYear) = ($dob, $dob, $dob);
	
	$birthYear =~ s/-.*$//;
	my $age = $theYearBeingProcessed - $birthYear;
	return $age;
	
} # end of AgeAtEndOfYear()



# GenerateCanonicalRegNum - convert the passed regnum into a canonical form:
#
# PASSED:
#	$regNum - the reg num, or an undefined or empty string.
#
# RETURNED:
#	$regNum - the reg num in a canonical form: 'xxxx-zzzzz' or $PMSConstants::INVALID_REGNUM.
#		xxxx and zzzzz are non-'-'
#		xxxx is 4 characters long and zzzzz is 5 or more characters long
#		x and z can be '?' if we had to fill in missing chars to make it long enough.
#
# NOTES:
# 	- Convert to all UPPER CASE.
# 	- Assume it's in the form 382106BJ5 or 382Z-07TDU (or with the '-' somewhere else) - return the
# 		reg num in this form:  382Z-07TDU
## 	- If it doens't start with a digit then return a known, 
##		illegal regnum: $PMSConstants::INVALID_REGNUM
#	- if an empty string or undefined value is passed then return a known, 
#		illegal regnum: $PMSConstants::INVALID_REGNUM
#	- if it's 5 or less characters add the left most characters to make it a legal length
#
sub GenerateCanonicalRegNum($) {
	my $regNum = $_[0]; 
	if( !defined($regNum) || $regNum eq '' ) {
		$regNum = $PMSConstants::INVALID_REGNUM;
#	} elsif( $regNum =~ m/^\D/ ) {
#		# starts with a non-digit
#		$regNum = $PMSConstants::INVALID_REGNUM;
	} else {
		$regNum = uc($regNum);
		$regNum =~ s/-//;		# should now look like this:   x...x where x is a non '-' and there are 0 or more of them
		if( length( $regNum ) < 5 ) {
			# pad with ?'s
			for( my $i = length( $regNum ); $i < 5; $i++ ) {
				$regNum .= "?";
			}
		}
		# now guaranteed to be 'xxxxx' where x is a non '-' and length >= 5
		if( length( $regNum ) < 9 ) {
			for( my $i = length( $regNum ); $i < 9; $i++ ) {
				$regNum .= "?";
			}
		}
		# now guaranteed to be 'xxxxx' where x is a non '-' and length >= 9
		# put the '-' back where it belongs
		$regNum =~ s/^(....)/$1-/;
		# guarantee a regnum of length 10 exactly:
		$regNum =~ s/^(..........).*$/$1/;
		# now guaranteed to be 'xxxx-zzzzz' where x and z are non '-' and length == 10
	}
	return $regNum;
} # end of GenerateCanonicalRegNum()




# GenerateCanonicalUSMSSwimmerId - convert the passed USMSSwimmerId into a canonical form:
#
# PASSED:
#	$USMSSwimmerId - a 5 character string, or an undefined or empty string.
#
# RETURNED:
#	$USMSSwimmerId - a valid USMSSwimmerId (although it might not be a real one)
#
# NOTES:
# 	- Convert to all UPPER CASE.
#	- Remove the leading '#" if it's there (USMS likes to do that in some places)
# 	- Assume it's in the form 06BJ5 or 456 
#	- if it's 1-4 characters add the left most characters (0's) to make it legal 5 chars.  We do
#		this because sometimes Excel is involved and if the swimmer id is all digits (like '00456') 
#		it will truncate the 0's since Excel will think it's a number and not a string of digits.
#	- if it's 0 length or undefined return '?????'
#
sub GenerateCanonicalUSMSSwimmerId($) {
	my $USMSSwimmerId = $_[0]; 
	if( !defined($USMSSwimmerId) || $USMSSwimmerId eq '' ) {
		$USMSSwimmerId = "?????";
	} else {
        # remove leading octothorp if it's there
        $USMSSwimmerId =~ s/^#//;
        # shift to upper case letters
		$USMSSwimmerId = uc($USMSSwimmerId);
		# if it's something like 123 change it to '00123'
		if( length( $USMSSwimmerId ) < 5 ) {
			# pad with 0's
			for( my $i = length( $USMSSwimmerId ); $i < 5; $i++ ) {
				$USMSSwimmerId .= "0";
			}
		}
		# now guaranteed to be 'xxxxx'
	}
	return $USMSSwimmerId;
} # end of GenerateCanonicalUSMSSwimmerId()



# ValidateAndCorrectSwimmerId - validate and return corrected USMSSwimmerId or regnum
#
# PASSED:
#	$id - either a swimmerId or a regnum.  Must be in canonical form, so we'll tell the difference
#		based on the presence of a '-'.  If any of the characters are '?' or
#		$id is $PMSConstants::INVALID_REGNUM then we'll know it's an invalid id and handle it appropriatly.
#	$caller - a string used in the generated log message if we discover a problem with the passed id.
#	$yearBeingProcessed - (optional) the year being processed.  Beginning in 2018 ALL characters are legal!
#
# RETURNED:
#	$id - the passed $id, or slightly modified to replace illegal characters with likely legal ones if
#		such a replacement would likely yield a valid id.
#
# NOTES:
#	The following characters are illegal in the passed id (2017 and before):
#		L   I   O (oh)      Q
#	they will be replaced with:
#		1   1   0 (zero)	0 (zero)
#
#	Also:  remove all but 5 chars in swimmerid; remove non letter/digit/-  (to remove * from old
#	style regnums marking non-pms.)
#
#	Today (26Apr2018) we got a new RSIDN file that had reg numbers with L, I, and Q in them.
#	After investigation it turns out that ALL letters/digits are legal beginning in 2018.
#
sub ValidateAndCorrectSwimmerId {
	my($id, $caller, $yearBeingProcessed) = @_;
	my $idType = "USMSSwimmerId";
	my $newId = $id;

	# if the $yearBeingProcessed isn't passed then just make it < 2018
	$yearBeingProcessed = 2017 if( !defined $yearBeingProcessed );

	# don't bother if the passed $id is $PMSConstants::INVALID_REGNUM
	if( $id ne $PMSConstants::INVALID_REGNUM ) {
		if( $id =~ m/-/ ) {
			# regnum
			$idType = "RegNum";
			if( $id !~ m/^....-/ ) {
				PMSLogging::DumpWarning(0, 0, "$caller [called PMSUtil::ValidateAndCorrectSwimmerId()]: " .
					"Left-part of RegNum ($id) is not valid.", 1 );
			} else {
				$newId =~ s/[^\dA-Za-z?-]//g;		# remove all non digit, non-letter, non-'-', non '?' chars
				if( $newId ne $id ) {
					PMSLogging::DumpWarning(0, 0, "$caller [called PMSUtil::ValidateAndCorrectSwimmerId()]: " .
						"\n    Found invalid RegNum with illegal chars ('$id') - replaced with '$newId'" );
				}
				if( length( $newId ) != 10) {
					PMSLogging::DumpWarning(0, 0, "$caller [called PMSUtil::ValidateAndCorrectSwimmerId()]: " .
						"\n    RegNum ($newId) is the wrong length.", 1 );
				}
			}
		} else {
			# swimmer id
			$newId =~ s/[^\dA-Za-z?]//g;		# remove all non digit, non-letter chars
			if( $newId ne $id ) {
				PMSLogging::DumpWarning(0, 0, "$caller [called PMSUtil::ValidateAndCorrectSwimmerId()]: " .
					"Found invalid USMSSwimmerId with illegal chars ('$id') - replaced with '$newId'", 1 );
			}
			if( length( $newId ) != 5 ) {
				PMSLogging::DumpWarning(0, 0, "$caller [called PMSUtil::ValidateAndCorrectSwimmerId()]: " .
					"USMSSwimmerId ($newId) is the wrong length.", 1 );
			}
		}

		# now replace known typos - make a guess as to the correct replacement
		# Only do this if this id is likely valid except typos
		# ALSO: only do this for the years 2017 and before:
		if( ($newId !~ m/\?/) && ($yearBeingProcessed < 2018) ) {
			$id = $newId;
			$newId =~ s/[LI]/1/gi;
			$newId =~ s/[OQ]/0/gi;
			if( $newId ne $id ) {
				PMSLogging::DumpWarning(0, 0, "$caller [called PMSUtil::ValidateAndCorrectSwimmerId()]: " .
					"\n    Found invalid $idType ('$id') - replaced with '$newId'" );
			}
		}
	} # end of if( $id ne $PMSConstants::INVALID_REGNUM ...
	
	return $newId;
	
} # end of ValidateAndCorrectSwimmerId()





# GenerateCanonicalGender - return the one letter gender designation (M or F) for the
#	passed gender.  Return '?' if the passed gender isn't recognized.
# Note:  errors are printed, not logged, because this routine is designed to be used prior
#	to initialization of our log file.
sub GenerateCanonicalGender($$$) {
	my($fileName, $lineNum) = @_;
	my $passedGender = $_[2];
	if( !defined $passedGender ) {
		$passedGender = '(undefined)';		# invalid gender - caught below
	} elsif( $passedGender eq "" ) {
		$passedGender = '(empty)';
	} else {
		$passedGender = uc($passedGender);
	}
	$passedGender =~ m/^(.)/;
	my $result = $1;			# default is first letter of gender term (e.g. 'W' for 'Women')
	$result = 'F' if( $result eq 'W');
	$result = 'F' if( $result eq 'G');
	$result = 'M' if( $result eq 'B');
	if( ($result ne 'M') && ($result ne 'F') ) {
		$result = "?";
		print "GenerateCanonicalGender: (error in '$fileName', line $lineNum): returning illegal value '$result' when passed '$passedGender'\n";
	}
	return $result;
} # end of GenerateCononicalGender()




# GetEventDetail - Given the passed results simple file name (e.g. "2014 Whiskeytown 2 Mile=CAT1.csv") 
#	return the requested detail from the passed calendar hash.
#
# PASSED:
#	$simpleFileName - the simple file name of the result file for the event, e.g. 
#		"2014 Whiskeytown 2 Mile=CAT1.csv"
#	$calendarRef - a reference to the calendar hash
#	$detail - the detail to return.  One of:  FileName, CAT, Date, Distance, FullName, UniqueID
#
# RETURNED:
#	$result - the data requested (if found), or "(unknown)" if not found.
#
sub GetEventDetail( $$$ ) {
    my ($simpleFileName, $calendarRef, $detail) = @_;
	my $result = "(unknown)";
	
	# get the key that matches the passed simple file name and from that we'll get the requested detail
	for( my $raceOrder=1; ; $raceOrder++ ) {
		my $match = $calendarRef->{$raceOrder};
		if( !defined( $match ) ) {
			# didn't find the right key - this is a problem!
			PMSLogging::DumpError( 0, 0, "PMSUtil::GetEventDetail(): " .
				"Unable to find the calendar key for the simple file name: '$simpleFileName'.  " .
				"Tried " . ($raceOrder-1) . "different matches.", 1 );
			last;
		}
		if( $simpleFileName eq $match ) {
			$result = $calendarRef->{"$raceOrder-$detail"};
			last;
		}
	}		
	return $result;
} # end of GetEventDetail()




# IncrementAgeGroup - given the passed age group compute the next one.
#
# PASSED:
# 	$ageGroup - of the form "18-24" or "35-39"
#
# RETURNED:
#	$nextAgeGroup - of the form "25-29" or "40-44"
#
sub IncrementAgeGroup( $ ) {
	my $ageGroup = $_[0];
	my $lowerAge = $ageGroup;
	$lowerAge =~ s/-.*$//;
	if( $lowerAge == 18 ) {
		$lowerAge = 25;
	} else {
		$lowerAge += 5;
	}
	my $upperAge = $lowerAge + 4;
	my $nextAgeGroup = "$lowerAge-$upperAge";
	return $nextAgeGroup;
} # end of IncrementAgeGroup()



# DecrementAgeGroup - given the passed age group compute the previous one.
#
# PASSED:
# 	$ageGroup - of the form "18-24" or "35-39"
#
# RETURNED:
#	$previousAgeGroup - of the form "" or "30-34"
#
sub DecrementAgeGroup( $ ) {
	my $ageGroup = $_[0];
	my $previousAgeGroup = "";
	my $lowerAge = $ageGroup;
	$lowerAge =~ s/-.*$//;
	if( $lowerAge != 18 ) {
		$lowerAge -= 5;
		my $upperAge = $lowerAge + 4;
		$previousAgeGroup = "$lowerAge-$upperAge";
	}
	return $previousAgeGroup;
} # end of DecrementAgeGroup()



# IsValidAgeGroup - determine if the passed age group is valid or not
#
# PASSED:
#	ageGrp - of the form 18-24
#
# RETURNED:
#	$result - 1 if the passed age group is a valid one, undefined if not.
#
sub IsValidAgeGroup($) {
    my $ageGrp = $_[0];
    my $result = $PMSConstants::AGEGROUPS_MASTERS_HASH{$ageGrp};
    my $x = $PMSConstants::AGEGROUPS_MASTERS_HASH{$ageGrp};		# get rid of perl error!
    return $result;
} # end of IsValidAgeGroup()



# FixInvalidAgeGroup - Attempt to fix invalid age groups, e.g. age group "80-" gets converted into "80-84"
#
# PASSED:
#	$ageGroup - the invalid age group
#	$age1 - first component of invalid age group
#	$age2 - second component of invalid age group
#
# RETURNED:
#	$result - may be the same as the passed $ageGroup, or may be fixed to be valid.
#
sub FixInvalidAgeGroup( $$$ ) {
    my ($ageGrp, $age1, $age2) = @_;
    my $fixed = 0;		# set to 1 once we think we fixed it
    if( ($age1 ne "") && ($age1 =~ m/^\d+$/) ) {
	    if( $age1 == 18 ) {
	    	$age2 = 24;
	    	$fixed = 1;
	    } elsif( ($age1 > 24) && (($age1%5)==0) ) {
	    	$age2 = $age1+4;
	    	$fixed = 1;
	    }
    }
    
    # did we fix it?
    if( !$fixed ) {
    	# nope - see if the $age2 value gives us a clue
    	if( ($age2 ne "") && ($age2 =~ m/^\d+$/) ) {
		    if( $age2 == 24 ) {
		    	$age1 = 18;
		    	$fixed = 1;
		    } elsif( ($age2 > 28) && ((($age2+1)%5)==0) ) {
		    	$age1 = $age2-4;
		    	$fixed = 1;
		    }
    	}
    }
    my $result = "$age1-$age2";		# may be the same as what was passed
    return $result;
} # end of FixInvalidAgeGroup()





# IsValidAge - determine whether or not the passed age is a valid age in the passed age group
#
# RETURNED:
#	$age - the passed age if the passed age is within the passed age group, or $PMSConstants::INVALIDAGE
#		if the passed age is not.
#
sub IsValidAge( $$ ) {
    my $age = $_[0];
    my $loAge = $_[1];
    my $hiAge = $_[1];
    $loAge =~ s/-.*$//;
    $hiAge =~ s/^.*-//;
    $age = $PMSConstants::INVALIDAGE if( ($age < $loAge) || ($age > $hiAge) );
    $age = $PMSConstants::INVALIDAGE if( ($age < $loAge) || ($age > $hiAge) );  #remove compiler warning
    return $age;
} # end of IsValidAge



# DifferentAgeGroups - return 1 if the two passed ages are in different age groups, or 0 if not.
#	Return -1 if one/both of the ages are invalid.
#
sub DifferentAgeGroups( $$ ) {
	my ($age1, $age2) = @_;
	my $result = 1;
	my $ageGroup1 = ComputeAgeGroup( $age1 );
	my $ageGroup2 = ComputeAgeGroup( $age2 );
	if( ($ageGroup1 eq "") || ($ageGroup2 eq "") ) {
		$result = -1;
	} elsif( $ageGroup1 eq $ageGroup2 ) {
		$result = 0;
	}
	return $result;
} # end of DifferentAgeGroups()


# ComputeAgeGroup - return the age group for the passed age, or "" if it's an invalid age
#
sub ComputeAgeGroup( $ ) {
	my $age = $_[0];
	my $result = "";
	if( ($age >= 18) && ($age < 25) ) {
		$result = $PMSConstants::AGEGROUPS_MASTERS[0];
	} elsif( $age >= 25 ) {
		my $index = int( $age / 5 ) - 4;
		$result = $PMSConstants::AGEGROUPS_MASTERS[$index];	
	}
	return $result;
} # end of ComputeAgeGroup()


# GenerateCanonicalNames - clean up first, middle, and last names.  
#
# PASSED:
#	$lastName -
#	$firstName -
#	$middleInitial -
#		...the name
#
# RETURNED:
#	$lastName -
#	$firstName -
#	$middleInitial -
#		... the name repaired...
#
# NOTES:
#	remove special chars; make the middle initial only 1 char.
#
sub GenerateCanonicalNames($$$) {
    my ($lastName, $firstName, $middleInitial) = @_;
    
    # remove commas and double-quotes from names
    $lastName =~ s/"|,//g;
    $firstName =~ s/"|,//g;
    # and then remove any leading/trailing spaces from the names which might be left there when the above chars are removed
    $lastName =~ s/^ *//;
    $lastName =~ s/ *$//;
    $firstName =~ s/^ *//;
    $firstName =~ s/ *$//;
    
    #remove all but first letter from initial
    $middleInitial =~ s/[^a-z]//gi;
    $middleInitial =~ s/^(.).*$/$1/;
    
    return( ($lastName,$firstName, $middleInitial) );

} # end of GenerateCanonicalNames



# NamesCompareOK2 - compare two names and return the fuzzy score:
#   0 if they are the same names exactly, 
#   >0 if they fuzzy compare by result but we consider them the "same"
#   <0 if they fuzzy compare by |result| but we consider them "different"
#
# PASSED:
#   $name1First, $name1Middle, $name1Last - one of the names
#   $name2 - $name2First, $name2Middle, $name2Last - the other name
#
# RETURNED:
#	$result - if >= 0 then we consider the name "the same" - the closer to 0 the more we believe it!
#		Otherwise, if < 0, then we don't consider the names the same.  The further from 0 the more we 
#		believe that, too!
#
sub NamesCompareOK2($$$$$$) {
	my( $name1First, $name1Middle, $name1Last,
		 $name2First, $name2Middle, $name2Last ) = @_; 
    my $result = 0;        # assume the names match exactly
    
    my( $first2, $last2, $middle2 ) = PMSUtil::GenerateCanonicalNames( 
    	$name2First, $name2Last, $name2Middle );
    
    # look for exact match:
    if( (lc($name1First) eq lc($first2)) && (lc($name1Last) eq lc($last2)) ) {
    	# exact match - return immediatly
    } else {
	    # fuzzy compare
	    my $fuzzyFirstResult = FuzzyCompareTwoStrings( $first2, $name1First );
	    my $fuzzyFirstLength = length($first2)<length($name1First)?length($first2):length($name1First);
	    $fuzzyFirstLength = int($fuzzyFirstLength/2);
	    $fuzzyFirstLength = $fuzzyFirstLength<3?$fuzzyFirstLength:3;
	    my $fuzzyLastResult = FuzzyCompareTwoStrings( $last2, $name1Last );
	    my $fuzzyLastLength = length($last2)<length($name1Last)?length($last2):length($name1Last);
	    $fuzzyLastLength = int($fuzzyLastLength/2);
	    $fuzzyLastLength = $fuzzyLastLength<2?$fuzzyLastLength:2;
	    # compute overall fuzzy score:
	    $result = $fuzzyLastResult + $fuzzyFirstResult;
	    if( ($fuzzyLastResult <= $fuzzyLastLength) && ($fuzzyFirstResult <= $fuzzyFirstLength) ) {
	        # names match "almost" exactly or fuzzy match
	        # since a return of 0 is reserved for exact match (above) we'll make sure we return 1 or more:
	        if( $result == 0 ) {
	        	$result = 1;
	        }
	    } else {
	        # names don't match, fuzzy or otherwise
	        $result *= -1;
	    }
    }
        
    return $result;
} # end of NamesCompareOK2()




# FuzzyCompareTwoStrings - (case insensitive) compare the passed two strings and get a rough
#   approximation of how likely the two strings are the "same".
#
# PASSED:
#	$shortString - 
#	$longString - 
#		... the two strings to compare
#
# RETURNED:
#   fuzzyResult = 0 if the strings seems to be the same, >0 if different.  The larger the fuzzyResult the
#       more likely it is that the strings are different.
#
# NOTES:
#   "Same" means "identical (except case) or nearly the same length with nearly the same 
#   number of the same characters."  Note the characters may not be in the same order.
#
sub FuzzyCompareTwoStrings( $$ ) {
    my $shortString = lc( $_[0] );      # assume this is the shorter string
    my $longString = lc( $_[1] );
    my $result = 0;
    my $fuzzyResult = 0;
    my $lengthDiff;
    
    # check for the boundary case:  the strings are identical
    if( $shortString eq $longString ) {
        # boundary case:  strings are identical.  fuzzyResult is 0
    } else {
        # which string is the shortest?
        if( length( $shortString ) > length( $longString ) ) {
            my $tmp = $shortString;
            $shortString = $longString;
            $longString = $tmp;
        }
        $lengthDiff = length( $longString ) - length( $shortString );
        $result = $lengthDiff - 3;
        $result = 1 if( $result < 0 );          # in case lengths are the same or within a few
        
        # get a list of the chars in the shortest string
        my $uniqueChars = SingleUniqueChars( $shortString );
        
        # for every char in the short string see if the same number of those chars exist in the longer string
        # The smaller the result the more likely the short string is a substring of the long string.
        # (Even if the chars are in a different order a small number implies fuzzy substring)
        for( my $i=0; $i < length( $uniqueChars ); $i++ ) {
            my $char = substr( $uniqueChars, $i, 1 );           # get the i'th char from $uniqueChars
            # if the number of x's in short string is > number of x's in long string then we have
            # a mismatch.  But since we know that the long string has more chars than the short
            # string, and we've already dinged the fuzzyresult for that difference, we allow
            # number of x's in short string to be <= number of x's in long string
            $result += 1 if( CountChar( $char, $shortString ) > CountChar( $char, $longString ) );
        }
    
    $fuzzyResult = $result;
    }
    
    return $fuzzyResult;
} # end of FuzzyCompareTwoStrings()



# CountChar( char, str )
#   return then number of 'char's found in str
sub CountChar {
    my $char = $_[0];
    my $str = $_[1];
    my $count = 0;
    eval "\$count = \$str =~ tr/$char//";
    return $count;
} # end of CountChar()




# SingleUniqueChars - take the passed string and return another string containing exactly 
#   one of each of the chars in the passed string.
#   Example:  This dog has fleas      returns:   This dogafle
#       
sub SingleUniqueChars( $ ) {
    my $str1 = $_[0];
    my %uniqueHash;
    $uniqueHash{ $_ } = 1 for split //, $str1;
    my @res = grep { $uniqueHash{ $_ } ? ( $uniqueHash{ $_ }-- , $_ ) : () } split //, $str1;
    my $result = "@res";
    $result =~ s/(.)\s/$1/seg;
    return $result;
} # end of SingleUniqueChars()


# CleanAndConvertRowIntoString - convert array into string
#
# PASSED:
#	$row - a reference to an array of fields
#
# RETURNED:
#	$rowAsString - a string containing each of the fields of $row separated by commas
#	$count - the number of non-empty fields found in $row
#	$row - some fields in the passed $row may be modified
#
# NOTES:
# 	march thru the passed row (array of fields), remove leading and trailing whitespace from each 
#	field, and count the number of non-empty fields.
# 	The passed row is reference to an array.  The contents of the array may be modified.
# 	The returned "row as string" shows the cleaned fields as a comma-separated string, where
#   an empty field is denoted as ",,".  Note that the count returned may not match the 
#   number of fields seen in the rowAsString, since some of those fields might be empty.
#
sub CleanAndConvertRowIntoString( $ ) {
    my $row = $_[0];
    my $rowAsString = "";
    my $count = 0;
    my $rowLength = scalar @$row;
    for( my $i = 0; $i < $rowLength; $i++ ) {
        my $field = $row->[$i];
        if( !defined $field ) {
            $field = "";
        }
        $field =~ s/^\s*//;
        $field =~ s/\s*$//;
        $count++ if( $field ne "" );
        $row->[$i] = $field;
        if( $i == 0 ) {
            $rowAsString .= "$field";
        } else {
            $rowAsString .= ",$field";
        }
    }
    return ($rowAsString, $count);
} # end of CleanAndConvertRowIntoString()




# PlaceToString - convert the passed number (1, 2, ...) into "1st", "2nd", etc.
#	Assumes the passed number is > 0.
#
# PASSED:
#	$num - the number > 0
#	$errStr - used when errors are found.  Denotes the calling routine so we can find the cause of the error.
#
# RETURNED:
#	$result - the string, e.g. "3rd"
#
sub PlaceToString( $$ ) {
	my ($num, $errStr) = @_;
	
	if( !defined $num ) {
		PMSLogging::DumpWarning( "", "", "PMSUtil::PlaceToString(): passed 'num' is not defined.  $errStr", 1 );
		return "?-th";
	}
	if( $num < 1 ) {
		PMSLogging::DumpWarning( "", "", "PMSUtil::PlaceToString(): illegal passed num: '$num'.  $errStr", 1 );
		return $num."th";
	}
	my @suffix = ("st", "nd", "rd", "th");
	my $index = $num-1;
	$index = 3 if( $index > 3 );
	my $result = $num . $suffix[$index];
	return $result;
} # end of PlaceToString()




# GetUSMSSwimmerIdFromRegNum - extract the USMS Swimmer ID from the passed regnum
#
# PASSED:
#	$regNum - the reg number
#
# RETURNED;
#	$USMSSwimmerId - the USMS Swimmer id part of the reg number
#
sub GetUSMSSwimmerIdFromRegNum( $ ) {
	my $regNum = $_[0];				# in the form    385X-03DZ2
	my $USMSSwimmerId = $regNum;		# will be in the form 03DZ2
	$USMSSwimmerId =~ s/^.*-//;
	return $USMSSwimmerId;
} # end of GetUSMSSwimmerIdFromRegNum



# my @arrOfBrokenNames = BreakFullNameIntoBrokenNames( $fullName );
# BreakFullNameIntoBrokenNames - break the $fullName into first, middle, and last names
#	(If the middle initial is not supplied then use "")
#
# PASSED:
#	$fullName - a string of the form "name1 name2 name3....nameN" where N is 1 or greater.
#		The assumption is that nameN is the middle name (or initial) or last part of the first name,
#		and name1 thru name(N-1)
#		make up the last followed by first names.  All names are space separated.
#		E.g. "Upshaw  Bob" or "Upshaw Bob B"  or "Upshaw Bob Robert" (where Bob is part of the
#		last name or part of the first name, and Robert is a middle name or part of first name)
#
# RETURNED:
#	@result - an array of possible interpretations of the passed $fullName
#
#	 Return an array of hashes:
#		arr[n]->{'first'} is a possible first name
#		arr[n]->{'middle'} is the matching possible middle initial
#		arr[n]->{'last'} is the matching possible last name
#		arr[n+1]->{'first'} is another possible first name
#		arr[n+1]->{'middle'} is the matching possible middle initial
#		arr[n+1]->{'last'} is the matching possible last name
#		...etc...
#	
# 	Return an empty array upon error.
#
sub BreakFullNameIntoBrokenNames($$$) {
	my ($fileName, $lineNum, $fullName) = @_;
	my @arrOfNames = split( /\s+/, $fullName );
	my @result = ();
	my $namesRef;		# reference to hash of names
	
	if( scalar(@arrOfNames) == 2 ) {
		# assume last and first name (only)
		$namesRef = {};
		$namesRef->{'last'} =  $arrOfNames[0];
		$namesRef->{'middle'} = "";
		$namesRef->{'first'} =  $arrOfNames[1];
		$result[0] = $namesRef;
	} elsif( scalar(@arrOfNames) == 3 ) {
		# assume last, first, middle
		$namesRef = {};
		$namesRef->{'last'} =  $arrOfNames[0];
		$namesRef->{'first'} =  $arrOfNames[1];
		# make sure middle initial is only 1 char
		$namesRef->{'middle'} =  $arrOfNames[2];
		$namesRef->{'middle'} =~ s/^(.).*$/$1/;
		$result[0] = $namesRef;
		# assume last last first
		$namesRef = {};
		$namesRef->{'last'} =  $arrOfNames[0] . " " . $arrOfNames[1];
		$namesRef->{'middle'} = "";
		$namesRef->{'first'} =  $arrOfNames[2];
		$result[1] = $namesRef;
		# assume last first first
		$namesRef = {};
		$namesRef->{'last'} =  $arrOfNames[0];
		$namesRef->{'middle'} = "";
		$namesRef->{'first'} =   $arrOfNames[1] . " " . $arrOfNames[2];
		$result[2] = $namesRef;
	} elsif( scalar(@arrOfNames) == 4 ) {
		# assume last last first middle
		$namesRef = {};
		$namesRef->{'last'} =  $arrOfNames[0] . " " . $arrOfNames[1];
		$namesRef->{'first'} =  $arrOfNames[2];
		$namesRef->{'middle'} =  $arrOfNames[3];
		# make sure middle initial is only 1 char
		$namesRef->{'middle'} =~ s/^(.).*$/$1/;
		$result[0] = $namesRef;
		# assume last first first middle
		$namesRef = {};
		$namesRef->{'last'} =  $arrOfNames[0];
		$namesRef->{'first'} =  $arrOfNames[1] . " " . $arrOfNames[2];
		$namesRef->{'middle'} =  $arrOfNames[3];
		# make sure middle initial is only 1 char
		$namesRef->{'middle'} =~ s/^(.).*$/$1/;
		$result[1] = $namesRef;
	} else {
		# the name supplied wasn't empty but also didn't look like what we expected...
		# Generate an error so we investigate.
		PMSLogging::PrintLog( "PMSUtil::BreakFullNameIntoBrokenNames(): Unrecognized format for the full name ['$fullName']. " .
			" File: '$fileName', line num: $lineNum" );
	}

	return @result;

} # end of BreakFullNameIntoBrokenNames()


# ValidateDateWithinSeason - confirm that the passed date is within the passed season
#
# PASSED:
#	$date - the date to be confirmed.  Must be in the form yyyy-mm-dd.
#	$course - a string that begins with one of:  'SCY', 'SCM', 'LCM'
#	$yearBeingProcessed - the year under considereation
#
# RETURNED:
#	$result - either an empty string if the passed date is within the season for the passed course, or a message
#		explaining why the passed date is not within the season.  This string will begin with the substring
#		"Illegal" if there was a problem with the passed parameters.  Otherwise it will not.
#
# EXAMPLE:
#	ValidateDateWithinSeason( "2016-03-03", "SCY", "20", "16" ) will return an empty string because the 2016 SCY
#		season includes March 3, 2016.
#	ValidateDateWithinSeason( "2015-08-03", "SCY Records", "20", "16" ) will return an empty string because the 2016 SCY
#		season includes August 3, 2015.
#	ValidateDateWithinSeason( "2016-06-03", "SCY", "20", "16" ) will return an error string because the 2016 SCY
#		season does NOT include June 3, 2016.  (June 3, 2016 is part of the 2017 SCY season.)
#	ValidateDateWithinSeason( "2016-06-03", "SCM", "20", "16" ) will return an empty string because the 2016 SCM
#		season includes June 3, 2016.
#
	
	
######### CODE:
sub ValidateDateWithinSeason( $$$ ) {
	my( $date, $course, $yearBeingProcessed ) = @_;
	my $minDate = "$yearBeingProcessed";		# e.g. '2016'
	my $maxDate = "$yearBeingProcessed";		# e.g. '2016'
	my $simpleCourse = $course;
	
	# make sure we know the dates defining the season for the passed course:
	PMSConstants::FixLCMSeasonRangeFor2021( $yearBeingProcessed );
	
	$simpleCourse =~ s/^(...).*$/$1/;
	my $result = "";
	
	if( ($simpleCourse eq "SCY") || ($simpleCourse eq "LCM") ) {
		$minDate--;			# turn '2016' into '2015'
	}
	
	if( ($simpleCourse ne "SCY") && ($simpleCourse ne "LCM") && ($simpleCourse ne "SCM") ) {
		# illegal $course passed
		TT_Logging::PrintLog( "TT_Util::ValidateDateWithinSeason: Illegal course passed: '$course'\n" );
		# we will pretend that the passed date is NOT within the passed season, but this is a bug.
		$result = "Illegal course ($course) - assume this date does not fall within the $simpleCourse season.";
	} elsif( $date !~ m/\d\d\d\d-\d+-\d+/ ) {
		# simple validation of the passed date
		# this is an illegal date that came from the results, so it's not a bug - it's bad data.
		# Assume that this date does NOT fall within the season.
		$result = "Illegal date ($date) - assume this date does not fall within the $simpleCourse season.";
	} else {
		$minDate .= $PMSConstants::season{$simpleCourse . 'start'};
		$maxDate .= $PMSConstants::season{$simpleCourse . 'end'};
		
		if( ($date lt $minDate) || ($date gt $maxDate) ) {
			# the passed date is outside the passed season
			$result = "The passed date '$date' is outside the season for $simpleCourse ($minDate -> $maxDate)";
		}
	}
	
	return $result;
	
} # end of ValidateDateWithinSeason()

# ConvertArrayIntoString - convert array into string
#
# PASSED:
#	$row - a reference to an array of fields
#
# RETURNED:
#	$arrayAsString - a string containing each of the fields of $row separated by commas


# this is NOT returned:
#	$count - the number of non-empty fields found in $row


#
# NOTES:
# 	march thru the passed array of fields and count the number of non-empty fields.
# 	The returned "array as string" shows the fields as a comma-separated string, where
#   an empty field is denoted as ",,".  Note that the count returned may not match the 
#   number of fields seen in the $arrayAsString, since some of those fields might be empty.
#	Any field that contains a ',' or a '"' (comma or double-quote) will be surrounded with 
#	double-quotes ("), and the double-quote in the field will be replaced with two 
#	double-quotes.
#
sub ConvertArrayIntoString( $ ) {
    my $row = $_[0];
    my $arrayAsString = "";
#    my $count = 0;
    my $rowLength = scalar @$row;
    for( my $i = 0; $i < $rowLength; $i++ ) {
		my $addQuotes = 0;		# set to 1 if we need to quote the field
        my $field = $row->[$i];
        if( !defined $field ) {
            $field = "";
        }
#        $count++ if( $field ne "" );
        
        # the field must be surrounded by quotes if it contains a comma 
        if( index( $field, ',' ) != -1 ) {
        	$addQuotes = 1;
        }
        
        # any double-quotes contained in the field must be doubled:
        if( index( $field, '"' ) != -1 ) {
        	$field =~ s/"/""/g;
        	$addQuotes = 1;
        }
        
        if( $addQuotes ) {
        	$field = '"' . $field . '"';
        }

# why were we doing this?  changed a passed parameter that was not documented as changed above   
#$row->[$i] = $field;


        if( $i == 0 ) {
            $arrayAsString .= "$field";
        } else {
            $arrayAsString .= ",$field";
        }
    }
    return $arrayAsString;
} # end of ConvertArrayIntoString()




# 			my $points = PMSUtil::ComputePointsFromPlace();
sub ComputePointsFromPlace($) {
	my $computedPlace = $_[0];
	my $points = 0;
	
	if( ($computedPlace > 0) && ($computedPlace <= 10) ) {
		$points = $PMSConstants::PLACE[$computedPlace];
	}
	my $xxx = $PMSConstants::PLACE[1];		# get rid of compiler warning...
	
	return $points;
} # end of ComputePointsFromPlace()


# Passed:
#	stroke - something representing a swim stroke (free, I.M., etc)
#
# Returned:
#	stroke = the canonical version of the stroke, e.g. Freestyle, IM, etc.)  If we can't figure
#		it out we'll generate an error message and return what was passed.
#
# Notes:
#	Not called with OW "strokes", which are actually the name of the host of the OW event.
#
my %strokes = ();		# $strokes{'x'} = canonical stroke, e.g. $strokes{'Fr'} = "Freestyle"
sub CanonicalStroke( $ ) {
	my $stroke = $_[0];
	
	if( $stroke =~ m/fly/i ) {
		# could be "fly" or "butter fly", etc.
		$strokes{$stroke} = "Butterfly";
		$stroke = "Butterfly";
	}
	elsif( $stroke =~ m/^f/i ) {
		# could be free, freestyle, etc
		$strokes{$stroke} = "Freestyle";
		$stroke = "Freestyle"
	}
	elsif( $stroke =~ m/back/i ) {
		# could be "back" or "Back Stroke", etc.
		$strokes{$stroke} = "Backstroke";
		$stroke = "Backstroke";
	}
	elsif( $stroke =~ m/breast/i ) {
		# could be "Breast" or "breast stroke", etc.
		$strokes{$stroke} = "Breaststroke";
		$stroke = "Breaststroke";
	} elsif( $stroke =~ m/^i.*m/i ) {
		# could be "individual Medley" or "IM", etc.
		$strokes{$stroke} = "Individual Medley";
		$stroke = "Individual Medley";
	} elsif( $stroke =~ /medley/i ) {
		# could be "medley" or "medley relay", but not "Individual Medley" since we already caught that.
		$strokes{$stroke} = "Medley";
		$stroke = "Medley";
	}
	else {
		PMSLogging::DumpError( "", "", "PMSUtil::CanonicalStroke:  Invalid stroke: $stroke", 1 );
	}

	return $stroke;	
	
} # end of CanonicalStroke()


sub DumpStrokes() {
	my $lastKey = "";
	my @keys = sort { $strokes{$a} cmp $strokes{$b} } keys %strokes;
	my $printToConsole = 1;
	
	PMSLogging::PrintLog( "", "", "\nDump Of All Strokes Seen And Their Aliases:", $printToConsole );
	foreach my $key (@keys) {
		if( $strokes{$key} ne $lastKey ) {
			$lastKey = $strokes{$key};
			PMSLogging::PrintLog( "", "", "Stroke:  $strokes{$key}", $printToConsole );
		}
		PMSLogging::PrintLog( "", "", "    $key", $printToConsole );
	}
}


# 				my( $distance, $stroke ) = GetDistanceAndStroke( $row[2] );
# PASSED:
#	$distStroke - a string combining the distance and stroke of a swim event, e.g. "100 I.M."
#		or "100 Y free"
#
# RETURNED:
#	$distance - a swim distance, e.g. "100"
#	$stroke - a valid stroke, e.g. "Free".
#
# NOTES:
#	If we can't figure it out we'll return something...
#
sub GetDistanceAndStroke( $ ) {
	my $distStroke = $_[0];
	
	# assume something of the form "xxx ccc vvv eee..." where "xxx" are digits and the rest identifies
	# the stroke.
	$distStroke =~ m/^(\d+)\s*(.*)$/;
	my $distance = $1;
	my $stroke = $2;			# e.g. "I.M." or "Y free"
	my @arr = split( /\s+/, $stroke );		# e.g. arr[0]="I.M." or arr[0]="Y", arr[1]="free"
	my $len = scalar @arr;		# e.g. 1 or 2
	$stroke = $arr[$len-1];		# get the last array element, e.g. "I.M." or "free"
	$stroke = CanonicalStroke( $stroke ) if( defined $stroke );
	
	if( !defined $distance || !defined $stroke ) {
		PMSLogging::DumpError( "", "", "PMSUtil::GetDistanceAndStroke(): Unable to parse the passed 'distance + stroke' " .
			"('$distStroke')", 1 );
		$distance = $distStroke;
		$stroke = "?";
	}
	
	return( $distance, $stroke );
	
} # end of GetDistanceAndStroke()



# $eventCourse = PMSUtil::CanonicalOWCourse( $eventCourse );
# PASSED:
#	$eventCourse - a representation of "Mile" or "K"
#
# RETURNED:
#	$OWCourse - either "Mile" or "K", or an error is generated and $eventCourse is returned.
#
sub CanonicalOWCourse( $ ) {
	my $eventCourse = $_[0];
	my $OWCourse = $eventCourse;
	
	if( $eventCourse =~ m/^m/i ) {
		$OWCourse = "Mile";
	}
	elsif( $eventCourse =~ m/^k/i ) {
		$OWCourse = "K";
	}
	else {
		PMSLogging::DumpError( "", "", "PMSUtil::CanonicalOWCourse(): Unable to parse the passed OW course: " .
			"('$eventCourse')", 1 );
	}
	
	return $OWCourse;
	
} # end of CanonicalOWCourse()


sub GetFullFileNameFromPattern {
	my ($fileNamePattern, $parentDir, $fileType, $dontPrintError) = @_;
	if( !defined $dontPrintError ) {
		$dontPrintError = 0;			# by default we print an error message if we can't find an RSIND file
	}
	my $swimmerDataFile = undef;
	
	if( (! defined( $fileNamePattern ) ) && (! $dontPrintError) ) {
		# no file name pattern - this is an ERROR!
		PMSLogging::DumpError( "", 0, "A $fileType file name pattern wasn't found in the properties.txt file -" .
			"we will assume that there is no new $fileType file to process, BUT FIX THIS!", 1 );
	} else {
		# got a file name pattern:
		# We will use the most recent version of the file we can find in the $parentDir
		# directory:
		$swimmerDataFile = 	PMSUtil::GetMostRecentVersion( $fileNamePattern, $parentDir );
		if( (!defined $swimmerDataFile) && (! $dontPrintError) ) {
			# no file found matching the pattern - this is an ERROR!
			PMSLogging::DumpError( "", 0, "A $fileType file wasn't found in\n" .
				"    '$parentDir'\n" .
				"    using the pattern '$fileNamePattern'. " .
				"we will assume that there is no new $fileType file to process, BUT FIX THIS!\n" .
				"    (Did you mean to execute with '-empty'?)", 1 );
		}
	}
	return $swimmerDataFile;
} # end of GetFullFileNameFromPattern()




#	$swimmerDataFile = 	PMSUtil::GetMostRecentVersion( ".*RSIDN.*", $PMSSwimmerData );
# GetMostRecentVersion - scan the passed directory and return the newest version of the file
#	whose name matches the passed reg exp.
#
# PASSED:
#	$filePattern - we only consider files which case sensitively matches this RE.  
#		NOTE:  it's a RE!  not a file glob.
#	$directory - search this directory (does not recursively search sub dirs)
#
# RETURNED:
#	$fileName - the full path file name of the found file, or undefined is none found
#
sub GetMostRecentVersion( $$ ) {
	my ($filePattern, $directory) = @_;
	my @files;
	my $fileName;
	my $newestTime = 2**31-1;
	
	#print "GetMostRecentVersion():filePattern='$filePattern', directory='$directory'\n";
	opendir(my $DH, $directory) or die "PMSUtil::GetMostRecentVersion(): Failed to open '" .
		"$directory': $! - ABORT!";
	while (defined (my $file = readdir($DH))) {
		#print "GetMostRecentVersion():file='$file'\n";
		if( $file =~ m/$filePattern/ ) {
			my $path = File::Spec->catfile( $directory, $file );
			next unless (-f $path);           # ignore non-files - automatically does . and ..
			if( -M $path < $newestTime ) {
				$newestTime = -M $path;
				$fileName = $path;
			}
		}
	}
	closedir($DH);
	#print "GetMostRecentVersion():return fileName='$fileName'\n";
	return $fileName;
} # end of GetMostRecentVersion()




sub GetStackTrace {
	my $trace = Devel::StackTrace->new;
	my $fullTrace = "***Begin Stack Trace:\n" . $trace . "***End Stack Trace.";
	return $fullTrace;
} # end of GetStackTrace()


sub PrintStack {
	print GetStackTrace();
} # end of PrintStack()


1;  # end of module
