#!/usr/bin/perl -w
# History_MySqlSupport.pm - support routines and values used by the MySQL History based code.
#	When generating the HTML page of open water swimmers and their swims and points this code
#	is used to generate historical results for each open water swimmer.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package History_MySqlSupport;

#use lib 'PMSPerlModules';
use PMSConstants;
use PMSLogging;
require PMSUtil;
require PMS_MySqlSupport;

use strict;
use sigtrap;
use warnings;

sub GetSwimmerEventHistory_Recursive( $$$$$$$$ );
sub GetSwimmerTotalsHistory_Recursive( $$$$$ );

###############################################################
######## History Data #########################################
###############################################################


# 	my $sth = GetListOfEvents( $dbh );
# GetListOfEvents - return the list of the events we have recorded in our database.  They came from the
#	property file read when we initialized at the beginning of execution.
#
# PASSED:
#	$dbh - handle to the database.
#
# RETURNED:
#	$sth - the statement handle points to the list of events.  Contains data from the Events table
#		(which was populated by the properties file.)  Note that this is only the list of events
#		during the specific year being processed when this routine is called.  IT IS NOT the list
#		of all events in history.
#
# EXAMPLE:
#	my $sth = History_MySqlSupport::GetListOfEvents( $dbh );
#	my $eventHash = $sth->fetchrow_hashref;
#	while( defined( $eventHash ) ) {
#		my $eventName = $eventHash->{'EventName'};
#		my $eventUniqueID = $eventHash->{'UniqueEventID'};
#		... etc ...
#
sub GetListOfEvents( $ ) {
	my $dbh = $_[0];
	my( $sth, $rv );
	
	($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT UniqueEventID, EventName, Distance, Category, Date FROM Events " .
		"ORDER BY UniqueEventID ASC", "");
	return $sth;
} # end of GetListOfEvents()



#			History_MySqlSupport::GetSwimmerTotalsHistory( $resultHash->{'RegNum'}, $theYearBeingProcessed,
#			$category );
# GetSwimmerTotalsHistory - compute the total distance swum by the passed swimmer during each year
#	from the year prior to the passed year to and including 2008.
#	The result of this calculation is a string showing the distance, year, total time swum, and average 
#	1 mile time for each year covered, oldest year first.
#
# PASSED:
#	$dbh - our database handle
#	$regNum - the regNum of the swimmer we are working with
#	$theYearBeingProcessed - the year we are working on
#	$category - the category of suit for the passed swimmer
#
# RETURNED:
#	$result - a single string, a message saying that this swimmer has no previous history of OW swims, or
#		a stromg with one or more lines (HTML), each line representing a year.  E.g.
#			2015 Results: Total Distance: 5.553 Miles, Total Time: 2:19:31.75, Average 1 Mile time: 0:25:07.60
#			2014 Results: Total Distance: 7.174 Miles, Total Time: 2:50:57.10, Average 1 Mile time: 0:23:49.76
#			2013 Results: Total Distance: 7.553 Miles, Total Time: 2:55:47.55, Average 1 Mile time: 0:23:16.47
#			2012 Results: Total Distance: 1.000 Miles, Total Time: 0:21:01.60, Average 1 Mile time: 0:21:01.60
#			2011 Results: Total Distance: 9.803 Miles, Total Time: 3:49:22.10, Average 1 Mile time: 0:23:23.86
#			2010 Results: Total Distance: 14.750 Miles, Total Time: 5:45:16.50, Average 1 Mile time: 0:23:24.50
#			2009 Results: Total Distance: 8.250 Miles, Total Time: 3:08:26.30, Average 1 Mile time: 0:22:50.46
#			2008 Results: Total Distance: 1.000 Miles, Total Time: 0:28:44.20, Average 1 Mile time: 0:28:44.20
#
# NOTES:
#	Some swimmers have different swimmerId's during different years.  This routine will handle that.
#
sub GetSwimmerTotalsHistory( $$$$ ) {
	my ($dbh, $regNum, $theYearBeingProcessed, $category) = @_;
	my $result = "";
	my $USMSSwimmerId = PMSUtil::GetUSMSSwimmerIdFromRegNum( $regNum );
	my( $totalDistance, $totalTimeInHundredths );
	my $avg = 0;
	
	my $distanceColumn = "Cat$category" . "TotalDistance";
	my $durationColumn = "Cat$category" . "TotalDuration";
	
	# get the total time and distance swum in all previous years
	for( my $year = $theYearBeingProcessed-1; $year >= 2008; $year-- ) {
		my( $totalDistance, $totalTimeInHundredths ) = 
			GetSwimmerTotalsHistory_Recursive( $dbh, $USMSSwimmerId, $year, $distanceColumn, $durationColumn );
		if( $totalTimeInHundredths > 0 ) {
			if( $totalDistance != 0 ) {
				$avg = PMSUtil::GenerateDurationStringFromHundredths( int($totalTimeInHundredths / $totalDistance) );
			} else {
				# we have a distance of zero but a time of non-zero - something is wrong!
				PMSLogging::DumpError( "", 0, "History_MySqlSupport::GetSwimmerTotalsHistory(): " .
					"Invalid totalDistance is zero: " .
					"$totalDistance for USMSSwimmerId='$USMSSwimmerId', year ='$year', " .
					"category=$category.", 1 );
				# we'll make up a bogus average so we can keep going.
				$avg = "99:59:59.99";
			}
			$result .= "$year Results: " .
				"Total Distance: $totalDistance Miles,  " .
				"Total Time: " . PMSUtil::GenerateDurationStringFromHundredths( $totalTimeInHundredths ) . ", " .
				"Average 1 Mile time: $avg<br>";
		}
	} # end of for( my $year =...
		
	if( $result eq "" ) {
		$result = "We have no history of any previous Category $category open water swims for this swimmer.";
	}
	return $result;
} # end of GetSwimmerTotalsHistory()


# GetSwimmerTotalsHistory_Recursive - worker routine to support GetSwimmerTotalsHistory()
#
# Used to handle the situation where a swimmer can have more than one USMS Swimmer Id.
#
sub GetSwimmerTotalsHistory_Recursive( $$$$$ ) {
	my ($dbh, $USMSSwimmerId, $year, $distanceColumn, $durationColumn) = @_;
	my $totalTimeInHundredths = 0;
	my $totalDistance = 0;

	my($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT $distanceColumn, $durationColumn FROM SwimmerHistory " .
		"WHERE USMSSwimmerId = '$USMSSwimmerId' AND Year = '$year' " .
		"AND $durationColumn > 0", "" );
	if( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		$totalTimeInHundredths = $resultHash->{"$durationColumn"};
		$totalDistance = $resultHash->{"$distanceColumn"};
	}
	
	# did this swimmer ever swim under a different USMSSwimmerId?
	($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT OldUSMSSwimmerId FROM MergedMembers " .
			"WHERE NewUSMSSwimmerId = '$USMSSwimmerId'", "" );
	while( my $resultHash = $sth->fetchrow_hashref ) {
		# we've got a previous swim of this event for this swimmer with a different regnum
		my $oldUSMSSwimmerId = $resultHash->{'OldUSMSSwimmerId'};
		
		my( $newTotalDistance, $newTotalTime ) = 
			GetSwimmerTotalsHistory_Recursive( $dbh, $oldUSMSSwimmerId, $year, $distanceColumn, $durationColumn );
		$totalDistance += $newTotalDistance;
		$totalTimeInHundredths += $newTotalTime;
	}

	return ($totalDistance, $totalTimeInHundredths);	
} # end of GetSwimmerTotalsHistory_Recursive()



#				History_MySqlSupport::ConvertEventIdToUniqueEventID( PMS_MySqlSupport::GetMySqlHandle(),
#					$swimRef->{'EventId'} );
# ConvertEventIdToUniqueEventID - Every PMS OW event swum since and including 2008 that awards points will
#	have a unique id to distinguish that event from every other event.  An "eventId" is an id assigned to
#	an event during a specific year, and may change from year to year.  An "uniqueEventID" is assigned to an
#	event that remains with that event forever.  In the history of PMS OW swims, two events are different if
#	they have different uniqueEventID's, and, in addition, they will have different names.
# 
# PASSED:
#	$dbh - our database handle
#	$eventId - the eventId of the event we're interested in
#
# RETURNED:
#	$uniqueEventID - The historical event id for this event
#
sub ConvertEventIdToUniqueEventID( $$ ) {
	my ($dbh,$eventId) = @_;
	my $uniqueEventID = -1;
	
	my($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT UniqueEventID FROM Events " .
		"WHERE EventId = '$eventId'", "" );
	if( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		# we found the event
		$uniqueEventID = $resultHash->{'UniqueEventID'};
	} else {
		PMSLogging::DumpError( 0, 0, "History_MySqlSupport::ConvertEventIdToUniqueEventID(): Unable to " .
			"to convert event #$eventId to a UniqueEventID\n" );
	}
	return $uniqueEventID;
} # end of ConvertEventIdToUniqueEventID()




# GetSwimmerEventHistory - Return the historical details about the passed swimmer swimming the passed
#	event for all years prior to the passed year that swimmer swam the event.
#
# PASSED:
#	$dbh - our database handle
#	$regNum - identifies the swimmer we are interested in
#	$uniqueEventID - - identifies the event we are interested in
#	$category - the category of the swim
#	$theYearBeingProcessed - only consider years older than this one
#	$level (optional) - >0 when called recursively.  Default to 0.
#
# RETURNED:
#	$result - a single string, a message saying that this swimmer has no previous history of the passed event, or
#		a string with one or more lines (HTML), each line representing a year that this swimmer swam the event.  E.g.
#			2015 Spring Lake 1 Mile: [Place: 4th; 1.0 miles in 0:28:05.20]
#			2014 Spring Lake 1 Mile: [Place: 3rd; 1.0 miles in 0:24:45.00]
#			2012 Spring Lake 1 Mile: [Place: 3rd; 1.0 miles in 0:26:25.00]
#			2010 Spring Lake 1 Mile: [Place: 2nd; 1.0 miles in 0:26:28.90]
#
# NOTES:
#	Some swimmers have different swimmerId's during different years.  This routine will handle that.
#
sub GetSwimmerEventHistory( $$$$$ ) {
	my ($dbh, $regNum, $uniqueEventID, $category, $theYearBeingProcessed) = @_;
	my $result = "";
	my $USMSSwimmerId = PMSUtil::GetUSMSSwimmerIdFromRegNum( $regNum );

	# for error handling only...we may not need it but we'll get it anyway...
	my($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT FirstName, MiddleInitial, LastName FROM Swimmer " .
			"WHERE RegNum = '$regNum'" );
	my ($firstName, $middleInitial, $lastName) = ("?", "?", "?");
	if( my $resultHash3 = $sth->fetchrow_hashref ) {
		$firstName = $resultHash3->{'FirstName'};
		$middleInitial = $resultHash3->{'MiddleInitial'};
		$lastName = $resultHash3->{'LastName'};
	} else {
		PMSLogging::DumpError( 0, 0, "History_MySqlSupport::GetSwimmerEventHistory(): Unable to " .
			"to get the swimmer's name for regNum $regNum.\n" );
	}

	# now, did this swimmer have any previous history in this event?
	$result = GetSwimmerEventHistory_Recursive( $dbh, $USMSSwimmerId, $uniqueEventID, $category, 
		$theYearBeingProcessed, $firstName, $middleInitial, $lastName );
	
	if( $result eq "" ) {
		$result = "We have no history of any previous Category $category swims for this swimmer for this event.";
	}
	return $result;
	
} # end of GetSwimmerEventHistory()



# GetSwimmerEventHistory_Recursive - Return the historical details about the passed swimmer swimming the passed
#	event for all years prior to the passed year that swimmer swam the event.  This is the worker routine used
# 	by GetSwimmerEventHistory(), but designed to handle those swimmers who have multiple swimmerId's.
#

sub GetSwimmerEventHistory_Recursive( $$$$$$$$ ) {
	my ($dbh, $USMSSwimmerId, $uniqueEventID, $category, $theYearBeingProcessed, $firstName, 
		$middleInitial, $lastName) = @_;
	my $result = "";
	my $debugLastName = "xxxxx";
	
	if( lc($lastName) eq $debugLastName ){
		print "GetSwimmerEventHistory_Recursive: got $debugLastName\n";
	}
	
	my($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT Date, Duration, RecordedPlace, ComputedPlace, AgeGroup FROM SwimmerEventHistory " .
			"WHERE USMSSwimmerId = '$USMSSwimmerId' AND UniqueEventId = '$uniqueEventID' " .
			"AND Category = '$category' AND " .
			"EXTRACT(YEAR FROM Date) < $theYearBeingProcessed " .
			"Order by Date DESC", 
			lc($lastName) eq lc($debugLastName) ? "GetSwimmerEventHistory_Recursive for $debugLastName" : "" );
	while( my $resultHash = $sth->fetchrow_hashref ) {
		# we've got a previous swim of this event for this swimmer 
		my $date = $resultHash->{'Date'};		# of the form 2016-05-12
		my $duration = $resultHash->{'Duration'};
		my $recordedPlace = $resultHash->{'RecordedPlace'};
		my $computedPlace = $resultHash->{'ComputedPlace'};
		my $ageGroup = $resultHash->{'AgeGroup'};
		my $year = $date;
		$year =~ s/-.*$//;			# e.g. 2016
		# get the event name
		my $eventName = "(unknown event name)";
		my $distance = "(unknown distance)";
		my($sth2, $rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
			"SELECT EventName, Distance FROM EventHistory " .
				"WHERE UniqueEventId = '$uniqueEventID' AND Category = '$category' " );
		if( my $resultHash2 = $sth2->fetchrow_hashref ) {
			$eventName = $resultHash2->{'EventName'};
			$distance = $resultHash2->{'Distance'};
			# clean up Distance to remove excess trailing 0's (rt of decimal pt)
			$distance =~ s/(\.\d)0*/$1/;
			if( $debugLastName eq lc($lastName) ){
				PMSLogging::DumpNote( 0, 0, "GetSwimmerEventHistory_Recursive for $debugLastName: " .
				"event=$eventName, year=$year", 1);
			}
		} else {
			PMSLogging::DumpError( 0, 0, "History_MySqlSupport::GetSwimmerEventHistory_Recursive(): Unable to " .
				"to get the EventName for the event with the UniqueEventID $uniqueEventID.\n" );
		}
		my $placeStr = "Place: " . 
			PMSUtil::PlaceToString( $recordedPlace, "History_MySqlSupport::GetSwimmerEventHistory_Recursive(): " .
				"USMSSwimmerID='$USMSSwimmerId', Event Name='$eventName', Type of Place: 'RecordedPlace'" ) . ";";
		
		if( ($recordedPlace != $computedPlace) && ($computedPlace > 0) ) {
			$placeStr = "Recorded Place: " . 
				PMSUtil::PlaceToString( $recordedPlace, "History_MySqlSupport::GetSwimmerEventHistory_Recursive(): " .
				"USMSSwimmerID='$USMSSwimmerId', Event Name='$eventName', Type of Place: 'RecordedPlace'" ) . 
				" upgraded to " .
				PMSUtil::PlaceToString( $computedPlace, "History_MySqlSupport::GetSwimmerEventHistory_Recursive():\n" .
				"    USMSSwimmerID='$USMSSwimmerId', Event Name='$eventName', event date='$date', " .
				"Swimmer='$firstName $middleInitial $lastName', " .
				"Type of Place: 'ComputedPlace'" ) . ";";
		} elsif( $computedPlace <= 0 ) {
			PMSLogging::DumpError( 0, 0, "History_MySqlSupport::GetSwimmerEventHistory_Recursive(): " .
				"We've got a non-PMS or DQed swimmer with a computed place of $computedPlace.\n" .
				"    We probably need to generate a synonym for this swimmer for the year $date " .
				"or investigate further.\n" .
				"    USMSSwimmerID='$USMSSwimmerId', Event Name='$eventName', event date='$date', " .
				"Swimmer='$firstName $middleInitial $lastName'" );
		}
		$result .= "$year $eventName: [$placeStr $distance miles in " . 
			PMSUtil::GenerateDurationStringFromHundredths( $duration ) . 
			"]<br>";

	} # end of while( ...
	
	# did this swimmer ever swim under a different USMSSwimmerId?
	($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT OldUSMSSwimmerId FROM MergedMembers " .
			"WHERE NewUSMSSwimmerId = '$USMSSwimmerId'", "" );
	while( my $resultHash = $sth->fetchrow_hashref ) {
		# we've got a previous swim of this event for this swimmer with a different regnum
		my $oldUSMSSwimmerId = $resultHash->{'OldUSMSSwimmerId'};
		my $newResult = GetSwimmerEventHistory_Recursive( $dbh, $oldUSMSSwimmerId, $uniqueEventID, $category, 
			$theYearBeingProcessed, $firstName, $middleInitial, $lastName );
		$result .= $newResult;
	}
	
	return $result;
} # end of GetSwimmerEventHistory_Recursive();
	
	




1;  # end of module
