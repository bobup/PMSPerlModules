#!/usr/bin/perl -w
# PMSMacros.pm - support routines to support Properties and Macros.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMSMacros;

use File::Basename;
#use lib 'PMSPerlModules';
use PMSConstants;
use PMSLogging;
require PMSUtil;


use strict;
use sigtrap;
use warnings;

my %calendar;						# $calendar['1-Date'] = date of event whose race order is 1.
									# See ProcessCalendarPropertyLine() for more info.

my $templateName = "";				# Global varialbe used to hold the name of the template file being processed

sub SetTemplateName( $ ) {
	$templateName = $_[0];
}

sub GetTemplateName() {
	return $templateName;
}

# Read the properties.txt file and set the necessary properties by setting name/values in 
# the %macros hash which is accessed by the reference returned by PMSStruct::GetMacrosRef().  For example,
# after the property file is processed by this routine, and if the macro "TotalMilesSwum" is set in the 
# properties file, then it's value is retrieved by 
#		my $myTotalMilesSwum = PMSStruct::GetMacrosRef()->{"TotalMilesSwum"};
# Furthermore, any macro can be created/changed by something like this:
#		PMSStruct::GetMacrosRef()->{"TotalMilesSwum"}++;
#
# The main use of these macros is to allow us to use "template" files to generate our output.  For example,
# here is some example HTML in a template file:
#			    		<a href="" onclick="return ponclick('{ListToPlaceID}-total')">
#			    			Total Distance: {TotalMilesSwum} Miles, Total Time: {TotalTimeSwum}, 
#			    			Average 1 Mile time: {AverageOneMileTime}</a>
# Note that during template processing every occurance of a {xxx} is replaced with the value of the
# macro 'xxx'.
#
# PASSED:
#	$propertiesDir - the full path name of the directory holding the property file
#	$simplePropFileName - the (simple) file name of the property file (usually 'properties.txt')
#		NOTE:  the two together make a full path name to the property file, so it's not necessary
#		that $simplePropFileName is a simple name as long as "$propertiesDir . $simplePropFileName" is
#		a valid full path to the property file.
#	$yearBeingProcessed - the year we think we're processing (taken from the date of
#		processing or passed program parameter or some other application-dependent method.)
#		Any attempt to change the macro with the same name ('YearBeingProcessed')
#		will throw a non-fatal error and then be ignored.
#
# NOTES:
#	A line in this file looks like one of the following:
#			name
#		(in this case the macro named 'name' is set to the empty string.  Leading and trailing whitespace
#		 is removed from the name)
#			name    value
#		(in this case the macro named 'name' is asigned the value 'value'.  Leading and trailing whitespace
#		 is removed from the name and from the value)
# 	or
#			name    value;  \
#				more value;  \
#				more value;
#		(in this case the macro named 'name' is asigned the value 'value;more value;more value;'.  
#		 Leading and trailing whitespace is removed from the name and from the values)
#
sub GetProperties( $$$ ) {
	my ($propertiesDir, $simplePropFileName, $yearBeingProcessed) = @_;
	my $propFileFD;
	my $propFileName = $propertiesDir . "/" . $simplePropFileName;
	SetTemplateName( $propFileName );
	my $lineNum = 0;
	my $processingCalendar = 0;		# set to 1 when processing a ">calendar....>endcalendar" block
	open( $propFileFD, "< $propFileName" ) || die( "Can't open $propFileName: $!" );
	while( my $line = <$propFileFD> ) {
		my $value = "";
		$lineNum++;
		chomp( $line );
		$line =~ s/\s*#.*$//;		# remove optional spaces followed by comment
		$line =~ s/^\s+|\s+$//g;			# remove leading and trailing space

		# handle a continuation line
		while( $line =~ m/\\$/ ) {
			$line =~ s/\s*\\$//;		# remove (optional) whitespace followed by continuation char
			# special case:  if the entire line is a single word add a space so we find the 'name', e.g. the lines
			# look like this:
			#    name \
			#		value...
			if( ! ($line =~ m/\s/) ) {
				$line .= " ";
			}
			my $nextLine;
			last if( ! ($nextLine = <$propFileFD>) );		# get the next line
			$lineNum++;
			chomp( $nextLine );
			$nextLine =~ s/\s*#.*$//;		# remove optional spaces followed by comment
			$nextLine =~ s/^\s+|\s+$//g;			# remove leading and trailing space
			$line .= $nextLine;
		}

		next if( $line eq "" );		# if we now have an empty line then get next line
#print "GetProperties(): [$propFileName]: line='$line'\n";
		my $macroName = $line;
		$macroName =~ s/\s.*$//;	# remove all chars from first space char until eol
		if( ($macroName =~ m/^>/) || $processingCalendar ) {
			# found a non macro definition (synonym, etc of the form ">....")
			$macroName = lc( $macroName );
			if( $macroName eq ">calendar" ) {
				$processingCalendar = 1;
				next;
			} elsif( $macroName eq ">endcalendar" ) {
				$processingCalendar = 0;
				next;
			} elsif( $processingCalendar ) {
				ProcessCalendarPropertyLine($line, $yearBeingProcessed);
				next;
			} elsif( $macroName eq ">include" ) {
				$line = ProcessMacros( $line, $lineNum );		# allow include path to contain other macros
				ProcessInclude( $propertiesDir, $line, $yearBeingProcessed );
				next;
			} elsif( $macroName eq ">endoffile" ) {
				last;
			}
			# normal one line ">..." line:
			$line =~ s/^\S+\s+//;  # remove >xxx part (and following spaces)
			my $lineLC = lc( $line );
			my $name1 = $lineLC;
			$name1 =~ s/\s.*$//;	# first word following '>xxx   ' part (no imbedded spaces)
			my $name2 = $lineLC;
			$name2 =~ s/^[^\s]+\s+//;	# second word and all that follows
			my $count = 1;
			if( $macroName eq ">last" ) {
				# last name synonyms NOT USED
				print( "!!! ERROR: Illegal '>' property: '$line' (>last not supported)\n");
			} elsif( $macroName eq ">first") {
				# first name synonyms NOT USED
				print( "!!! ERROR: Illegal '>' property: '$line' (>first not supported)\n");
			} elsif( $macroName eq ">team" ) {
				# team name synonyms NOT USED
				print( "!!! ERROR: Illegal '>' property: '$line' (>team not supported)\n");
			} elsif( $macroName eq ">key" ) {
				# key synonyms
				print( "!!! ERROR: Illegal '>' property: '$line' (>key not supported)\n");
			} elsif( $macroName eq ">last,first" ) {
				# syntax:    >last,first   last,first   >	 Last,First[,Extra]
				# where
				#	>last,first is in any case
				#	last,first (name1) is in any case, can contain spaces, only one allowed (use last one seen), no commas, double quotes.
				#	Last,First[,Extra] (name2) is in correct case, and can contain an optional ",Extra"
				#	Any time we see a result and we extract the last,first (and extra if there) for the swimmer,
				#	we see if it matches name1 (case-
				#	insensitive).  If it does, we will use name2 (in Canonical form)
				$name1 = lc($line);
				$name1 =~ s/\s*>.*$//;		# remove second name
				$name2 = $line;
				$name2 =~ s/^.*>\s*//;	# remove first name and leading spaces, left with second name which may have a ,Extra
				PMSStruct::GetSynonymFirstLastNameRef()->{$name1} = $name2;
			} elsif( $macroName eq ">regnum" ) {
				# >regnum badRegNum goodRegNum
				# Reg Number synomyms, e.g.
				# >regnum 384D-D414R 384P-0414R
				my $name1 = uc($lineLC);
				$name1 =~ s/\s+[^\s]+$//;
				my $name2 = uc($lineLC);
				$name2 =~ s/^.+\s//;
				PMSStruct::GetSynonymRegNumRef()->{$name1} = $name2;
				
			} elsif( $macroName eq ">regnumname" ) {
				# >regnumName xxxxxx > last,first,middle	> yyyyyyy		where ,middle is optional
				# 	$lineLC = 'xxxxxx > last,first,middle	> yyyyyyy'  ALL LOWER CASE!
				# 	'xxxxxx' can be anything but no trailing spaces (spaces prior to '>' will be removed); no >
				# 	'yyyyyyy' does not contain spaces or >  WILL CONVERT TO UPPER CASE!
				# 	'last, first, middle can contain spaces; no commas, no >.  Must be name in PMS db
				$lineLC =~ m/^([^>]+)>([^>]+)>\s*(\S+)/;
				my $reg1 = $1;		# 'xxxxxx    '
				my $fullName = $2;	# '    last,first,middle     ' ALL LOWER CASE
				my $reg3 = uc($3);		# 'yyyyyyy'
				# clean it up
				$reg1 =~ s/\s*$//;			# 'xxxxxx'
				$fullName =~ s/^\s*//;		# remove leading spaces 'last,first,middle     ' or 'last,first     '
				$fullName =~ s/\s*$//;		# remove trailing spaces 'last,first,middle' or 'last,first'
				PMSStruct::GetSynonymRegNumRef()->{"$reg1>$fullName"} = $reg3;
			} elsif( $macroName eq ">error" ) {
				$lineLC =~ s/\s*//g;			# remove all whitespace
				#### do we need this?:  $PMSConstants::ErrorsToIgnore .= "|||$lineLC|||";			
			} else {
				print( "!!! ERROR: Illegal '>' property: '$line'\n");
			}
		} else {
			# we have a macro, but there are special cases where we IGNORE macros defined by property files:
			# Don't set the "YearBeingProcessed" macro to the value in the property file since it's required
			# that the yearBeingProcessed be known prior to reading the property file.
			if( $macroName eq "YearBeingProcessed" ) {
				print( "!!! (non-fatal) ERROR: It is illegal to attempt to set YearBeingProcessed " .
					"in a property file. Fix this line: '$line'\n");
				next;
			}
			# this is executed only if a non-empty or empty value is assigned to the property in the property file.
			if( $macroName eq $line ) {
				# empty value
				PMSStruct::GetMacrosRef()->{$macroName} = "";
			} else {
				# non-empty value
				$line = ProcessMacros( $line, $lineNum );		# allow values in property file to contain other macros
				$value = $line;
				$value =~ s/^[^\s]+\s+//;
				PMSStruct::GetMacrosRef()->{$macroName} = $value;
			}
			#PMSLogging::DumpNote( "", "", "macroname='$macroName', value='$value'" );
		}
	}
} # end of GetProperties





# ProcessInclude - This function is used to process an >include directive in a template file
#	Such a line is of the form ">include Historical/2015/2015-properties.txt"
# 
#
# PASSED:
#	$propertiesDir - the directory holding the property file that contains the >include directive.
#	$simplePropFileName - the file name of the property file.  It's basically the string following the
#		>include phrase.  Thus it could be a simple name, a partial path (as above), or a full path.
#	$yearBeingProcessed - 
#
# If the $simplePropFileName is a simple name or a partial path name then the $propertiesDir is prepended to
#	form the full path name of the property file to be included.
#
#		--- See GetProperties()
#	
sub ProcessInclude( $$$ ) {
	my $propertiesDir = $_[0];
	my $simplePropFileName = $_[1];
	my $yearBeingProcessed = $_[2];
	$simplePropFileName =~ s/^[^\s]+\s+//;		# get rid of the '>include   ' part
	my $value = $simplePropFileName;
	# is the "simple" name really a full path name?
	if( $value =~ m,^/, ) {
		# YES!  use it by itself
	} else {
		# NO! construct a full path name
		$value = $propertiesDir . "/" . $simplePropFileName;
	}
	# now, divide into full path directory and simple file name
	my $propertiesDir2 = dirname($value);
	my $propertiesFileName = basename($value);
	
	GetProperties( $propertiesDir2, $propertiesFileName, $yearBeingProcessed );
}



# ProcessCalendarPropertyLine - process the sequence of lines between the >calendar and >endcalendar 
#	directivies in a property file.
#
# PASSED:
#	calendar line - a line used to define one specific OW event.  This line tells us the file
#		containing the results, the category of the event, the date, the distance, the name
#		that we'll call that event in our Accumulated Points report, and a "UniqueID" that allows
#		us to recognize the exact same event across multiple years.
#	$yearBeingProcessed - See GetProperties()
#
# RETURNED:
#	n/a
#
# NOTES:
#
# Basically, all the important lines look like this:
#		file								CAT		date		 distance			event name   	UniqueID
#		name							  				  		(miles)	
#--------------------------------------------------------------------------------------
#	2014 Spring Lake 1 Mile=CAT1.csv	-> 	1	->	2014-05-17	->	1		->	 Spring Lake 1 Mile  ->  1
#
# Note:  the order of the races is implied by the order of lines in the property file, not the
#	date of the races.  This is due to the fact that there are often multiple races on the
#	same day and we want the order of events in the generated Accumulated Points page to
#	be deterministic.
#
my $raceOrder = 0;		# used to keep track of the order of races
sub ProcessCalendarPropertyLine($$) {
	my ($fileName, $cat, $date, $distance, $eventName, $uniqueID) = split( /\s*->\s*/, $_[0] );
	my $yearBeingProcessed = $_[1];
	if( !defined( $uniqueID ) ) {
		print( "GenerateOWResults::ProcessCalendarPropertyLine(): " .
			"Insufficient number of fields in a calendar entry: '$_[0]' (Ignore this line.)\n" );
		return;
	}
	$raceOrder++;
	$calendar{$raceOrder} = $fileName;
	$calendar{$fileName} = $raceOrder;
	$calendar{"$raceOrder-FileName"} = $fileName;
	$calendar{"$raceOrder-CAT"} = $cat;
	$calendar{"$raceOrder-Date"} = $date;
	$calendar{"$raceOrder-Distance"} = $distance;
	$calendar{"$raceOrder-EventName"} = "$eventName";
	$calendar{"$raceOrder-UniqueID"} = $uniqueID;
	
	# perform some sanity checks.  Errors won't cause the program to halt - just display the error to the user.
	# get the year of this event and make sure it's the year we're processing
	# (A date is of the form "2015-05-16")
	my $eventYear = $date;
	$eventYear =~ s/-.*$//;
	if( $eventYear != $yearBeingProcessed ) {
		# oops!
        print "PMSMacros::ProcessCalendarPropertyLine(): WARNING: Found the event " .
        	"'$eventName' " .
        	"with the date of $date, but we are supposed to be processing data in the year of " .
        	"'$yearBeingProcessed'.\n";
	}
	
} # end of ProcessCalendarPropertyLine()



# GetCalendarValue - accessor function to give us the value of one field of one specific event.
#
# PASSED:
#	field - the field desired.  See ProcessCalendarPropertyLine() to see the different fields.
#
# RETURNED:
#	field value - a concatenation of the "race order" with the field name, in the form:
#			xxx-zzz
#		where
#			xxx is the order of the race (we count all races for the year, starting at 1)
#		and
#			zzz is the desired field.
#
# NOTES:
#	For example:
# 			my $cat = PMSMacros::GetCalendarValue( "3-CAT" );
#	will return the category of what we're calling the 3rd race of the year.
#
sub GetCalendarValue( $ ) {
	return $calendar{$_[0]};
}


# GetCalendarRef - return a reference to the %calendar hash
sub GetCalendarRef() {
	return \%calendar;
}



# ProcessMacros - process all template macros in the passed line, if any.
#
# PASSED:
#	$line - the line to be processed.  May or may not contain one or more macros.
#	$lineNum - the number of the line in the template file.  Used for error messages.
#
# RETURNED:
#	$line - the passed line with all macros replaced with their values.
#
sub ProcessMacros {
	my $line = $_[0];
	my $lineNum = $_[1];
	my ($posLeft, $posRight) = 0;
	my $count = 0;
	
	while( ($posLeft != -1) && ($count < 200) )  {
		$posLeft = index( $line, "{", $posLeft );
		if( $posLeft != -1 ) {
			$count++;
		
			# If the { is immediatly followed by whitespace OR end of string we'll ignore it (support for javascript)
			if( (($posLeft+1) >= length( $line )) || (substr( $line, $posLeft+1, 1 ) =~ m/\s/ ) ) {
				$posLeft++;
				next;
			}
			
			$posRight = index( $line, "}", $posLeft+1 );
			if( $posRight != -1 ) {
				my $length = $posRight - $posLeft - 1;
				my $macroName = substr( $line, $posLeft+1, $length );
				my $substitute = PMSStruct::GetMacrosRef()->{$macroName};
				if( ! defined( $substitute ) ) {
					die( "!!! Error on line $lineNum in $templateName: unknown macro '$macroName'\n" );
				} else {
					$line = substr( $line, 0, $posLeft ) . $substitute . substr( $line, $posRight + 1 );
					$posLeft = 0;		# re-process the entire line for another macro
				}
			} else {
				die( "!!! Error on line $lineNum in $templateName (Probably missing '}' in template file: posRight='$posRight'): $!\n" );
			}
		}
	}
	
	if( $count >= 200 ) {
		die( "!!! Error on line $lineNum: macro expansion infinite loop (>$count)': $!\n" );
	}
	
	return $line;
} # end of ProcessMacros





#
# ValidateCalendar - Validate the calendar we read in our property file.  If something is wrong then
#	log an error.  If we discover an event we don't know about then add it to our database.
#
# NOTE:
#	When reading the property file we don't yet have a logging system nor a database handle set up, so
#	we must postpone the work done here until those things are set up.  We can't set up the database
#	connection until we have logging, and we can't set up logging until we've read our properties.
#
sub ValidateCalendar() {
	
	for( my $i = 1; ; $i++ ) {
		last if( !defined $calendar{$i} );		
		my $eventName = $calendar{"$i-EventName"};
		my $eventUniqueID = $calendar{"$i-UniqueID"};
		my $category = $calendar{"$i-CAT"};
		my $eventDate = $calendar{"$i-Date"};
		my $distance = $calendar{"$i-Distance"};
		
		# we have a calendar entry containing a unique event, etc.
		# do we have this event in our history?
		my $dbh = PMS_MySqlSupport::GetMySqlHandle();
		my ($sth2,$rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
			"SELECT EventName FROM EventHistory " .
			"WHERE UniqueEventID = '$eventUniqueID' AND " .
			"Category = '$category'" );
		if( defined(my $resultHash = $sth2->fetchrow_hashref) ) {
			# we have this UniqueEventID in our Event History - does it look like what we expect?
			if( $eventName ne $resultHash->{'EventName'} ) {
	        	PMSLogging::DumpError( "", "", "PMSMacros::ValidateCalendar(): Found an event " .
	        		"in the EventHistory with UniqueEventID='$eventUniqueID' but the name of the event in " .
	        		"the EventHistory is '" . $resultHash->{'EventName'} . "' which doesn't match the " .
	        		"name '$eventName' found in the Events table.", 1 );
			} # else we have this event in our history - go on to the next one
		} else {
			# we don't have this particular event in our event history - add it
			($sth2,$rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
	    		"INSERT INTO EventHistory " .
	    		"(UniqueEventID, EventName, Distance, Category) " .
	    		"VALUES (\"$eventUniqueID\", \"$eventName\", \"$distance\", \"$category\")" );
	    	my $eventHistoryId = $dbh->last_insert_id(undef, undef, "EventHistory", "EventHistoryId");
	    	if( !defined( $eventHistoryId ) ) {
	        	PMSLogging::DumpError( 0, 0, "MaintainOWSwimmerHistory::UpdateEventsHistory(): Unable to " .
	        	"INSERT into EventHistory with VALUES (\"$eventUniqueID\", \"$eventName\", \"$distance\", \"$category\")", 1 );
	    	}
		}
	} # end of for( ...
	
} # end of ValidateCalendar()

1;  # end of module

