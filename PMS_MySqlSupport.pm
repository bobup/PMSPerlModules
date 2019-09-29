#!/usr/bin/perl -w
# PMS_MySqlSupport.pm - support routines and values used by the MySQL based code.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMS_MySqlSupport;

use DBI;

use lib 'PMSPerlModules';
use PMSConstants;
use PMSLogging;
require PMSUtil;
require PMSStruct;


use strict;
use sigtrap;
use warnings;

###############################################################
######## General Infrastructure ###############################
###############################################################


# initialized when the database is initialized:

# the %databaseParameters hash is used to store the parameters for every database used by the application using
# this module.  Every set of parameters is associated with an id (known as the 'dbid', e.g. "default" or "TopTen").  
# The parameters for the database are stored in this hash like this:
#	$databaseParameters{'default-host'} = the server hosting the database.
#	$databaseParameters{'default-database'} = the name of the database.
#	$databaseParameters{'default-user'} = the user of the database.
#	$databaseParameters{'default-password'} = the password for the database.
#	$databaseParameters{'default-handle'} = the handle to the open database, or -1 if not open, or 0 if we
#		tried to open the database but failed..
my %databaseParameters = ();


# this static is used to allow us to report swimmer age errors that are only warnings.
my $SwimmerAgeWarningAlreadyReported = 0;


#***************************************************************************************************
#************************************** General MySql Support Routines *****************************
#***************************************************************************************************


# SetSqlParameters - called by the user of this module to set the MySql parameters for the database being used.
#
# PASSED:
#	$dbid - a unique identifier for the database being used.  The caller can use multiple databases.
#	$host -
#	$database -
#	$user -
#	$password -
#
# RETURNED:
#	n/a
#
sub SetSqlParameters( $$$$$ ) {
	$databaseParameters{"$_[0]-host"} = $_[1];
	$databaseParameters{"$_[0]-database"} = $_[2];
	$databaseParameters{"$_[0]-user"} = $_[3];
	$databaseParameters{"$_[0]-password"} = $_[4];
	$databaseParameters{"$_[0]-handle"} = -1;	
} # end of SetSqlParameters()


# GetMySqlHandle - return a handle to the MySQL database requested.  If no specific database is requested
#	then return the handle to the "default" one.  Return 0 if we're unable to get a handle.  The
#	reason will be logged.
#
# PASSED:
#	$dbid (optional) - if specified this will specify the database whose handle is to be returned.  If not
#		specified then the dbid 'default' will be used.
# RETURNED:
#	The database handle, or 0 if there is an error.
#
sub GetMySqlHandle {
	my $dbid = $_[0];
	my $dbh = 0;
	my ($sth, $rv, $status);
	if( !defined $dbid ) {
		$dbid = "default";
	}
	my $host = $databaseParameters{"$dbid-host"};
	my $database = $databaseParameters{"$dbid-database"};
	my $user = $databaseParameters{"$dbid-user"};
	my $password = $databaseParameters{"$dbid-password"};
	$dbh = $databaseParameters{"$dbid-handle"};
	if( $dbh == -1 ) {
		# first attempt at getting the connection
		#print "\nNOTE:  Using MySQL ($dbid: $database) for storage.\n";
		$dbh = DBI->connect("DBI:mysql:database=$database;host=$host", $user, $password);
		if( !defined($dbh) ) {
        	PMSLogging::DumpError( 0, 0, "PMS_MySqlSupport::GetMySqlHandle(): ABORT: failed to get the " .
        		"DB handle for '$dbid': '" . DBI::errstr . "'", 1 );
        	#$dbh = $Mysql::db_errstr;		# ignore this - here to remove compiler warning
        	$dbh = 0;
        	$databaseParameters{"$dbid-handle"} = 0;
        	# it really does no good to move on, so we're going to die!
        	die "ABORT!!";
		} else {
        	$databaseParameters{"$dbid-handle"} = $dbh;
        	($sth, $rv, $status) = PrepareAndExecute( $dbh, "SET collation_connection = 'utf8_general_ci'" );
			if( $status ne "" ){
        		PMSLogging::DumpError( 0, 0, "PMS_MySqlSupport::GetMySqlHandle(): Failed to set " .
        			"SESSION variable: '$status'", 1 );
			}
        	($sth, $rv, $status) = PrepareAndExecute( $dbh, "SET collation_database = 'utf8_general_ci'" );
			if( $status ne "" ){
        		PMSLogging::DumpError( 0, 0, "PMS_MySqlSupport::GetMySqlHandle(): Failed to set " .
        			"SESSION variable: '$status'", 1 );
			}
        	($sth, $rv, $status) = PrepareAndExecute( $dbh, "SET collation_server = 'utf8_general_ci'" );
			if( $status ne "" ){
        		PMSLogging::DumpError( 0, 0, "PMS_MySqlSupport::GetMySqlHandle(): Failed to set " .
        			"SESSION variable: '$status'", 1 );
			}
			if(0) {
        	($sth, $rv, $status) = PrepareAndExecute( $dbh, "SHOW SESSION VARIABLES LIKE 'collation_%';" );
			while( defined( my $ary_ref = $sth->fetchrow_arrayref ) ) {
				my $varName = $ary_ref->[0];
				my $varValue = $ary_ref->[1];
				print "var $varName = $varValue\n";
			} # end of while(...
        	($sth, $rv, $status) = PrepareAndExecute( $dbh, "SHOW SESSION VARIABLES LIKE 'character_%';" );
			while( defined( my $ary_ref = $sth->fetchrow_arrayref ) ) {
				my $varName = $ary_ref->[0];
				my $varValue = $ary_ref->[1];
				print "var $varName = $varValue\n";
			} # end of while(...
			}
			
		}
	}
	return $dbh;
} # end of GetMySqlHandle()



# CloseMySqlHandle - close the handle to the specified MySQL database.  If no specific database is requested
#	then close the handle to the "default" one.  If the handle is not a valid connection or an error
#	occurs we just ignore it and return.
#
# PASSED:
#	$dbid (optional) - if specified this will specify the database whose handle is to be closed.  If not
#		specified then the dbid 'default' will be used.
#
# RETURNED:
#	n/a
#
sub CloseMySqlHandle {
	my $dbid = $_[0];
	my $dbh = 0;
	if( !defined $dbid ) {
		$dbid = "default";
	}
	my $database = $databaseParameters{"$dbid-database"};
	$dbh = $databaseParameters{"$dbid-handle"};
	if( $dbh != -1 ) {
		# looks like a valid handle
		print "\nNOTE:  Closing the handle to the MySQL database ($dbid: $database).\n";
		$dbh->disconnect();
		$databaseParameters{"$dbid-handle"} = -1;
	}
} # end of CloseMySqlHandle()




# GetTableList - update the passed tableList hash to indicate what db tables we currently have.
#
# PASSED:
#	$tableListRef - a reference to a hash of db tables
#	$tableListInitializedRef - a reference to a variable that indicates whether or not the 
#		tableList has been initialized.
#	$dbid (optional) - if specified this will specify the database whose handle is to be returned.  If not
#		specified then the dbid 'default' will be used.
#
# RETURNED:
#	$tableListRef->{tableName} is set to 1 if the table 'tableName' exists in our db, untouched if not.
#	$tableListInitializedRef - set to 1 the first time we initialize our tableListRef
#	
#
sub GetTableList {
	my ($tableListRef, $tableListInitializedRef) = @_;
	my $dbid = $_[2];
	if( !defined $dbid ) {
		$dbid = "default";
	}
	my $dbh = PMS_MySqlSupport::GetMySqlHandle($dbid);
	my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};
	if( !$$tableListInitializedRef ) {
		$tableListRef->{"RSIDN_$yearBeingProcessed"} = 0;		# the RSIDN table we use for this execution
		my($sth, $rv) = PrepareAndExecute( $dbh, "SHOW TABLES" );
		while( defined( my $ary_ref = $sth->fetchrow_arrayref ) ) {
			my $tableName = $ary_ref->[0];
			if( !defined( $tableListRef->{$tableName}) ) {
				if( $tableName !~ m/^RSIDN/ ) {
					# allow any table that begins with "RSIDN" - otherwise, this is a warning
					PMSLogging::DumpWarning( "", "", "PMS_MySqlSupport::GetTableList(): Current db has " .
						"a table named $tableName which is not expected.", 1 );
				}
			} else {
				$tableListRef->{$tableName} = 1;
			}
		} # end of while(...
		$$tableListInitializedRef = 1;
	}
} # end of GetTableList()




# PrepareAndExecute - prepare and execute a SQL query.  Die with message on error.
#
# PASSED:
#	$dbh - database handle
#	$qry - the query to prepare and execute
#	$log - (optional) non-empty string means to log the query, "" (or undefined = default) means don't
#			The value of $log is used as the title of the log entry, if any.
#
# Return:
#	$sth - statement handle
#	$rv - result value (handle)
#	$status - If all is good this will be an empty string, otherwise it's a rather cryptic and
#		short error message.
#
#  e.g.:
#    ($sth, $rv, $status) = PrepareAndExecute( $dbh, 
#    			"CREATE TABLE Events (EventId INT AUTO_INCREMENT PRIMARY KEY, " .
#    			"EventName Varchar(200), EventFullPath Varchar(400), Distance INT, " .
#    			"DistanceUnits Varchar(20))" ); 
#
#
sub PrepareAndExecute {
	my $dbh = $_[0];
	my $qry = $_[1];
	my $log = $_[2];
	my $rv = 0;
	my $sth = 0;
	my $i;
	my $status = "x";			# assume no error
	my $weRetried = 0;			# we haven't had to retry...yet
	for( $i = 3; ; $i++ ) {
		if( !defined( $_[$i] ) ) {
			$i--;
			last;
		}
	}
	
	if( !defined( $log ) || $log eq "" ) {
		$log = "";
	}
	
	if( $log ne "" ) {
		PMSLogging::DumpNote( "", "", "$log: $qry" );
	}
	
	# NOTE: The following loop is here so that we can recover a lost database handle.
	# Experience has shown that the DB handle may become stale and the following execute 
	# will fail with this error:
	#	DBD::mysql::st execute failed: Lost connection to MySQL server during query at....
	# which is caused by:
	#   DBD::mysql::db do failed: MySQL server has gone away at....
	# This has only been seen on the PMS Linux web server.
	for( my $tryCount=1; ($status ne "") && ($tryCount < 3); $tryCount++ ) {
		$status = "";
		eval {
		$sth = $dbh->prepare( $qry );
			if( !$sth ) {
		    	$status = "Can't prepare: '$qry'\n";
			} else {
			    if( $i >= 3 ) {
			# todo: need to fix this to be more general
			        $rv = $sth->execute($_[3], $_[4] );
			        if( !$rv ) { 
			        	my $errStr = $sth->errstr;
			    		$status = "Can't execute-1: '$qry' (error: '$errStr')\n";
			        }
			    } else {
			        $rv = $sth->execute;
			        if( !$rv ) { 
			        	my $errStr = $sth->errstr;
			    		$status = "Can't execute-2: '$qry' (error: '$errStr')\n";
			        }
			    }
			}
			$status eq "";
		} or do {
			if( $@ ) {
				$status .= "...Exception thrown: $@\n";
			}
		};

		if( $status ) {
			# got an error - try to recover
			$weRetried++;
			PMSLogging::DumpWarning( "", "", "PMS_MySqlSupport::PrepareAndExecute(): $status (retrying #$weRetried...)", 1);
			PMSUtil::PrintStack();
			if( $sth ) {
				# we're done with this statement handle, too.  We'll get another when we loop and try again
				$sth->finish;
			}
			PMS_MySqlSupport::CloseMySqlHandle();
			$dbh = PMS_MySqlSupport::GetMySqlHandle();
		}
	} # end of for( ...
	if( $status ) {
		# got an error - failed to recover
		PMSLogging::DumpError( "", "", "PMS_MySqlSupport::PrepareAndExecute(): $status (retry FAILED!)", 1);
	} elsif( $weRetried ) {
		PMSLogging::DumpWarning( "", "", "PMS_MySqlSupport::PrepareAndExecute(): We retried $weRetried " .
			"times and it finally worked.", 1);
	}
	return( $sth, $rv, $status );
} # end of PrepareAndExecute()





#	PMS_MySqlSupport::DropTables( \%tableList, \@tableListNotDropped);

# DropTables - drop (almost) all (existing) tables in our db
#
sub DropTables( $$ ) {
	my( $tableListRef, $tableListNotDroppedRef ) = @_;
	my $dbh = GetMySqlHandle();
	my $qry = "DROP tables ";
	my $gotTableToDrop = 0;
	
	# construct the DROP TABLES query:
	foreach my $tableName (keys %$tableListRef) {
		# $tableName contains the name of a table - does it exist in our database?
		if( $tableListRef->{$tableName} ) {
			# $tableName contains the name of an existing table - do we drop it?
			my $dropThisTable = 1;		# assume we will drop this table...
			foreach my $dontDrop ( @$tableListNotDroppedRef ) {
				# this table is one that we must NOT drop!
				$dropThisTable = 0 if( $tableName =~ m/$dontDrop/ );
			}
			if( $dropThisTable ) {
				print "Table '$tableName' exists - dropping it.\n";
				$qry .= ", " if( $gotTableToDrop );
				$qry .= $tableName;
				$gotTableToDrop = 1;
				# update our cache to show that this table doesn't exist
				$tableListRef->{$tableName} = 0;
			}
		}
	}
	
	if( $gotTableToDrop ) {
		# Execute the DROP query
		my $sth = $dbh->prepare( $qry ) or 
	    	die "PMS_MySqlSupport::DropTables(): Can't prepare: '$qry'\n";
	    my $rv;
	    $rv = $sth->execute or 
	    	die "PMS_MySqlSupport::DropTables(): Can't execute: '$qry'\n"; 
	}   
} # end of DropTables()




# $dbiErrorString is used by DBIErrorHandler and initialized by DBIErrorPrep().  To get more information
# about a DBI error use the following sequence:
# ...
# DBIErrorPrep( "Some useful info just in case the next DBI call fails" );
# my ($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
# DBIErrorPrep( "" );
# ...
my $dbiErrorString = "";

sub DBIErrorPrep( $ ) {
	$dbiErrorString = $_[0];
	return 1;
} # end of DBIErrorPrep()

sub DBIErrorHandler( $$$ ) {
	my ($message, $handle, $value) = @_;
	$value = "(undefined)" if( !defined $value );
	TT_Logging::PrintLog( "DBI failure.  Custom message: '$dbiErrorString', handle='$handle', value='$value'.  This is fatal - abort with DBI message!" );
	die( $message );
} # end of DBIErrorHandler()




# GetFullTeamName - get the full name for the passed (PMS) team abbr.
#
# PASSED:
#	$abbr - team abbr of a valid PMS team
#
# RETURNED:
#	$fullName - the full name, or "" if unknown (i.e. $abbr isn't a valid PMS team)
#
sub GetFullTeamName( $ ) {
	my $abbr = $_[0];
	my $fullName = "";
	my $dbh = GetMySqlHandle();

	my ($sth,$rv) = PrepareAndExecute( $dbh,
		"SELECT FullTeamName FROM PMSTeams where TeamAbbr = '$abbr'" );
	if( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		$fullName = $resultHash->{'FullTeamName'};
	}
	return $fullName;
} # end of GetFullTeamName()

###############################################################
######## Points Calculation ###################################
###############################################################




#
## InsertSwimmerIntoMySqlDB - insert the passed swimmer into our DB if necessary (if not already there)
##	or update the swimmer's age2 and AgeGroup.  This assumes that this routine is called for every
##	swim the swimmer participates in, in order of swims.
##	This will also insert their regnum into the RegNums table if it's not already there.
##	Also update ReferencedTeams table.
##
## Passed:
##	$dateOfBirth
##	$regNum - the regnum supplied when this swimmer entered the event being processed.  This
##		regnum will be entered into the RegNums table if not already there for this swimmer.  
##		This regnum WILL NOT be used to populate the regnum field of the created Swimmer
##		table row unless we confirm that this swimmer is a PMS swimmer.  The passed regnum
##		is guaranteed to be a syntactically valid regnum (as confirmed by PMSUtil::GenerateCanonicalRegNum())
##		which means it could be $PMSConstants::INVALID_REGNUM.
##	$firstName
##	$middleInitial
##	$lastName
##	$gender
##	$age - their age at the time of the swim
##	$ageGrp - their age group at the time of the swim
##	$genAgeGrpRace - uniquely identifies a race, e.g. gender:age/swim#/category (not used!?)
##	$raceFileName - the simple file name of the file holding the results of the race we're processing 
##		(the last simple name in the fileName path)
##	$team - team initials, e.g. WCM.  Since this came from results it may not be accurate.
##		We'll truncate if necessary
##	$eventId
##	$recordedPlace - the place they got in the passed race according to the event results
##		(May be changed later if non-PMS swimmers finished ahead of this person)
##
## Returned:
##	$resultSwimmerId - Id of passed swimmer entered (or previously entered) in the DB.  0 if error
##  $regNum - the regnum that we decide really belongs to this swimmer.  It might be exactly
##		what was passed for $regNum, or modified slightly to be put into cononical form, or 
##		completely different if we find an authoritative regNum for this swimmer (from the RSIDN
##		file.)  It could be 0 if there is an error.
##	$isPMS - 1 if the passed swimmer is confirmed to be a PMS swimmer, 0 otherwise.
#
# todo: add team to db (may not be official) - are we doing this already?
#
sub InsertSwimmerIntoMySqlDB( $$$$$$$$$$$$$ ) {
	(my $dateOfBirth, my $regNum, my $firstName, my $middleInitial, my $lastName,
		my $gender, my $age, my $ageGrp, my $genAgeGrpRace, my $raceFileName, 
		my $team, my $eventId, my $recordedPlace ) = @_;
	my $swimmerRegNum = 0;		# RegNumId of this swimmer in the swimmer table
	my $sth, my $rv;
	my $resultHash;
	my $age2;					# fetched below
	my $ageGroup;				# fetched below
	my $isPMS = -1;				# set to 0 if we determine that this swimmer is NOT a PMS
								# swimmer, set to 1 if we determine that they are.  Leave
								# as -1 until we know for sure.
	my $resultSwimmerId = 0;
	my $registeredTeam = "";	# team this swimmer is really registered with (if PMS) - should be
								# the same as $team but may not be.  Empty string if non-PMS or
								# this swimmer is already in the Swimmer table.
	my $resultMissingDataType = "";
	my $resultErrorNote = "";
	my $resultErrorRegnum = "";
	my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};

	# debugging...look for specific first/last name to log lots of details
	my $debugLastName = 'Nuñez-Zeped';
	
	if( (lc($lastName) eq lc($debugLastName)) ) {
		print "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): got '$firstName' '$middleInitial' '$lastName'\n";
		print "...raceFileName='$raceFileName', eventId='$eventId', regnum=$regNum\n";
	}

	# get ready to use our database:
	my $dbh = GetMySqlHandle();
	
	# make sure we have something for their birth date
	my $dateOfBirthDef = PMSUtil::GenerateCanonicalDOB($dateOfBirth);		# yyyy-mm-dd

	# See if this swimmer with the passed regnum is already in our database:
	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT SwimmerId, Age2, AgeGroup, DateOfBirth FROM Swimmer " .
			"WHERE RegNum = \"$regNum\" " .
			"AND FirstName = \"$firstName\" " .
#			"AND MiddleInitial = \"$middleInitial\" " .
			"AND ( " .
				"   (MiddleInitial = '$middleInitial') " .
				"OR (MiddleInitial IS NULL) " .
				"OR ('$middleInitial' = '') ) " .
			"AND LastName = \"$lastName\"", "" );
	if( defined($resultHash = $sth->fetchrow_hashref) ) {
		if( (lc($lastName) eq lc($debugLastName)) ) {
			print "found $lastName in Swimmer table via RegNum $regNum\n";
		}
		my $found = 0;
		# Is there more than one row that matches our query?  If so that's weird
		while( defined($resultHash) ) {
			if( ++$found > 1 ) {
				print "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): Found multiple rows for swimmer " .
					"firstname='$firstName', middleInitial='$middleInitial', lastName='$lastName', regNum='$regNum, " .
					"swimmerId='" . $resultHash->{'SwimmerId'} . "'";
			}
			# this swimmer is already in our DB with a regnum - this means that this swimmer
			# is a valid PMS swimmer and we have everything we need to know in our db
			# - get their db id
			$resultSwimmerId = $resultHash->{'SwimmerId'};
			$age2 = $resultHash->{'Age2'};
			$ageGroup = $resultHash->{'AgeGroup'};
			$isPMS = 1;
			my $dob = $resultHash->{'DateOfBirth'};
			# validate the supplied age compared against their date of birth: as of 2017 the age of 
			# the swimmer is their age on December 31 of this year.
			my $computedAge = PMSUtil::AgeAtEndOfYear( $dob );
			if( ($yearBeingProcessed >= $PMSConstants::YEAR_RULE_CHANGE) && ($computedAge != $age) ) {
				# The entered age is not correct.  We'll report this ONCE as a warning if it doesn't 
				# really matter (i.e. the swimmer's age group doesn't change), but we'll generate an
				# ERROR for every one we find where the age group is wrong.
				if( PMSUtil::DifferentAgeGroups( $computedAge, $age ) ) {
					# ages are different age groups (or invalid...!!?!?!?!)
					PMSLogging::DumpError( "", "", "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): invalid age[#1] " .
						"(AND AGE GROUP!) for " .
						"'$firstName $lastName' in file\n    $raceFileName." .
						"The 'dob='$dob', entry age='$age', but computed age based on dob='$computedAge'.\n" .
						"    This swimmer will still get their points, BUT IT MIGHT BE IN THE WRONG AGE GROUP!", 1);
				} else {
					# this error doesn't affect the swimmer's age group - report it only once as a warning:
					if( ($SwimmerAgeWarningAlreadyReported == 0) || 1 ) {
						$SwimmerAgeWarningAlreadyReported = 1;
						PMSLogging::DumpWarning( "", "", "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): " .
						"[not REPORTED ONLY ONCE]: invalid age[#1] for " .
							"'$firstName $lastName' in file $raceFileName.\n  " .
							"The 'dob='$dob', entry age='$age', but computed age based on dob='$computedAge'.", 0);
					}
				}
				# we will ignore this error and keep going...what else can we do?
			}
			#print "Found swimmer: swimmerId=$resultSwimmerId, regNumId=$SwimmersRegNumId, swimmer=$firstName $lastName\n";
			# this next statement is only here to support the while() above, which is only there to catch the unlikely error
			# where the same swimmer with the same regnum is in our Swimmer table twice.
			$resultHash = $sth->fetchrow_hashref;
			# update this swimmer's Age2 and AgeGroup if necessary:
			HandleAgeUp( $resultSwimmerId, $age, $age2, $ageGroup, $raceFileName, $firstName, $lastName );
		}
	} else {
		# one of these is true:
		# - this swimmer is in the Swimmer table with no regnum
		# - this swimmer is in the Swimmer table with a different regnum
		# - this swimmer isn't in our Swimmer table yet
		# also, one of these is true:
		# - this swimmer is a PMS swimmer and we can find them in the RSIDN file
		# - this swimmer is not a PMS swimmer and we can't find them in the RSIDN file
		# So here is what we're going to do:  See if this swimmer is in our RSIDN file.
		# If they are they might have a different reg number and/or different name.
		# If that's the case, so be it - that's how we'll identify this swimmer.
		# But if the swimmer is NOT in the RSIDN file then we know for sure they are
		# not a PMS swimmer.  In that case we'll just see if they are in our DB and, 
		# if not, add them.  In any case they will remain non-PMS forever.
		if( (lc($lastName) eq lc($debugLastName)) ) {
			print "...swimmer not found in Swimmer table via regnum $regNum.\n";
		}
		my ($correctedFirstName, $correctedMiddleInitial, 
			$correctedLastName, $correctedRegNum, $correctedTeam, $correctedDOB, $rsidnId);
			
		($correctedFirstName, $correctedMiddleInitial, 
			$correctedLastName, $correctedRegNum, $correctedTeam, $correctedDOB,
			$resultMissingDataType, $resultErrorNote, $resultErrorRegnum, $rsidnId) = 
				LookUpSwimmerInRSIDN( $firstName, $middleInitial, $lastName, $regNum, 
					$dateOfBirthDef, $gender, $team, $age, $recordedPlace );

		if( $correctedRegNum eq "" ) {
			# This swimmer is NOT known in the RSIDN
#			# We log this if the reg # implies that this swimmer is a PMS swimmer:
#			if( substr( $regNum, 0, 2 ) eq "38" ) {
#				PMSLogging::DumpNote( "", 0, "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): " .
#					"Name + reg # not known in PAC member database: $firstName $middleInitial $lastName : " .
#					"$regNum", 0 );
#			}
			# This swimmer is not known in RSIDN
			if( (lc($lastName) eq lc($debugLastName)) ) {
				print "...swimmer not found in RSIDN_$yearBeingProcessed, corrected name: " .
					"'$correctedFirstName' '$middleInitial' $correctedLastName'.\n";
			}

			$isPMS = 0;
			$registeredTeam = $team;		# the only team we know about that they are associated with
			if( length($registeredTeam) > $PMSConstants::MAX_LENGTH_TEAM_ABBREVIATION ) {
				$registeredTeam = substr( $registeredTeam, 0, $PMSConstants::MAX_LENGTH_TEAM_ABBREVIATION );
			}
			# NOTE:  we'll leave their $regNum the same as what was passed (it may be invalid or
			# it may be a regNum for a different LMSC, or it could be a valid PMS regNum but not
			# belonging to the passed swimmer.)
		} else {
			# This swimmer is known in the RSIDN

			if( (lc($lastName) eq lc($debugLastName)) ) {
				print "...swimmer was found in RSIDN_$yearBeingProcessed, correctedRegNum=$correctedRegNum, " .
				"correctedDOB='$correctedDOB'.  corrected name: " .
					"'$correctedFirstName' '$middleInitial' $correctedLastName'.\n";
			}
			$regNum = $correctedRegNum;
			$registeredTeam = $correctedTeam;
			$isPMS = 1;
			# Check for age error:  as of 2017 the age of the swimmer is their age on December 31 of this year.
			my $computedAge = PMSUtil::AgeAtEndOfYear( $correctedDOB );
			
			
			
			if( ($yearBeingProcessed >= $PMSConstants::YEAR_RULE_CHANGE) && ($computedAge != $age) ) {
				# The entered age is not correct.  We'll report this ONCE as a warning if it doesn't 
				# really matter (i.e. the swimmer's age group doesn't change), but we'll generate an
				# ERROR for every one we find where the age group is wrong.
				if( PMSUtil::DifferentAgeGroups( $computedAge, $age ) ) {
					# ages are different age groups (or invalid...!!?!?!?!)
					PMSLogging::DumpError( "", "", "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): invalid age[#2] " .
						"(AND AGE GROUP!) for " .
						"'$firstName $lastName' in file\n    $raceFileName." .
						"The 'dob='$correctedDOB', entry age='$age', but computed age based on dob='$computedAge'.\n" .
						"    This swimmer will still get their points, BUT IT MIGHT BE IN THE WRONG AGE GROUP!", 1);
				} else {
					# this error doesn't affect the swimmer's age group - report it only once as a warning:
					if( ($SwimmerAgeWarningAlreadyReported == 0) || 1 ) {
						$SwimmerAgeWarningAlreadyReported = 1;
						PMSLogging::DumpWarning( "", "", "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): " .
						"[not REPORTED ONLY ONCE]: invalid age[#2] for " .
							"'$firstName $lastName' in file \n    $raceFileName." .
							"The 'dob='$correctedDOB', entry age='$age', but computed age based on " .
							"dob='$computedAge'.\n    This swimmer will still get their points.", 0);
					}
				}
				# we will ignore this error and keep going...what else can we do?
			}			
#			PMSLogging::DumpNote( "", 0, "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): " .
#				"resultRegNum 2: $regNum, $registeredTeam", 1 );
		}
		# If they are PMS ($isPMS = 1):  we now know their real name and reg number
		# If they are not PMS ($isPMS = 0):  we have a name and "invalid" (non-pms) reg number
		# Are they already in our Swimmer table?
		($sth, $rv) = PrepareAndExecute( $dbh,
			"SELECT SwimmerId, Age2, AgeGroup, RegNum FROM Swimmer " .
				"WHERE FirstName = \"$correctedFirstName\" " .
				"AND DateOfBirth = \"$correctedDOB\" " .
				"AND Gender = \"$gender\" " .
				"AND ( " .
					"   (MiddleInitial = '$correctedMiddleInitial') " .
					"OR (MiddleInitial IS NULL) " .
					"OR ('$correctedMiddleInitial' = '') ) " .
				"AND LastName = \"$correctedLastName\"", (lc($lastName) eq lc($debugLastName)) );
		if( defined($resultHash = $sth->fetchrow_hashref) ) {
###			# yep - this swimmer is in our db (based on their full name only)  ###--31may2016: dob and gender too but not middle
			if( (lc($lastName) eq lc($debugLastName)) ) {
				print "found $lastName in Swimmer table via dob '$dateOfBirthDef', gender '$gender'\n";
			}
			$resultSwimmerId = $resultHash->{'SwimmerId'};
			my $found = 0;
			# Is there more than one row that matches our query?  If so that's weird
			while( defined($resultHash) ) {
				if( (lc($lastName) eq lc($debugLastName)) ) {
					print "...swimmer found in Swimmer table by name ($correctedFirstName " .
					"$correctedMiddleInitial $correctedLastName, regnum in swimmer table=" .
					$resultHash->{'RegNum'} . ", correctedRegNum=$correctedRegNum.\n";
				}
				if( ++$found > 1 ) {
					print "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): Found multiple rows for swimmer " .
						"firstname='$firstName', middleInitial='$middleInitial', lastName='$lastName', " .
						"swimmerId='" . $resultHash->{'SwimmerId'} . "'\n";
				}
				if( $isPMS ) {
					# they better have the expected PMS regnum or else something went wrong!
					if( $resultHash->{'RegNum'} eq $correctedRegNum ) {
						# this is the passed swimmer.  Return their SwimmerId.
						$regNum = $resultHash->{'RegNum'};
						$age2 = $resultHash->{'Age2'};
						$ageGroup = $resultHash->{'AgeGroup'};
						# update this swimmer's Age2 and AgeGroup if necessary:
						HandleAgeUp( $resultSwimmerId, $age, $age2, $ageGroup, $raceFileName, $firstName, $lastName );

#						PMSLogging::DumpNote( "", 0, "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): " .
#							"resultRegNum 3: $regNum", 1 );
					} else {
						# This will happen if this is not the first time we've seen this swimmer, and a previous
						# time we saw them we had a valid PMS name but an invalid regnum, so we entered them into
						# the Swimmer table with their valid name but 0 for a regnum and isPMS = false.  
						# Now, in
						# this case we have their correct reg number, so we need to update them in the Swimmer
						# table.
						$regNum = $correctedRegNum;
						$age2 = $resultHash->{'Age2'};
						$ageGroup = $resultHash->{'AgeGroup'};
						# update this swimmer's Age2 and AgeGroup if necessary:
						HandleAgeUp( $resultSwimmerId, $age, $age2, $ageGroup, $raceFileName, $firstName, $lastName );
						UpdateRegNumForPMSSwimmer( $resultSwimmerId, $regNum, $rsidnId );
					}
				} # end of if( $isPMS ...
				else {
					# this is a non-PMS swimmer, but they are still stored in our db, so we need to return their ID
				}
				# are there any more results?  There will be if we have 2 or more swimmers with exactly
				# the same first, middle, and last name, which is interesting and tells us to look carefully
				# at our results!
				$resultHash = $sth->fetchrow_hashref;
			} # end of while( ... 
		} # end of yep - this swimmer is in our db (based on their full name and dob and gender)  
		else {
			# swimmer is NOT in our Swimmer table - put them in
			if( (lc($lastName) eq lc($debugLastName)) ) {
				print "...swimmer NOT in Swimmer table by name ('$correctedFirstName' " .
				"'$correctedMiddleInitial', '$correctedLastName', " .
				"dob='$dateOfBirthDef', gender='$gender', " .
 				"regnum=" . $regNum . ", correctedRegNum=$correctedRegNum, isPMS='$isPMS'.  ADD THEM\n";
			}
			my $insertedRegNum = 0;	# assume non-pms
			$insertedRegNum = $regNum if( $isPMS );
			my $unqKey = "$firstName|$middleInitial|$lastName";
			($sth, $rv) = PrepareAndExecute( $dbh,
				"INSERT INTO Swimmer " .
					"(FirstName, MiddleInitial, LastName, Gender, RegNum, " .
					"Age1, Age2, AgeGroup, RegisteredTeamInitials, DateOfBirth, " .
					"isPMS, RSIDN_ID) " .
					"VALUES (\"$correctedFirstName\", \"$correctedMiddleInitial\", \"$correctedLastName\", " .
					"\"$gender\", \"$insertedRegNum\", " .
					"\"$age\", \"$age\", \"$ageGrp\", \"$registeredTeam\", \"$correctedDOB\", " .
					"$isPMS, \"$rsidnId\")") ;
			# get the SwimmerId of the swimmer we just entered into our db
	    	$resultSwimmerId = $dbh->last_insert_id(undef, undef, "Swimmer", "SwimmerId");
	    	die "Can't determine SwimmerId of newly inserted Swimmer" if( !defined( $resultSwimmerId ) );
			# create a link from the passed regnum to this swimmer, but first make sure one
			# doesn't already exist.
			LinkRegNumToSwimmer( $resultSwimmerId, $regNum );
		} # end of swimmer is NOT in our db...
	}  # end of one of these is true...

	# all done updating the Swimmer table.  Now, associate this swimmer with the team they entered
	# with (and also the team they are really registered with) if necessary.
	AssociateSwimmerWithTeams( $resultSwimmerId, $registeredTeam, $team );
	
	# if we found any errors we'll update the MissingData table with details
	if( $resultMissingDataType ne "" ) {
		PMS_MySqlSupport::LogInvalidSwimmer( $resultMissingDataType, $resultSwimmerId, $resultErrorRegnum,
			$eventId, $resultErrorNote );
	}

#	PMSLogging::DumpNote( "", 0, "PMS_MySqlSupport::InsertSwimmerIntoMySqlDB(): " .
#		"return swimmerId: '$resultSwimmerId', regNum: '$regNum', isPMS: '$isPMS', " .
#		"registeredTeam: '$registeredTeam'", 1 );

	return ($resultSwimmerId, $regNum, $isPMS);
} # end of InsertSwimmerIntoMySqlDB()





#						UpdateRegNumForPMSSwimmer( $resultSwimmerId, $regNum );
# UpdateRegNumForPMSSwimmer - set the regnumber for the passed swimmer, and also set them as a PMS swimmer.
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#	$regNum - the regnum to be assigned to this swimmer.
#	$rsidnId -
#
sub UpdateRegNumForPMSSwimmer( $$$ ) {
	my( $swimmerId, $regNum, $rsidnId ) = @_;
	my $dbh = GetMySqlHandle();

    my($sth,$rv) = PrepareAndExecute( $dbh, 
		"UPDATE Swimmer SET RegNum = '$regNum', isPMS = '1', RSIDN_ID = '$rsidnId' " .
		"WHERE SwimmerId = $swimmerId" );
	
	# link the passed regnum to the passed swimmer if necessary
	LinkRegNumToSwimmer( $swimmerId, $regNum );
} # end of UpdateRegNumForPMSSwimmer()



#			LinkRegNumToSwimmer( $resultSwimmerId, $regNum );
# LinkRegNumToSwimmer - update the RegNums table to create a link from the passed regNum to the
#	passed swimmer IF such a link doesn't yet exist.
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#	$regNum - the regnum to be assigned to this swimmer.
#
sub LinkRegNumToSwimmer( $$ ) {
	my( $swimmerId, $regNum ) = @_;
	my $dbh = GetMySqlHandle();

	# does this link already exist?
	my ($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT RegNumsId FROM RegNums " .
			"WHERE SwimmerId = \"$swimmerId\"" .
			"AND RegNum = \"$regNum\"" );
	if( !defined(my $resultHash = $sth->fetchrow_hashref) ) {
		# this swimmer isn't yet associated with the passed regnum - link them together
		($sth, $rv) = PrepareAndExecute( $dbh,
			"INSERT INTO RegNums " .
				"(RegNum, Affiliation, SwimmerId) " .
				"VALUES (\"$regNum\", \"\", \"$swimmerId\")" );
	}
} # end of LinkRegNumToSwimmer()




# 	AssociateSwimmerWithTeams( $resultSwimmerId, $registeredTeam, $team );
# AssociateSwimmerWithTeams - update the ReferencedTeams table for the passed swimmer.
#
# PASSED:
#	$swimmerId - the passed swimmer
#	$registeredTeam - the team that this swimmer is registered with, if any.  Assumed to
#		be a valid team abbreviation.
#	$team - the team this swimmer said they were swimming for when entering a race, if any.
#		May not be a valid team abbreviation; we'll truncate it if necessary.
#	
# Notes:  the $registeredTeam and $team may be the same; either or both can be empty.
#
sub AssociateSwimmerWithTeams( $$$ ) {
	my($swimmerId, $registeredTeam, $team) = @_;
	my( $sth, $rv, $resultHash );
	my $dbh = GetMySqlHandle();
	
	# is this swimmer already associated with the passed $registeredTeam?
	if( $registeredTeam ne "" ) {
		($sth, $rv) = PrepareAndExecute( $dbh,
			"SELECT ReferencedTeamsId FROM ReferencedTeams " .
				"WHERE SwimmerId = \"$swimmerId\"" .
				"AND TeamAbbr = \"$registeredTeam\"" );
		if( !defined($resultHash = $sth->fetchrow_hashref) ) {
			# this swimmer isn't yet associated with the passed team - link them together
			($sth, $rv) = PrepareAndExecute( $dbh,
				"INSERT INTO ReferencedTeams " .
					"(TeamAbbr, SwimmerId) " .
					"VALUES (\"$registeredTeam\", \"$swimmerId\")" );
		}
	}
	
	# is this swimmer already associated with the passed $team?
	if( length($team) > $PMSConstants::MAX_LENGTH_TEAM_ABBREVIATION ) {
		$team = substr( $team, 0, $PMSConstants::MAX_LENGTH_TEAM_ABBREVIATION );
	}
	if( ($registeredTeam ne $team) && ($team ne "") ) {
		($sth, $rv) = PrepareAndExecute( $dbh,
			"SELECT ReferencedTeamsId FROM ReferencedTeams " .
				"WHERE SwimmerId = \"$swimmerId\"" .
				"AND TeamAbbr = \"$team\"" );
		if( !defined($resultHash = $sth->fetchrow_hashref) ) {
			# this swimmer isn't yet associated with the passed team - link them together
			($sth, $rv) = PrepareAndExecute( $dbh,
				"INSERT INTO ReferencedTeams " .
					"(TeamAbbr, SwimmerId) " .
					"VALUES (\"$team\", \"$swimmerId\")" );
		}
	}
} # end of AssociateSwimmerWithTeams()


# GetListOfTeamsForSwimmer - return a comma-separated list of teams associated with the passed swimmer.
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#
sub GetListOfTeamsForSwimmer( $ ) {
	my $swimmerId = $_[0];
	my $result = "";
	my( $sth, $rv, $resultHash );
	my $dbh = GetMySqlHandle();

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT TeamAbbr FROM ReferencedTeams " .
			"WHERE SwimmerId = \"$swimmerId\"" );
	while( defined($resultHash = $sth->fetchrow_hashref) ) {
		$result .= "," if( $result ne "" );
		$result .= $resultHash->{'TeamAbbr'};
	}
	return $result;
} # end of GetListOfTeamsForSwimmer()


# GetListOfRegNumsForSwimmer - return a comma-separated list of reg numbers associated with the passed swimmer.
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#
sub GetListOfRegNumsForSwimmer( $ ) {
	my $swimmerId = $_[0];
	my $result = "";
	my( $sth, $rv, $resultHash );
	my $dbh = GetMySqlHandle();

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT RegNum FROM RegNums " .
			"WHERE SwimmerId = \"$swimmerId\"" );
	while( defined($resultHash = $sth->fetchrow_hashref) ) {
		$result .= "," if( $result ne "" );
		$result .= $resultHash->{'RegNum'};
	}
	return $result;
} # end of GetListOfRegNumsForSwimmer()



# GetDOBForSwimmer - return the date of birth for the passed swimmer
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#
sub GetDOBForSwimmer( $ ) {
	my $swimmerId = $_[0];
	my $result = "?";
	my( $sth, $rv, $resultHash );
	my $dbh = GetMySqlHandle();

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT DateOfBirth FROM Swimmer " .
			"WHERE SwimmerId = \"$swimmerId\"" );
	if( defined($resultHash = $sth->fetchrow_hashref) ) {
		$result = $resultHash->{'DateOfBirth'};
	}
	return $result;
} # end of GetDOBForSwimmer()



# GetAgeGroupsForSwimmer - return the 1 or 2 age groups for the passed swimmer
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#
sub GetAgeGroupsForSwimmer( $ ) {
	my $swimmerId = $_[0];
	my $result = "?";
	my( $sth, $rv, $resultHash );
	my $dbh = GetMySqlHandle();

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT Age1, Age2, AgeGroup FROM Swimmer " .
			"WHERE SwimmerId = \"$swimmerId\"" );
	if( defined($resultHash = $sth->fetchrow_hashref) ) {
		my $age1 = $resultHash->{'Age1'};
		my $age2 = $resultHash->{'Age2'};
		$result = $resultHash->{'AgeGroup'};
		if( ($age1 != $age2) && ($age2 != 20) && (($age2 % 5) == 0) ) {
			# they have two age groups!  The other age group is one age group LESS than
			# the one we already have.
			my $previousAgeGroup = PMSUtil::DecrementAgeGroup( $result );
			$result = $previousAgeGroup . ", " . $result;
		}
	}
	return $result;
} # end of GetAgeGroupsForSwimmer()



# GetListOfSwimsForSwimmer - return a list of hashes describing each swim for the passed swimmer, ordered
#	by ComputedPlace and then by swim date (oldest first)
# Ignore swims that don't have a positive computed place - such swims were recorded when the swimmer 
#	wasn't recognized as a PMS swimmer.
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#	$category - the category of swims we are interested in
#	$orderByPlace - (optional) if defined and true then the list returned will be ordered by
#		place (highest place first).  For example, if the swimmer swam in events 1, 2, 4, and 5,
#		scoring places 3rd, 5th, 1st, 2nd, respectively, then the list returned will be in the
#		order:  event 4, event 5, event 1, and event 2.  If false, or not defined, the list
#		will be in order of event date, the oldest event first.  Same example, the
#		list returned would be event 1, event 2, event 4, and event 5.
#
# RETURNED:
#	@result - Array of hashes - looks like this:
#		$result[1] = reference to %swim
#			$swim->{'EventId'} = unique event id
#
sub GetListOfSwimsForSwimmer {
	my $swimmerId = $_[0];
	my $category = $_[1];
	my $orderByPlace = $_[2];		# optional
	$orderByPlace = 0 if( !defined $orderByPlace );		# order by event #
	my @result = ();
	my( $sth, $rv, $resultHash );
	my $dbh = GetMySqlHandle();
	my $orderBy = "Events.EventId ASC";
	$orderBy = "Swim.ComputedPlace ASC, Events.Date ASC" if( $orderByPlace );

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT Swim.EventId, Events.EventName, Swim.RecordedPlace, Swim.ComputedPlace, " .
		"Events.Distance, Swim.Duration, Events.Date " .
		"FROM Swim JOIN Events " .
			"WHERE Swim.SwimmerId = \"$swimmerId\"" .
			"AND Swim.ComputedPlace > 0 " .
			"AND Swim.EventId = Events.EventId " .
			"AND Events.Category = \"$category\" " .
			"ORDER BY $orderBy", "");
	while( defined($resultHash = $sth->fetchrow_hashref) ) {
		my $swimRef = { 
			"EventId" => $resultHash->{'EventId'},
			"EventName" => $resultHash->{'EventName'},
			"RecordedPlace" => $resultHash->{"RecordedPlace"},
			"ComputedPlace" => $resultHash->{"ComputedPlace"},
			"Distance" => $resultHash->{'Distance'},
			"Duration" => $resultHash->{'Duration'},		# hundredths of a second
			"Date" => $resultHash->{'Date'},
		};
		push @result, $swimRef;
	}
	return @result;	
} # end of GetListOfSwimsForSwimmer()


# GetTieBreakingReasonsForSwimmer - return a string of reasons for deciding all ties for the passed swimmer.
#	Return an empty string if the swimmer was not part of a tie, or was part of a tie but it was decided that
#	the passed swimmer was the "slowest" of all the swimmers they were tied with.
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#	$category - the category of swims we are interested in
#
# RETURNED:
#	$result - string of reasons (should only be 1 reason)
#
sub GetTieBreakingReasonsForSwimmer( $$ ) {
	my $swimmerId = $_[0];
	my $category = $_[1];
	my $fieldName = "Cat" . $category . "Reason";
	my $result="";
	my( $sth, $rv, $resultHash );
	my $dbh = GetMySqlHandle();

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT $fieldName as Reason FROM Swimmer " .
			"WHERE SwimmerId = \"$swimmerId\"" );
	if( defined($resultHash = $sth->fetchrow_hashref) ) {
		$result = $resultHash->{'Reason'};
	}
	return $result;	
} # end of GetTieBreakingReasonsForSwimmer()





# 						HandleAgeUp( $resultSwimmerId, $age, $age2, $ageGroup, $raceFileName, $firstName, $lastName );
# HandleAgeUp - update the passed swimmer's age group if their passed age is in a higher age group when compared
#	with their passed Age2 value from their Swimmer row.
#
# PASSED:
#	$swimmerId - the swimmerId of the Swimmer row for this swimmer
#	$age - the age of the swimmer at the time of an event
#	$age2 - the oldest age of the swimmer we've seen prior to this event.
#	$ageGroup - the age group of the swimmer prior to this event.
#	$raceFileName - the simple file name of the file holding the results of the race we're processing 
#		(the last simple name in the fileName path)
#	$firstName -
#	$lastName -
#
# RETURNED:
#	n/a
#
# NOTES:
#	As of 2017 there will no longer be swimmers who "age-up", because the age group of a swimmer is determined 
#	by their age on December 31 of the year of the event.  Thus, if an "age-up" is detected when processing 
#	an open water event for any year beyond and including 2017 then an error will be generated.  (We will
#	still allow age-up for years prior to and including 2016 for historical purposes.)
#	Definition:  "age-up" means the age of a swimmer changes during the season and causes them to change
#	age groups.  However, for the purposes of this function we will only look for a change in their age.  If
#	we find a change (even if it doesn't case an age-up) we'll generate an error.  
#
sub HandleAgeUp( $$$$$ ) {
	my($swimmerId, $age, $age2, $ageGroup, $raceFileName, $firstName, $lastName) = @_;
	my $dbh = GetMySqlHandle();

	if( $age > $age2 ) {
		# this swimmer's age changed during the year - not allowed on or past 2017:
		my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};
		if( $yearBeingProcessed >= $PMSConstants::YEAR_RULE_CHANGE ) {
			PMSLogging::DumpError( "", "", "PMS_MySqlSupport::HandleAgeUp(): Invalid age for " .
				"swimmerid $swimmerId ($firstName $lastName) in file $raceFileName.\n    " .
				"Their age should not change during the year.  Previous age seen: $age2, new age seen: $age", 1 );
			# continue with the possible age-up... (what else can we do...?)
		}
		# they had a birthday during the open water season...did they age up?
		if( ($age != 20) && (($age % 5) ==0) ) {
			# yep!
			my $nextAgeGroup = PMSUtil::IncrementAgeGroup( $ageGroup );
			PMSLogging::DumpNote( "", 0, "PMS_MySqlSupport::HandleAgeUp(): " .
				"SwimmerId $swimmerId ($firstName $lastName) aged up from $age2 ($ageGroup) to $age ($nextAgeGroup)\n", 2 );
    		my($sth,$rv) = PrepareAndExecute( $dbh, 
				"UPDATE Swimmer SET Age2 = $age, AgeGroup = '$nextAgeGroup' " .
				"WHERE SwimmerId = $swimmerId" );
		} else {
			# they didn't "age up" (go into an older age group), but they did change their age
    		my($sth,$rv) = PrepareAndExecute( $dbh, 
				"UPDATE Swimmer SET Age2 = $age " .
				"WHERE SwimmerId = $swimmerId" );
		}
	}
} # end of HandleAgeUp()






# LookUpSwimmerInRSIDN - see if the passed name is in the RSIDN (PMS DB)
#
#    This routine is passed a swimmer's name, reg number, birthdate, gender, team, and age.  
#		Age is used only for logging.
#    The name has a first and last name, possibly a middle initial.
#    The reg number is either a properly formatted reg number or $PMSConstants::INVALID_REGNUM.
#    The birthdate is a properly formatted birthdate (yyyy-mm-dd) OR $PMSConstants::INVALID_DOB.
#    The purpose of this routine is to tell us if the swimmer is a PMS swimmer, and
#    if they are, tell us their reg number.  Possibilities:
#    	1) the passed name and regnum matches name and regnum in RSIDN: is a pms swimmer, return regnum
#		2) the passed name doesn't match a name in RSIDN, but the regnum does match a regnum in
#			RSIDN:  fuzzy match the associated name in RSIDN.  If the fuzzy match score is >=0
#			then it's likely this is a PMS swimmer.  However, in all cases we will NOT assume that
#			the swimmer is a PMS swimmer - we will generate a synonym in the log file and then
#			it's up to the user to add the synonym to the property file.
#    	3) the passed name is in RSIDN only once (but regnum doesn't match):  We will 
#			NOT assume that this swimmer is a valid
#			PMS swimmer BUT we'll log all descripancies in the match.  We'll generate a synonym
#			in the log file and then it's up to the user to add the synonym to the property file.
#    	4) passed name in RSIDN 2+ times, regnum found in RSIDN and matches one of the names: is pms, 
#			return regnum
#    	5) passed name in RSIDN 2+ times, regnum found in RSIDN and matches 0 of the names: non-pms, 
#			return 0
#    	6) passed name in RSIDN 2+ times, passed regnum is INVALID_REGNUM: non-pms, return 0

# remove this:
#    	3) the passed name is in RSIDN only once, passed regnum is INVALID_REGNUM: SECOND LEVEL CHECK (below)
#    	4) the passed name is in RSIDN only once, passed regnum non-empty but doesn't match the
#    		found swimmer (but MAY match a different swimmer in RSIDN): SECOND LEVEL CHECK (below)
#    	5) passed name in RSIDN 2+ times, regnum found in RSIDN and matches one of the names: is pms, 
#			return regnum
#    	6) passed name in RSIDN 2+ times, regnum found in RSIDN and matches 0 of the names: non-pms, 
#			return 0
#    	7) passed name in RSIDN 2+ times, passed regnum is INVALID_REGNUM: non-pms, return 0
#
#    SECOND LEVEL CHECK:  In this case we'll do another test:
#    	10) The passed name matches only one swimmer in RSIDN, so if the supplied birthdate matches 
#			the birthdate in the RSIDN table then this swimmer is pms so return the corresponding 
#			regnum from RSIDN table, otherwise 11)
#		11) The passed name matches only one swimmer in RSIDN, so if the supplied birthdate is $PMSConstants::INVALID_DOB 
#			and the gender and team in the RSIDN table matches the gender and team of the passed swimmer then this
#			swimmer is pms so return the corresponding regnum from the RSIDN table, otherwise return 0.
#
# PASSED:
#	firstname, middleinitial, lastname - the name to search for.  The middleinitial is used
#		only if it's non-empty.
#	$regNum - the reg num supplied for the passed swimmer.  May be PMSConstants::$INVALID_REGNUM. 
#	$birthDate - the birthdate supplied for the passed swimmer.  Of the form yyyy-mm-dd. It could
#		be "0000-00-00" ($PMSConstants::INVALID_DOB)
#	$gender - the gender of the swimmer noted in the results.
#	$team - the team of the swimmer noted in the results.
#	$age - the age of the swimmer noted in the results.
#	$recordedPlace
#	
# RETURNED:
#	($resultFirstName, $resultMiddleInitial, $resultLastName) - this swimmer's
#		actual name from RSIDN.  Will be the same as the passed names if they are not found in RSIDN, or
#		if they are found but the names are identical.
#	$resultRegNum - the regnum found in the RSIDN table for the passed swimmer, or "".
#		If this person is not found in the RSIDN table then they are NOT a PMS swimmer thus
#		"" is returned.
#	$resultTeam - The passed $team unless RSIDN tells us that they are registered with a different
#		team.
#	$resultDOB - the passed $birthDate unless RSIDN tells us differently.
#	$resultMissingDataType, 
#	$resultErrorNote - formatted for HTML and logging - needs a bit of conversion prior to displaying via
#		HTML or in the log file.  When printing HTML change all '\n' to '<br>' and '\t# ' to nothing.
#		When printing to the log file change '<b>' and '</b>' to nothing.
#	$resultErrorRegnum - 
#
#
# If this swimmer is found in the RSIDN table more than once (multiple people with the same first/last
#	name [and middleinitial if used]) then warning will be generated.  If the passed $regNum is associated
#	with one of the RSIDN entries then $regNum is returned.  Otherwise "" is returned.
#
sub LookUpSwimmerInRSIDN( $$$$$$$$ ) {
	my ($firstName, $middleInitial, $lastName, $regNum, $birthDate, $gender, 
		$team, $age, $recordedPlace) = @_;
	$middleInitial = "" if( !defined($middleInitial) );		# make sure we have a defined middle initial
	# we are going to use the swimmer's USMSSwimmerId, not their full reg number:
	my $USMSSwimmerId = $regNum;
	$USMSSwimmerId =~ s/^.*-//;
	my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};

	# make it easier to log the middle initial whether missing or not:
	my $printableMiddleInitial = " ";
	$printableMiddleInitial = " '$middleInitial' " if( $middleInitial ne "" );
	# prepare log:
	my $logSupplied = "\t# <b>Supplied (w/ synonyms):</b> '$firstName'$printableMiddleInitial'$lastName': regNum=$regNum, " .
		"(USMS SwimmerId=$USMSSwimmerId), birthDate=$birthDate, gender=$gender, " .
		"team=$team, age=$age, recorded place=$recordedPlace.\n";
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $sth, my $rv, my $status;
	my @RSIDNFirstName = ();
	my @RSIDNMiddleInitial = ();
	my @RSIDNLastName = ();
	my @RSIDNRegNum = ();
	my @RSIDNUSMSSwimmerID = ();
	my @RSIDNTeam = ();
	my @RSIDNGender = ();
	my @RSIDNBirthDate = ();
	my @RSIDNId = ();
	my $logRSIDN = "";
	
	
	
	my $debugLastName = "xxxxx";
#	use utf8;
#	utf8::decode($debugLastName);

	# initialize the returned values - assume the returned names are the same as the passed names
	my $resultFirstName = $firstName;
	my $resultMiddleInitial = $middleInitial;
	my $resultLastName = $lastName;
	my $resultRegNum = "";	# assume this person is not a PMS swimmer
	my $resultTeam = $team;
	my $resultAge = $age;
	my $resultDOB = $birthDate;
	my $resultMissingDataType = "";		# assume no error with this swimmer
	my $resultErrorNote = "";
	my $resultErrorRegnum = "";
	my $resultRsidnId = 0;
	
	# search the PMS DB for this person's name:
	my $count=0;
	my $query = "SELECT MiddleInitial, RegNum, USMSSwimmerId, RegisteredTeamInitialsStr, " .
		"Gender, DateOfBirth, RSIDNId " .
		"FROM RSIDN_$yearBeingProcessed where FirstName = '" .
		MySqlEscape($firstName) .
		"' AND LastName = '" .
		MySqlEscape($lastName) .
		"'";
		
	if( (lc($debugLastName) eq lc($lastName)) &&
		1 ) {
			print "PMS_MySqlSupport::LookUpSwimmerInRSIDN(): Work on $firstName '$middleInitial' $lastName, " .
				"passed regNum='$regNum', query='$query'\n";
		}
		
	($sth,$rv, $status) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query, "" );
	while( my $resultHash = $sth->fetchrow_hashref ) {
		$RSIDNMiddleInitial[$count] = $resultHash->{'MiddleInitial'};
		$RSIDNFirstName[$count] = $firstName;
		$RSIDNLastName[$count] = $lastName;

		if( (lc($debugLastName) eq lc($lastName))) {
			print "PMS_MySqlSupport::LookUpSwimmerInRSIDN(): Looking for '$firstName' '$middleInitial' '$lastName' " .
				"in RSIDN_$yearBeingProcessed, found names #$count are: '$RSIDNFirstName[$count]' " .
				"'$RSIDNMiddleInitial[$count]' '$RSIDNLastName[$count]'\n";
		}


		# this swimmer we found in RSIDN could be the swimmer passed IF the first and 
		# last name matches, AND:
		#	- the RSIDN swimmer has no middle initial (regardless of the passed swimmer), OR
		#	- the passed swimmer has no middle initial (regardless of the RSIDN swimmer), OR
		#	- the passed swimmer and RSIDN swimmer have the same middle initial
		# This way the following will happen:
		#	John L Smith matches John L Smith and John Smith
		#	John Smith matches John Smith and John L Smith
		#	John L Smith DOES NOT MATCH John K Smith
		if( ($middleInitial eq "") || 
			($RSIDNMiddleInitial[$count] eq "") ||
			(lc $middleInitial eq lc $RSIDNMiddleInitial[$count]) ) {
			# name found at least once - does the passed regnum match this swimmer's?
			$RSIDNUSMSSwimmerID[$count] = $resultHash->{'USMSSwimmerId'};
			$RSIDNRegNum[$count] = $resultHash->{'RegNum'};
			$RSIDNBirthDate[$count] = $resultHash->{'DateOfBirth'};
			$RSIDNTeam[$count] = $resultHash->{'RegisteredTeamInitialsStr'};
			$RSIDNGender[$count] = $resultHash->{'Gender'};
			$RSIDNId[$count] = $resultHash->{'RSIDNId'};
			# make it easier to log the middle initial whether missing or not:
			my $RSIDNprintableMiddleInitial = " ";
			$RSIDNprintableMiddleInitial = " '$RSIDNMiddleInitial[$count]' " if( $RSIDNMiddleInitial[$count] ne "" );
			$logRSIDN .= "\t# <b>PAC database:</b> '$firstName'$RSIDNprintableMiddleInitial" .
				"'$lastName': regNum=$RSIDNRegNum[$count], " .
				"(USMS SwimmerId=$RSIDNUSMSSwimmerID[$count]), " .
				"birthDate=$RSIDNBirthDate[$count], gender=$RSIDNGender[$count], team=$RSIDNTeam[$count].\n";
			if( $USMSSwimmerId eq $RSIDNUSMSSwimmerID[$count] ) {
				# cases 1,4)  found swimmer's name and regnum in RSIDN - they are a pms swimmer
				$resultRegNum = $RSIDNRegNum[$count];
				$resultTeam = $RSIDNTeam[$count];
				$resultMiddleInitial = $RSIDNMiddleInitial[$count];
				$resultDOB = $RSIDNBirthDate[$count];
				$resultRsidnId = $RSIDNId[$count];
				# at this point we found our swimmer and they have the correct regnum, so we're 
				# done.
				$count++;
				last;
			}
			$count++;
		}
	}
	# At this point $count represents the number of swimmers we FOUND in RSIDN with the passed name.
	# (There may actually be more swimmers with the passed name but if we found an exact match we
	# stopped looking for others.)
	# All that is left is to handle the case where we failed to find the swimmer by name + regnum
	
	if( $resultRegNum eq "" ) {
		# EITHER name not found in RSIDN or name was found in RSIDN but corresponding regnum in 
		# RSIDN did not match the passed regnum - check for name match or regnum match
		if( $count == 0 ) {
			# the name didn't match anyone - check regnum to see if we have a fuzzy match
			($sth,$rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
				"SELECT RSIDNId, FirstName, MiddleInitial, LastName, RegisteredTeamInitialsStr, " .
				"Gender, DateOfBirth, RegNum FROM RSIDN_$yearBeingProcessed where USMSSwimmerId = '$USMSSwimmerId'", "" );
			if( defined( my $resultHash = $sth->fetchrow_hashref ) ) {
				# found the regnum - see if the names are close
				$RSIDNId[0] = $resultHash->{'RSIDNId'};
				$RSIDNFirstName[0] = $resultHash->{'FirstName'};
				$RSIDNMiddleInitial[0] = $resultHash->{'MiddleInitial'};
				$RSIDNLastName[0] = $resultHash->{'LastName'};
				$RSIDNUSMSSwimmerID[0] = $USMSSwimmerId;
				$RSIDNRegNum[0] = $resultHash->{'RegNum'};
				$RSIDNBirthDate[0] = $resultHash->{'DateOfBirth'};
				$RSIDNTeam[0] = $resultHash->{'RegisteredTeamInitialsStr'};
				$RSIDNGender[0] = $resultHash->{'Gender'};
				# make it easier to log the middle initial whether missing or not:
				my $RSIDNprintableMiddleInitial = " ";
				$RSIDNprintableMiddleInitial = " '$RSIDNMiddleInitial[0]' " if( $RSIDNMiddleInitial[0] ne "" );
				$logRSIDN = "\t# <b>PAC database:</b> '$RSIDNFirstName[0]'$RSIDNprintableMiddleInitial" .
					"'$RSIDNLastName[0]': regNum=$RSIDNRegNum[0], " .
					"(USMS SwimmerId=$RSIDNUSMSSwimmerID[0]), " .
					"birthDate=$RSIDNBirthDate[0], gender=$RSIDNGender[0], team=$RSIDNTeam[0].\n";
				my $fuzzyScore;
				if( ($fuzzyScore = PMSUtil::NamesCompareOK2( $firstName, $middleInitial, $lastName, 
					$RSIDNFirstName[0], $RSIDNMiddleInitial[0], $RSIDNLastName[0] )) == 0 ) {
					# case 1:  in this case the regnum and name match.  This is a strange case to handle 
					# a strange situation:  the SELECT failed to match the names even though they really do match.
					# This is usually the case when utf8 chars are used in the names and the database isn't
					# comparing them correctly.  This should be fixed...
					$resultFirstName = $RSIDNFirstName[0];
					$resultMiddleInitial = $RSIDNMiddleInitial[0];
					$resultLastName = $RSIDNLastName[0];
					$resultRegNum = $RSIDNRegNum[0];
					$resultRsidnId = $RSIDNId[0];
					PMSLogging::DumpWarning( "", "", "PMS_MySqlSupport::LookUpSwimmerInRSIDN(): This query failed:\n" .
						"    $query\n    But the name and regnum match an RSIND entry exactly.  We will assume they \n" .
						"    are a PMS swimmer, but the SELECT failure should be investigated.", 1 );
				} elsif( $fuzzyScore > 0 ) {
					# case 2)  we found this swimmer's regnum in the RSIDN table, and the associated name is "close"
					# to the swimmer's name, so WE MAY have the right swimmer
					$resultFirstName = $RSIDNFirstName[0];
					$resultMiddleInitial = $RSIDNMiddleInitial[0];
					$resultLastName = $RSIDNLastName[0];
					# log the error:
					$resultMissingDataType = "PMSFuzzyNameWithRegnum";
					$resultErrorRegnum = $regNum;

					$resultErrorNote = ConstructSynonym( $lastName, $firstName, $middleInitial, $RSIDNLastName[0],
						$RSIDNFirstName[0], $RSIDNMiddleInitial[0], 
						"[Good Fuzzy match ($fuzzyScore):]" .
						"\n$logSupplied\t# " .
						"entered a race but their reg number belongs to" .
						"\n$logRSIDN\t# These two names are similar " .
						"so these may be the same person.  If so, use this message to create a synonym." .
						"\n\t# We will assume this swimmer is NOT a PacMasters swimmer until the appropriate synonym " .
						"is created." );
	
					$resultRegNum = "";	# don't accept "close" names - make them fix it in the
						# results or with a ">last,first" property
				} else {
					# case 2)  the name is not a fuzzy match to the owner of this regnum
						
					# log the error:
					$resultMissingDataType = "PMSRegNoName";
					$resultErrorRegnum = $regNum;
						
					$resultErrorNote = ConstructSynonym( $lastName, $firstName, $middleInitial, $RSIDNLastName[0],
						$RSIDNFirstName[0], $RSIDNMiddleInitial[0], 
						"[Bad Fuzzy match ($fuzzyScore):]" .
						"\n$logSupplied\t# " .
						"entered a race but their reg number belongs to" .
						"\n$logRSIDN\t# These two names are NOT similar " .
						"so these are not likely to be the same person.  " .
						"We will assume this swimmer is NOT a PacMasters swimmer." );
					
					$resultRegNum = "";
				}
			} else {
				# name not found in RSIDN and neither is regnum.  No easy way to "fuzzy-match" 
				# the name and/or regnum in the RSIDN table so we're just going to declare this
				# swimmer as a non-pms swimmer.
				# log the error:
				$resultMissingDataType = "PMSNoRegNoName";
				$resultErrorRegnum = $regNum;
				$resultErrorNote = 
					"Neither Name nor Regnum found " .
						"in PAC database.\n$logSupplied\t# We will assume this swimmer is NOT a PacMasters swimmer.";
			}
			
		} elsif( $count == 1 ) {
			# case 3) we found the name exactly once but the regnum didn't match, so we're
			# going to NOT assume that this swimmer is a PMS swimmer, BUT we'll log everything and
			# leave it to the user to confirm our assumption.
			# log the error:
			$resultMissingDataType = "PMSBadRegButName";
			$resultErrorRegnum = $regNum;
			$resultErrorNote = ConstructSynonym( $lastName, $firstName, $middleInitial, $RSIDNLastName[0],
				$RSIDNFirstName[0], $RSIDNMiddleInitial[0], 
				"[Found name in PAC database exactly once, but regnum didn't match:]" .
				"\n$logSupplied $logRSIDN" .
				"\t# We will assume this swimmer is NOT a PacMasters swimmer." );

			} # end of case 3....
		else { # $count > 1
			# cases 5,6) we found 2+ entries in RSIDN whose name matches the passed name, but none of them have a
			# matching regnum - assume non-pms.
			# log the error:
			$resultMissingDataType = "PMSNamesButNoRegnum";
			$resultErrorRegnum = $regNum;
			$resultErrorNote .= 
				"# [Found name in PAC database 2+ times, " .
					"but nothing else (including regnum) clearly makes this swimmer a PacMasters swimmer.]\n$logSupplied" .
					"\t# We will assume this swimmer is NOT a PMS swimmer, but here are some possibilities:\n";
			for( my $i=0; $i < $count; $i++ ) {
				my $syn = ConstructSynonym( $lastName, $firstName, $middleInitial, $RSIDNLastName[$i],
					$RSIDNFirstName[$i], $RSIDNMiddleInitial[$i], ($i+1) . 
					": regNum=$RSIDNRegNum[$i], (USMS SwimmerId=$RSIDNUSMSSwimmerID[$i]), " .
					"birthDate=$RSIDNBirthDate[$i], gender=$RSIDNGender[$i], team=$RSIDNTeam[$i]" );
				$resultErrorNote .= "$syn\n";
				# generate a corresponding >regnumName synonym
				my $fullName = "$RSIDNLastName[$i],$RSIDNFirstName[$i]";
				if( (defined $RSIDNMiddleInitial[$i]) && ($RSIDNMiddleInitial[$i] ne "") ) {
					$fullName .= ",$RSIDNMiddleInitial[$i]";
				}
				$resultErrorNote .= ">regnumName \t $regNum \t> $fullName \t> " . $RSIDNRegNum[$i] .
					" \t # " . ($i+1);
				if( $i != ($count-1) ) {
					$resultErrorNote .= "\n";
				}
			}
		}
	}
	
	if( (lc($debugLastName) eq lc($lastName)) ) {
		print "PMS_MySqlSupport::LookUpSwimmerInRSIDN(): Returning: '$resultFirstName', " .
			"'$resultMiddleInitial', '$resultLastName', '$resultRegNum', '$resultTeam', '$resultDOB', " .
			"'$resultMissingDataType', '$resultErrorNote', '$resultErrorRegnum', '$resultRsidnId'\n";
	}
	return $resultFirstName, $resultMiddleInitial, $resultLastName, $resultRegNum, $resultTeam, $resultDOB,
		$resultMissingDataType, $resultErrorNote, $resultErrorRegnum, $resultRsidnId;
	
} # end of LookUpSwimmerInRSIDN()


# ConstructSynonym - construct a '>last,first' synonym which can be added to the properties.txt file
#	to fix an incorrectly spelled name.  May not be appropriate, so this synomym needs to be reviewed.
#	This generated string will end up in the log file.
#
# PASSED:
#	$lastName - "misspelled" name
#	$firstName - "misspelled" name
#	$middleInitial - "misspelled" name
#	$RSIDNLastName - "corrected" name
#	$RSIDNFirstName - "corrected" name
#	$RSIDNMiddleInitial - "corrected" name
#	$msgStr - a comment that is part of the '>last,first' synonym
#
# RETURNED:
#	$result - the generated '>last,first' synonym
#
sub ConstructSynonym( $$$$$$$ ) {
	my( $lastName, $firstName, $middleInitial, $RSIDNLastName, $RSIDNFirstName, $RSIDNMiddleInitial, $msgStr ) = @_;
	my $hash = "";
	my $RSIDNMiddleInitialStr = "";
	my $middleInitialStr = "";
	
	# if the submitted name is the same as the RSIDN name then we don't really need the synonym
	if( ($lastName eq $RSIDNLastName) &&
		($firstName eq $RSIDNFirstName) &&
		($middleInitial eq $RSIDNMiddleInitial ) ) {
		$hash = "# ";
		}

	if( $RSIDNMiddleInitial ne "" ) {
		$RSIDNMiddleInitialStr = ",$RSIDNMiddleInitial";
	}
	
	if( $middleInitial ne "" ) {
		$middleInitialStr = ",$middleInitial";
	}
	
	my $result = "$hash>last,first\t$lastName,$firstName$middleInitialStr\t\t" .
		">\t$RSIDNLastName,$RSIDNFirstName$RSIDNMiddleInitialStr\t\t# $msgStr";

	return $result;

} # end of ConstructSynonym()




# Compute number of swims for a person in a particular category
#
# PASSED:
#	$swimmerId - the id into the Swimmer table for the passed swimmer
#	$category - the category of swims we are interested in
#
# RETURNED:
#	$count - the number of swims
#
sub NumSwims( $$ ) {
	my ($swimmerId, $category) = @_;
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $sth, my $rv;
	my $count;

	my $query = "SELECT COUNT(*) from Swim JOIN Events WHERE " .
		"Swim.SwimmerId = '$swimmerId' AND " .
		"Swim.EventId = Events.EventId AND " .
		"Events.Category = '$category'";
		
	($sth,$rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query );
	if( my $resultHash = $sth->fetchrow_hashref ) {
		$count = $resultHash->{'COUNT(*)'};
	} else {
		print "PMS_MySqlSupport::NumSwims(): unable to get swims for swimmerid $swimmerId, " .
			"category=$category\n";
	}

	return $count;
} # end of NumSwims()



#     PMS_MySqlSupport::AddSwim( $eventId, $swimmerId, $timeOrDistance, $recordedPlace, $place,
#    	$rowRef, $rowNum );
# AddSwim - Add a swim (splash) to our list of swims.  This will record the swim for every PMS swimmer in
#	our database.  Only PMS swims are recorded in this table.
#
# Passed:
#	eventId - the event that this swim belongs to
#	swimmerId - the swimmer making the splash
#	time - the swimmer's recorded time (must be of the form 'hh:mm:ss[.tt]')
#	recordedPlace - the swimmer's recorded place.  Not necessarily their final place.
#	place - the swimmer's final place in the event.  Can be better than the recordedPlace if 
#		faster non-PMS swimmers were removed from the event.  -10 if non-PMS swimmer
#   rowRef - reference to a string
#	rowNum -
#
# NOTE: used for OW only
#
sub AddSwim( $$$$$$$ ) {
	(my $eventId, my $swimmerId, my $time, my $recordedPlace, my $place, my $rowRef, my $rowNum) = @_;
	my $dbh = GetMySqlHandle();

	# we're going to save the entire row from which we got this swimmer's result
    (my $rowAsString, my $numNonEmptyFields) = PMSUtil::CleanAndConvertRowIntoString( $rowRef );
	$rowAsString = MySqlEscape( $rowAsString );
	
#todo compute age and agegroup
	# convert the time into an int (number of hundreths)
	my $timeAsInt = PMSUtil::GenerateCanonicalDurationForDB_v2( $time, 
		DistanceForThisEvent( $eventId, $rowRef, $rowNum ),
		$rowRef, $rowNum );
	
	(my $sth, my $rv) = PrepareAndExecute( $dbh, 
		"INSERT INTO Swim " .
			"(EventId, SwimmerId, Duration, RecordedPlace, ComputedPlace, Age, AgeGroup, Row, RowNum) " .
			"VALUES (\"$eventId\", \"$swimmerId\", \"$timeAsInt\", \"$recordedPlace\", \"$place\", " .
			"'0', '0', '$rowAsString', '$rowNum' )") ;

} # end of AddSwim()


# NOTE: used for OW only
sub DistanceForThisEvent( $$$ ) {
	my ($eventId,$rowRef, $rowNum) = @_;
	my $distance;
	
	if( (defined $eventId) && ($eventId != 0) ) {
		my $dbh = PMS_MySqlSupport::GetMySqlHandle();
		my ($sth, $rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
			"SELECT Distance FROM Events where EventId = '$eventId'" );
		my $resultHash = $sth->fetchrow_hashref;
		if( !defined( $resultHash ) ) {		
	        PMSLogging::DumpRowError( $rowRef, $rowNum, "PMS_MySqlSupport::DistanceForThisEvent(): " .
	        	"failed to get the distance for event id '$eventId' - assume 1/2 mile.  " .
	        	"This may cause false errors later.", 1 );
	        $distance = 0.5 * 1760;
		} else {
			$distance = $resultHash->{'Distance'} * 1760;
		}
	} else {
		# our sanity checks will likely make invalid assumptions
        PMSLogging::DumpRowError( $rowRef, $rowNum, "PMS_MySqlSupport::DistanceForThisEvent(): " .
        	"No eventId passed, which means we can't make a reasonable assumption for the " .
        	"distance for event for which we have a time - assume 1/2 mile.  " .
        	"This may cause false errors later.", 1 );
        $distance = 0.5 * 1760;
	}
	return $distance;
} # DistanceForThisEvent()


# InitialRecordThisEvent - record all the details that we know about a single event into our DB
#   The event will only be recorded if it is not already in the DB.  To compare events we
#	look at:
#		eventName, EventSimpleFileName, distance
# todo:  should use just UniqueEventID...???
#
#	If an event is already recorded then this function will re-set the values for:
#		numSplashes, numDQs
#
# Passed:
#	 $eventName, $eventFullPath, $EventSimpleFileName, $fileType, $distance, $numSplashes, $numDQs
#
# Returned:
#	$eventId - the unique event id for this event.
#
# An Events table looks like this:
# --- EventName : taken from property file defining all events
# --- EventFullPath : full path name of the file containing the event results
# --- EventSimpleFileName : last simple name in EventFullPath, includes extension
# --- FileType : the file extension in lower case (csv, txt, xls)
# --- Category : either 1 or 2
# --- Distance : the distance of the event in miles
# --- Date : the date of this event.
# --- UniqueEventID : a UNIQUEID for this exact event over all years this exact event was held.
#		The same event for cat 1 and cat 2 has the same UniqueEventID
# --- NumSplashes : number of individual swimmers in this event
# --- NumDQs : of the NumSplashes swimmers this is the number that were DQed.
#
sub InitialRecordThisEvent( $$$$$$$$$$ ) {
	my $sth, my $rv;
	(my $eventName, my $eventFullPath, my $eventSimpleFileName, my $fileType, my $category, my $eventDate,
		my $distance, my $UniqueEventID, my $numSplashes, my $numDQs) = @_;
	my $eventId = 0;
	my $dbh = GetMySqlHandle();

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT EventId FROM Events where EventName = '$eventName' AND " .
			"EventSimpleFileName = '$eventSimpleFileName' AND " .
			"Distance = '$distance' and Category = '$category'" );
	my $resultHash = $sth->fetchrow_hashref;
	if( !defined( $resultHash ) ) {
		# insert this new event	
		# what category is this event?
		if( $eventSimpleFileName =~ m/CAT2/ ) {
			$category = 2;
		}
		
		($sth, $rv) = PrepareAndExecute( $dbh,
	    	"INSERT INTO Events " .
	    		"(EventName, EventFullPath, EventSimpleFileName, FileType, Category, " .
	    		"Distance, Date, UniqueEventID, NumSplashes, NumDQs) " .
	    		"VALUES (\"$eventName\", \"$eventFullPath\", \"$eventSimpleFileName\", \"$fileType\", " .
	    		"\"$category\", \"$distance\", \"$eventDate\", \"$UniqueEventID\", \"$numSplashes\", \"$numDQs\")" );
    	$eventId = $dbh->last_insert_id(undef, undef, "Events", "EventId");
    	die "Can't determine EventId of newly inserted Event" if( !defined( $eventId ) );
	} else {
		# event already exists
		# update the num splashes and dq's for this existing event - set them to 0
		($sth, $rv) = PrepareAndExecute( $dbh,
			"UPDATE Events SET NumSplashes = -1, NumDQs = -1 " .
			"WHERE EventId = $eventId" );
		# get the EventId of this existing event
		$eventId = $resultHash->{'EventId'};
	}
    return $eventId;
		
} # end of InitialRecordThisEvent()




# UpdateThisEvent - update the passed event with some statistics
#
# Passed:
#	$eventId - the EventId of the event to update
#	$numSplashes
#	$numDQs
#
# Returned:
#	n/a
#
# An Events table looks like this:
# --- EventName : derived from the file name
# --- EventFullPath : full path name of the file containing the event results
# --- EventSimpleFileName : last simple name in EventFullPath, includes extension
# --- FileType : the file extension in lower case (csv, txt, xls)
# --- Category : either 1 or 2
# --- Distance : the distance of the event in miles
# --- Date : the date of this event.
# --- NumSplashes : number of individual swimmers in this event
# --- NumDQs : of the NumSplashes swimmers this is the number that were DQed.
#
sub UpdateThisEvent( $$$ ) {
	(my $eventId, my $numSplashes, my $numDQs) = @_;
	my $dbh = GetMySqlHandle();

	(my $sth, my $rv) = PrepareAndExecute( $dbh,
		"UPDATE Events SET NumSplashes = $numSplashes, NumDQs = $numDQs " .
		"WHERE EventId = $eventId" );
} # end of UpdateThisEvent()



# PMS_MySqlSupport::LogInvalidSwimmer( "PMSBadRegButName", $swimmerId, $regNumId, $eventId, $notes );
# LogInvalidSwimmer - add a new row to the MissingData table recording the passed information and type of
#	missing data.
#
# PASSED:
#	mdType - the type of missing data, e.g. 'PMSFuzzyNameWithRegnum'
#	swimmerId - references the swimmer with the missing data
#	regNum -  the regNum they used to enter a race
#	eventId - the event the passed swimmer competed in
#	notes (optional) - additional information (usually intended for the log) - max 255 chars
#
#
my $logInvalidSwimmersCount = 0;		# number of invalid swimmers we've logged
sub LogInvalidSwimmer {
	my ($mdType, $swimmerId, $regNum, $eventId, $notes) = @_;
	my($sth, $rv);
	my $dontLog = 0;		# set to 1 to disable logging this error
	my $eventName = "";
	my $category = "";
	my $dataString = "";
	my $debugSwimmerId = "xxx";
	my $dbh = GetMySqlHandle();

	$notes = '' if( !defined( $notes ) );
	my $escNotes = MySqlEscape( $notes );

	# convert the missing data type into its corresponding id
	($sth,$rv) = PrepareAndExecute( $dbh, 
		"SELECT MissingDataTypeId from MissingDataType where ShortName='$mdType'" );
	my $resultHash = $sth->fetchrow_hashref;
	my $missingDataTypeId = 0;		# invalid value...
	if( defined($resultHash) ) {
		$missingDataTypeId = $resultHash->{'MissingDataTypeId'};
	} else {
    	die "PMS_MySqlSupport::LogInvalidSwimmer(): Can't find MissingDataTypeId in MissingDataType " .
    		"where ShortName is '$mdType'";
	}
	
	# get the event that this error occurred in:
	($sth,$rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh,
		"SELECT EventName,Category from Events where EventId = $eventId" );
	$resultHash = $sth->fetchrow_hashref;
	if( defined( $resultHash ) ) {
		$eventName = $resultHash->{'EventName'};
		$category = $resultHash->{'Category'};
	} else {
		die "PMS_MySqlSupport::LogInvalidSwimmer(): Failed to SELECT EventName and Category from Events " .
			"with eventId='$eventId'";
	}
	
	if( $debugSwimmerId eq $swimmerId ) {
		print "PMS_MySqlSupport::LogInvalidSwimmer(): missingDataTypeId=$missingDataTypeId, " .
			"regnum=$regNum, swimmerid=$swimmerId, notes='$notes'\n";
	}

	my $uniqueKey = "";
	my $newDataString = "";
	
	if( $mdType eq "PMSNoRegNoName") {
		$uniqueKey = MySqlEscape( $swimmerId );
		$newDataString = "$eventName (Cat $category), " . MySqlEscape( $regNum );
	} elsif( $mdType eq "PMSRegNoName" ) {
		$uniqueKey = MySqlEscape( $swimmerId );
		$newDataString = "$eventName (Cat $category)";
	} elsif( $mdType eq "PMSBadRegButName" ) {
		$uniqueKey = MySqlEscape( $swimmerId ) . ">$regNum";
		$newDataString = "$eventName (Cat $category)";
	} elsif( $mdType eq "PMSFuzzyNameWithRegnum" ) {
		$uniqueKey = MySqlEscape( $swimmerId );
		$newDataString = "$eventName (Cat $category)";
	} elsif( $mdType eq "PMSNamesButNoRegnum" ) {
		$uniqueKey = MySqlEscape( $swimmerId );
		$newDataString = "$eventName (Cat $category)";		
	} else {
		die "PMS_MySqlSupport::LogInvalidSwimmer(): Unknown MissingDataType ShortName='$mdType'.";
	}
	
	
	# see if this error has already been seen
	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT MissingDataId FROM MissingData " .
		"WHERE UniqueKey = '$uniqueKey' " . 
		"AND MissingDataTypeId = '$missingDataTypeId'", "" );
	$resultHash = $sth->fetchrow_hashref;
	if( defined( $resultHash ) ) {
		# we've logged this error before.  Update the existing error with this particular error's details
		my $missingDataId = $resultHash->{'MissingDataId'};
		my($sth2, $rv2) = PrepareAndExecute( $dbh,
			"SELECT DataString FROM MissingData " .
			"WHERE MissingDataId = '$missingDataId'" );
		my $resultHash2 = $sth2->fetchrow_hashref;
		if( !defined $resultHash2 ) {
			# error - can't happen
			die "PMS_MySqlSupport::LogInvalidSwimmer(): Found existing error but no associated DataString. " .
				"MissingDataType='$mdType', UniqueKey='$uniqueKey'";
		} else {
			$dataString = $resultHash2->{'DataString'};
			$dataString .= "; $newDataString";
			$dataString = MySqlEscape( $dataString );
			# now, put the updated DataString back
			my $rowsAffected = $dbh->do(
				"UPDATE MissingData SET DataString='$dataString' WHERE MissingDataId='$missingDataId'" );
			$rowsAffected = 0 if( !defined $rowsAffected );
			if( $rowsAffected != 1 ) {
				die "PMS_MySqlSupport::LogInvalidSwimmer(): UPDATE failed - $rowsAffected rows affected.";
			}
		}
	} else {
		# we haven't seen this error before - create a new log entry
		$dataString = MySqlEscape( "Event(s) and details: $newDataString" );
		$dataString .= "; uniqueKey='$uniqueKey'";
		$dataString = MySqlEscape( $dataString );

		($sth,$rv) = PrepareAndExecute( $dbh, 
			"INSERT INTO MissingData VALUES (0, $missingDataTypeId, $swimmerId, '$regNum'," .
				"$eventId,'$escNotes', '$uniqueKey', '$dataString')", "" );
	}
	$logInvalidSwimmersCount++;
} # end of LogInvalidSwimmer()


# GetNumLoggedInvalidSwimmers - return the number of logged invalid swimmers
sub GetNumLoggedInvalidSwimmers() {
	return $logInvalidSwimmersCount;
}


#  MySqlEscape( $string )
# process the passed string, escaping quotes and backslashes
sub MySqlEscape( $ ) {
	my $string = $_[0];
	if( defined( $string ) ) {
		$string =~ s/"/\\"/g;
		$string =~ s/'/\\'/g;
		$string =~ s/\\/\\/g;
	}
	return $string;
} # end of MySqlEscape()



# $sthRef = PMS_MySqlSupport::GetListOfResults( $category, $resultGender, $resultAgeGroup, \$sth );

# GetListOfResults - return an array of swimmers with their category-specific points and
#	category-specific Reason
#	who are in the passed gender and agegroup.  Return the list in order of number of points,
#	most points first.  Ties are NOT broken here.
#
# PASSED:
#	$category - the category of swims we are interested in
#	$gender - the gender of swimmers we are interested in
#	$ageGroup - we only want to consider swimmers in this age group
#
# RETURNED:
#	$sth - result returned by PrepareAndExecute().  Use it to pass through the results, like this:
#		while( my $resultHash = $sth->fetchrow_hashref ) {
#			if( $resultHash->{'Points'} > 0 ) {
#				....
#
# Each swimmer returned in the array is a hash
#
sub GetListOfResults( $$$ ) {
	my($category,$gender,$ageGroup) = @_;
	my ($rv, $sth);
	my $dbh = GetMySqlHandle();

	my $pointsToGet = "Cat".$category."Points";
	my $fieldName = "Cat" . $category . "Reason";

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT SwimmerId, RegNum, Age1, Age2, Gender, AgeGroup, FirstName, " .
		"MiddleInitial, LastName, $pointsToGet as Points, $fieldName as Reason, " .
		"RegisteredTeamInitials, RSIDN_ID FROM Swimmer " .
		"WHERE $pointsToGet >= 0 " .
		"AND AgeGroup = '$ageGroup' " .
		"AND Gender = '$gender' " .
		"ORDER BY $pointsToGet DESC", "");
	return $sth;

} # end of GetListOfResults()



#			my($address, $city, $state, $zip, $country, $firstN, $middleI, $lastN, $email) = 
#										PMS_MySqlSupport::GetSwimmersAddress( $rsidnId );
sub GetSwimmersAddress( $ ) {
	my $rsidnId = $_[0];
	my($address, $city, $state, $zip, $country, $firstN, $middleI, $lastN, $email) = 
		("?unknown address?", "?unknown city?", "??", "00000", "??","?unknown first name",
		"?unknown middle initial", "?unknown last name", "?unknown email");
	my $dbh = GetMySqlHandle();
	my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};

	my $query = "SELECT Address1, City, State, Zip, Country, FirstName, MiddleInitial, LastName, Email " .
				"FROM RSIDN_$yearBeingProcessed WHERE RSIDNId = $rsidnId";
	my($sth,$rv) = PrepareAndExecute($dbh, $query);


	my $resultHash = $sth->fetchrow_hashref;
	if( !defined $resultHash ) {
		# error - can't happen
        PMSLogging::DumpError( 0, 0, "PMS_MySqlSupport::GetSwimmersAddress(): failed to get the " .
        		"address for swimmer with RSIDNId '" . $rsidnId . "'.  NON-Fatal.  Bogus values used." );
	} else {
		$address = $resultHash->{'Address1'};
		$city = $resultHash->{'City'};
		$state = $resultHash->{'State'};
		$zip = $resultHash->{'Zip'};
		$country = $resultHash->{'Country'};
		$firstN = PMSUtil::trim( $resultHash->{'FirstName'} );
		$middleI = PMSUtil::trim( $resultHash->{'MiddleInitial'} );
		$lastN = PMSUtil::trim( $resultHash->{'LastName'} );
		$email = PMSUtil::trim( $resultHash->{'Email'} );
	}
	return ($address, $city, $state, $zip, $country, $firstN, $middleI, $lastN, $email);
} # end of GetSwimmersAddress()


# GetListOfRaces - return an array of races in the passed category.
#	Return the list in order of number of EventId, lowest (earliest in year) first.
#
# PASSED:
#	$category - the category we're interested in.
#
# RETURNED:
#	$sth - result returned by PrepareAndExecute().  Use it to pass through the results, like this:
#		while( my $raceHash = $sth->fetchrow_hashref ) {
#			my $raceName = $raceHash->{'EventName'};
#				....
#
# Each race returned in the array is a hash
#
sub GetListOfRaces( $ ) {
	my($category) = @_;
	my ($rv, $sth);
	my $dbh = GetMySqlHandle();

	my $pointsToGet = "Cat".$category."Points";

	($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT EventId, EventName, Date FROM Events " .
		"WHERE Category = $category " .
		"ORDER BY EventId ASC", "");
	return $sth;

} # end of GetListOfRaces()




# CalculateEachSwimmersPoints();
# For each swimmer and category go through their places and take the top 
#	PMSStruct::GetMacrosRef()->{"numSwimsToConsider"} 
# places, and then using those places, compute their points and update their
# swimmer record.  If they have no swims for a specific category their total
# points recorded for that category will be -1.  Otherwise it will be 0 or more.
#
sub CalculateEachSwimmersPoints() {
	my $debugSwimmerId = 000;
	my $dbh = GetMySqlHandle();

	print "CalculateEachSwimmersPoints() working...";
	# get the list of ALL PMS swimmers who we got results for
# todo:  we're getting all swimmers, not just PMS swimmers.  this is wasteful
	my($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT SwimmerId,FirstName, LastName FROM Swimmer ORDER BY SwimmerId" );
	
	# for each PMS swimmer we'll get all of their finishes (places) from their Swims
# todo:  we're looking at all swimmers, not just PMS swimmers.  this is wasteful
	while( my $resultHash = $sth->fetchrow_hashref ) {
		# we got a new swimmer
		my $swimmerId = $resultHash->{'SwimmerId'};
		my $firstName = $resultHash->{'FirstName'};
		my $lastName = $resultHash->{'LastName'};
		# work on each category separately...
		foreach my $cat ( (1,2) ) {
			# work on a specific category
				

			my @arrayOfPlaces = ();
			my $numSwimsToConsider = PMSStruct::GetMacrosRef()->{"numSwimsToConsider"};
# todo:  the following causes non-pms swimmers to get 0 rows because their computed places are -10.
# but this is weird.
			my $query = "SELECT ComputedPlace from Swim JOIN Events " .
				"ON Swim.EventId = Events.EventId where Category = \"$cat\" " .
				"AND ComputedPlace > 0 " .
				"AND SwimmerId = \"$swimmerId\"";
# todo: order by computedplace to eliminate sort below?
			if( $swimmerId == $debugSwimmerId ) {
				print "CalculateEachSwimmersPoints(): query='$query'\n";
			}
			my($sth2, $rv2) = PrepareAndExecute( $dbh, $query );
			while( my $resultHash2 = $sth2->fetchrow_hashref ) {
				push @arrayOfPlaces, $resultHash2->{'ComputedPlace'};
			}
			my $length = scalar(@arrayOfPlaces);
			#print "Number of places for $firstName $lastName ($swimmerId): " . $length . "\n";
			my $points = 0;
# todo:  length will be 0 for non-pms, so they end up getting no points.  this is weird
			if( $length > 0 ) {
				# this swimmer had some finishes in the current category
				if( $length > $numSwimsToConsider ) {
			#		@arrayOfPlaces = sort sort { $a <=> $b } @arrayOfPlaces;
					@arrayOfPlaces = sort { $a <=> $b } @arrayOfPlaces;
				}
				if( $length < $numSwimsToConsider ) {
					$numSwimsToConsider = $length;
				}
				for( my $i=0; $i < $numSwimsToConsider; $i++ ) {
					$points += $PMSConstants::PLACE[$arrayOfPlaces[$i]] if( $arrayOfPlaces[$i] <= 10 );
				}
				# since this swimmer finished some races in this category we will put their 
				# total points into their Swimmer row
	    		my($sth3,$rv3) = PrepareAndExecute( $dbh, 
					"UPDATE Swimmer SET Cat".$cat."Points = $points " .
					"WHERE SwimmerId = $swimmerId" );
			} # end of this swimmer scored some points
		} # end of work on a specific category		
	} # end of we got a new swimmer
	print "...done!\n";
	
} # end of CalculateEachSwimmersPoints()




# GetTeamMembers - return a string listing all team members for the passed team.  Include all the points earned by
#	each swimmer and a link to the accumulated points page.
#
# PASSED:
#	team - team abbreviation, e.g. "WCM"
#	$simpleGeneratedFileName - The simple file name of the individual Accumulated Results, e.g. in 2016
#		this was "2016PacMastersAccumulatedResults.html"
#
# RETURNED:
#	members - string of the form "first m last (123), first m last (345), ..."
#
sub GetTeamMembers( $$ ) {
	my $team = $_[0];
	my $simpleGeneratedFileName = $_[1];
	my $members = "";
	my $resultHash;
	my $dbh = GetMySqlHandle();

	my ($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT Cat1Points, Cat2Points, FirstName, MiddleInitial, LastName, SwimmerId FROM Swimmer " .
		"WHERE RegisteredTeamInitials = '$team' " .
		"AND isPMS = 1 " .
		"ORDER BY Cat1Points + Cat2Points desc", "");
		
	while( defined($resultHash = $sth->fetchrow_hashref) ) {
		# got another member
		if( $members ne "" ) {
			$members .= ", ";
		}
		my $middle = $resultHash->{"MiddleInitial"};
		if( $middle eq "" ) {
			$middle = " ";
		} else {
			$middle = " $middle ";
		}
		my $cat1Points = $resultHash->{'Cat1Points'};
		my $cat2Points = $resultHash->{'Cat2Points'};
		my $points = 0;
		my $pointsStr = "";
		# this swimmer is responsible for accumulating all of their CAT1 and CAT2 points to their team's standing:
		$points += $cat1Points if( $cat1Points > 0 );
		$points += $cat2Points if( $cat2Points > 0 );
		if( $points == 0 ) {
			# this swimmer earned no points, so we show their name only
		} else {
			$pointsStr = "(";
			# distinguish their category points if they earned in two categories
			# result will be of the form:
			#	First M Last(CAT1:43)     or
			#	First M Last(CAT2:43)     or
			#	First M Last(CAT1:33+CAT2:10=43)
			if( $cat1Points > 0 ) {
				my $listToPlaceID = PMSStruct::GetListToPlaceIDs( "1-" . $resultHash->{'SwimmerId'} );
				if( defined $listToPlaceID ) {
					# this means that the swimmer appears in the Accumulated Results.
					# ($listToPlaceID wouldn't be defined if the swimmer had 0 points and we're not
					# configured to show swimmers with 0 points, or if we're only configured to show
					# the top N swimmers and this swimmer isn't in that group)
#					$pointsStr .= "<a href=\"$simpleGeneratedFileName?open=$listToPlaceID\">" .
#						"<span style=\"font-size:8px\">CAT1:</span>$cat1Points" . "</a>";
					$pointsStr .= "<a href=\"$simpleGeneratedFileName?open=$listToPlaceID\">" .
						"$cat1Points" . "</a>";
				}
				if( $cat2Points > 0 ) {
					$pointsStr .= "+";
				}
			}
			if( $cat2Points > 0 ) {
				my $listToPlaceID = PMSStruct::GetListToPlaceIDs( "2-" . $resultHash->{'SwimmerId'} );
				if( defined $listToPlaceID ) {
					# this means that the swimmer appears in the Accumulated Results.
					# ($listToPlaceID wouldn't be defined if the swimmer had 0 points and we're not
					# configured to show swimmers with 0 points, or if we're only configured to show
					# the top N swimmers and this swimmer isn't in that group)
#					$pointsStr .= "<a href=\"$simpleGeneratedFileName?open=$listToPlaceID\">" .
#						"<span style=\"font-size:8px\">CAT2:</span>$cat2Points" . "</a>";
					$pointsStr .= "<a href=\"$simpleGeneratedFileName?open=$listToPlaceID\">" .
						"$cat2Points" . "</a>";
				}
			}
			if( ($cat1Points > 0) && ($cat2Points > 0) ) {
				$pointsStr .= "=$points";
			}
			$pointsStr .= ")";
			
		}
	$members .= $resultHash->{"FirstName"} . $middle . $resultHash->{'LastName'} . $pointsStr;
	
		
if(0) {
		# generate the <div ID....> used to show/hide the ListTo info...
		my $listToPlaceID = PMSStruct::GetListToPlaceIDs( $resultHash->{'SwimmerId'} );
		if( defined $listToPlaceID ) {
			# this means that the swimmer appears in the Accumulated Results.
			# ($listToPlaceID wouldn't be defined if the swimmer had 0 points and we're not
			# configured to show swimmers with 0 points, or if we're only configured to show
			# the top N swimmers and this swimmer isn't in that group)
			$members .= "<a href=\"$simpleGeneratedFileName?open=$listToPlaceID\">";
		}
		$members .= $resultHash->{"FirstName"} . $middle . $resultHash->{'LastName'} . $points;
		if( defined $listToPlaceID ) {
			$members .= "</a>";
		}
}




	}

	return $members;
} # end of GetTeamMembers()






#		PMS_MySqlSupport::StoreSwimmersTotals( $resultHash->{'SwimmerId'}, $totalDistance, $totalTimeInHundredths, 
#			$resultHash->{'Category'} );
# StoreSwimmersTotals - store the total distance and total time swum by the passed swimmer in the passed category.
#
# PASSED:
#	$swimmerId - The swimmer 
#	$totalDistance - the swimmer's total distance
#	$totalTimeInHundredths - the swimmer's total time to swim that distance
#	$category - the category of swims
#
sub StoreSwimmersTotals( $$$$ ) {
	my( $swimmerId, $totalDistance, $totalTimeInHundredths, $category ) = @_;
	my $colDistanceName = "Cat$category" . "TotalDistance";
	my $colDurationName = "Cat$category" . "TotalDuration";
	my $dbh = GetMySqlHandle();

	my ($sth, $rv) = PrepareAndExecute( $dbh,
		"UPDATE Swimmer SET $colDistanceName = $totalDistance, $colDurationName = $totalTimeInHundredths " .
		"WHERE SwimmerId = $swimmerId" );
		
} # end of StoreSwimmersTotals()



# GetUSMSSwimmerIdFromName - get the swimmer's USMSSwimmerId using their name to look them up in the Rsidn table.
#	This is only used when trying to parse a name of the form "xxx yyy zzz ..." and determine which of those
#	strings is the first, middle, and last names.
#
# PASSED:
#	$fileName - the full path file name of the result file being processed
#	$lineNum - the line number of the line being processed
#	$firstName - swimmer's first name
#	$middleInitial - swimmer's middle initial, or an empty string ("")
#	$lastName - swimmer's last name
#	$optionalMiddle - (optional - default is false) if true then the middle initial in
#		the rsind file doesn't have to match the passed $middleInitial IF the passed
#		$middleInitial is "".  (This is usually what we want but note it's not the default...)
#
# RETURNED:
#	EITHER the swimmer's Id number (e.g. 0A94S), or an empty string ("") if the swimmer isn't found 
#		in our Rsidn table.
#		EXAMPLES (from real data):  
#			Sarah Jane Sapiano  :	first: "Sarah Jane"   Last: "Sapiano"
#			Miek Mc Cubbin		:	first: "Miek"		  Last: "Mc Cubbin"
#
sub GetUSMSSwimmerIdFromName {
	my($fileName, $lineNum, $firstName, $middleInitial, $lastName,$optionalMiddle) = @_;
	my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};
	if( !defined( $optionalMiddle ) ) {
		$optionalMiddle = 0;
	}
	my $USMSSwimmerId = "";
	my $resultHash;
	my $dbh = GetMySqlHandle();
	my $middleSql = "AND MiddleInitial = \"$middleInitial\"";

	if( $optionalMiddle && ($middleInitial eq "") ) {
		# query does NOT depend on middle initial
		$middleSql = "";
	}
	my ($sth, $rv) = PrepareAndExecute( $dbh,
		"SELECT USMSSwimmerId " .
		"FROM RSIDN_$yearBeingProcessed " .
		"WHERE FirstName = \"$firstName\" $middleSql AND LastName = \"$lastName\"" );
	if( defined($resultHash = $sth->fetchrow_hashref) ) {
		# this swimmer was found in our DB - get the USMSSwimmerId
		$USMSSwimmerId = $resultHash->{'USMSSwimmerId'};
	}
	return $USMSSwimmerId;
} # end of GetUSMSSwimmerIdFromName()


# return >0 if we have some results to report, 0 otherwise.
sub WeHaveResultsToReport() {
	my $result = 1;
	my $query = "SELECT COUNT(*) from Events";
	my $dbh = GetMySqlHandle();
	
	my ($sth, $rv) = PrepareAndExecute( $dbh, $query );
	if( my $resultHash = $sth->fetchrow_hashref ) {
		$result = $resultHash->{'COUNT(*)'};
	} else {
		print "PMS_MySqlSupport::WeHaveResultsToReport(): unable to count the number of events\n";
	}

	return $result;
} # end of WeHaveResultsToReport()


1;  # end of module
