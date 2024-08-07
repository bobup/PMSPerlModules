#!/usr/bin/perl -w
# PMSLogging.pm - General logging utilities used by GenerateOWResults and MaintainOWSwimmerHistory


# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMSLogging;

use diagnostics;
use strict;
use warnings;

#use lib 'PMSPerlModules';
require PMSConstants;
use IO::Handle;		# for flush()

my $numErrorsLogged = 0;


sub GetNumErrorsLogged() {
	return $numErrorsLogged;
} # end of GetNumErrorsLogged()


my $logOnlyLines = 0;		# number of lines of logging to the log file only
sub GetLogOnlyLines() {
	return $logOnlyLines;
}


sub FlushLogFile() {
	if( defined fileno( LOG ) ) {
		LOG->flush();
	}
} # end of FlushLogFile()

#
# InitLogging - initialize our logging system
#
# PASSED:
#	$generatedLogFileName - the full file name of the log file to log to.
#
# RETURNED:
#	$message - "" if initialization was OK, an error message if an error occurred.
#
# NOTES:
#	If logging is desired then this function must be called first.  However, if this function
#	is not called then logging will not be initialized, so calls to log something will be ignored.
#
sub InitLogging( $ ) {
    my $generatedLogFileName = $_[0];
    my $message = "";
    open( LOG, ">$generatedLogFileName" ) || ($message = "Can't open $generatedLogFileName: $!\nAbort.\n");
    if( $message eq "" ) {
	    # eliminate "Wide character in print" error:
	    binmode STDOUT, ":utf8";
	    binmode LOG, ":utf8";
    }
    return( $message );
} # end of InitLogging()


# printLog - print the passed string to the log file if it exists, otherwise to stdout.
# A newline is NOT AUTOMATIC - you must put it into the passed string if you want one.
sub printLog( $ ) {
	if( defined fileno( LOG ) ) {
		print LOG $_[0];
	} else {
		print "*** LOG FILE NOT INITIALIZED.*** " . $_[0];
	}
} # end of printLog()



#
# PrintLog - print the passed string to the log file and optionally to the console
#
# PASSED:
#	$line - the result line from a result file being processed when this ERROR is dumped.
#		Set to "" (or 0) if not known.
#	$lineNum - the number of the $line in the result file.  Set to 0 (or "") if not known.
#	$errStr - the string to dump
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
# 
sub PrintLog {
	if( defined fileno( LOG ) ) {
	    my ( $line, $lineNum, $errStr, $console ) = @_;
	    my $totalErr = $errStr;
		my $detail = "";
		if( (($line eq "") || ($line eq "0")) && (($lineNum eq "") || ($lineNum eq "0")) ) {
			$detail = "";
		} elsif( ($lineNum eq "") || ($lineNum eq "0") ) {
			$detail = ": ['$line']";
		} elsif( (($line eq "") || ($line eq "0")) ) {
			$detail = ": [line #$lineNum]";
		} else {
			$detail = ": [line #$lineNum, '$line']";
		}

		# if we have detail to show then show it after the first : in the $errStr, or at the
		# end of the $errStr if there is no :
		my $ind;
		if( $detail ) {
			if( ($ind = index( $errStr, ":" )) == -1 ) {
				# no : - append on end
				$totalErr = $errStr . "    " . $detail;
			} else {
				# insert detail just after first :
				$totalErr =~ s/:/$detail:\n   /;
			}
		}		

		
	    $totalErr .= "\n";
	    printLog( $totalErr );
		if( $console ) {
			print $totalErr; 
		} else {
			$logOnlyLines++;
		}
	}
} # end of PrintLog


#
# PrintLogNoNL - print the passed string to the log file and optionally to the console with no EOL
#
# PASSED:
#	$line - the result line from a result file being processed when this ERROR is dumped.
#		Set to "" (or 0) if not known.
#	$lineNum - the number of the $line in the result file.  Set to 0 (or "") if not known.
#	$errStr - the string to dump
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
# 
sub PrintLogNoNL {
	if( defined fileno( LOG ) ) {
	    my ( $line, $lineNum, $errStr, $console ) = @_;
	    my $totalErr;
		my $detail = "";
		if( (($line eq "") || ($line eq "0")) && (($lineNum eq "") || ($lineNum eq "0")) ) {
			$detail = "";
		} elsif( ($lineNum eq "") || ($lineNum eq "0") ) {
			$detail = ": ['$line']";
		} elsif( (($line eq "") || ($line eq "0")) ) {
			$detail = ": [line $lineNum]";
		} else {
			$detail = ": [line $lineNum, '$line']";
		}
	    $totalErr = $errStr . $detail;
	    printLog( $totalErr );
		if( $console ) {
			print $totalErr;
		} else {
			$logOnlyLines++;
		}
	}
} # end of PrintLogNoNL




#
# DumpError - dump a ERROR to the log file, and optionally to the console
#
# SYNOPSIS:
#	DumpError( $line, $lineNum, "error str", 1 );
#
# PASSED:
#	$line - the result line from a result file being processed when this ERROR is dumped.
#		Set to "" (or 0) if not known.
#	$lineNum - the number of the $line in the result file.  Set to 0 (or "") if not known.
#	$errStr - the ERROR to dump
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
# 
sub DumpError {
	if( defined fileno( LOG ) ) {
	    my ( $line, $lineNum, $errStr, $console ) = @_;
	    PrintLog( $line, $lineNum, "! ERROR: $errStr\n  -- END ERROR", $console );
		$numErrorsLogged++;
	}
} # end of DumpError


#
# DumpFatalError - dump a FATAL ERROR to the log file, and optionally to the console
#
# SYNOPSIS:
#	DumpFatalError( $line, $lineNum, "error str", 1 );
#
# PASSED:
#	$line - the result line from a result file being processed when this ERROR is dumped.
#		Set to "" (or 0) if not known.
#	$lineNum - the number of the $line in the result file.  Set to 0 (or "") if not known.
#	$errStr - the ERROR to dump
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
#
# NOTES:
#	Most errors are fatal, but we have some errors that we know are fatal and also need 
#	a more user friendly error message. For those errors use this routine.
# 
sub DumpFatalError {
	if( defined fileno( LOG ) ) {
	    my ( $line, $lineNum, $errStr, $console ) = @_;
	    PrintLog( $line, $lineNum, "! FATAL ERROR: $errStr\n  -- END FATAL ERROR", $console );	    
		$numErrorsLogged++;
	}
} # end of DumpFatalError





#
# DumpWarning - dump a WARNING to the log file, and optionally to the console
#
# PASSED:
#	$line - the result line from a result file being processed when this ERROR is dumped.
#		Set to "" (or 0) if not known.
#	$lineNum - the number of the $line in the result file.  Set to 0 (or "") if not known.
#	$errStr - the WARNING to dump
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
# 
sub DumpWarning {
	if( defined fileno( LOG ) ) {
     	my ( $line, $lineNum, $errStr, $console ) = @_;
    	PrintLog( $line, $lineNum, "! WARNING: $errStr\n  -- END WARNING", $console );
	}
} # end of DumpWarning



#
# DumpNote - dump a note to the log file, and optionally to the console
#
# PASSED:
#	$line - the result line from a result file being processed when this NOTE is dumped.
#		Set to "" (or 0) if not known.
#	$lineNum - the number of the $line in the result file.  Set to 0 (or "") if not known.
#	$errStr - the note to dump
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
# 
sub DumpNote {
	if( defined fileno( LOG ) ) {
    	my ( $line, $lineNum, $errStr, $console ) = @_;
    	PrintLog( $line, $lineNum, "! NOTE: $errStr", $console );
	}
} # end of DumpNote





# DumpArray - Dump the contents of an array
#
# PASSED:
#	\@arr - a reference to the array to dump
#	$title - the title of the dump
#	$localDebug - (optional - default set below) If $localdebug is <= the global $debug then dump the array.  
# 		Set to 0 to force the dump if $debug is any non-negative value.
#
sub DumpArray {
    my @arr = @{$_[0]};
    my $title = $_[1];
    my $localDebug = 3;
    if( defined( $_[2] ) ) {
        $localDebug = $_[2];
    }
    
    if( $PMSConstants::debug >= $localDebug ) {
        printLog( "DumpArray:  $title\n" );
        for( my $i = 0; $i <= $#arr; $i++ ) {
            if( defined( $arr[$i] ) ) {
                printLog "    #$i:  '$arr[$i]'\n";
            } else {
                printLog "    #$i:  'undefined'\n";
            }

        }   
    }
    
} # end of DumpArray


# DumpArraySTDOUT - same as DumpArray except to stdout instead of the log file
sub DumpArraySTDOUT {
    my @arr = @{$_[0]};
    my $title = $_[1];
    my $localDebug = 3;
    if( defined( $_[2] ) ) {
        $localDebug = $_[2];
    }
    
    if( $PMSConstants::debug >= $localDebug ) {
        print "DumpArray:  $title\n" if( $title ne "" );
        for( my $i = 0; $i <= $#arr; $i++ ) {
            if( defined( $arr[$i] ) ) {
                print   "    #$i:  '$arr[$i]'\n";
            } else {
                print   "    #$i:  'undefined'\n";
            }

        }   
    }
    
} # end of DumpArraySTDOUT



# Dump2DArray - Dump the contents of a 2D array
#
# PASSED:
#	\@arr - a reference to the array to dump
#	$title - the title of the dump
#	$localDebug - (optional - default set below) If $localdebug is <= the global $debug then dump the array.  
# 		Set to 0 to force the dump if $debug is any non-negative value.
#
sub Dump2DArray {
    my @arr = @{$_[0]};
    my $title = $_[1];
    my $localDebug = 3;
    if( defined( $_[2] ) ) {
        $localDebug = $_[2];
    }
    
    if( $PMSConstants::debug >= $localDebug ) {
        for( my $i = 0; $i <= $#arr; $i++ ) {
            for( my $j = 0; $j <= $#{$arr[$i]}; $j++ ) {
                printLog "$title: ";
                if( defined( $arr[$i][$j] ) ) {
                    printLog "#$i,$j:  '$arr[$i][$j]'\n";
                } else {
                    printLog "#$i,$j:  'undefined'\n";
                }
            }
        }
    }
    
} # end of Dump2DArray


# DumpHash - Dump the contents of a hash
#
# PASSED:
#	\%hash - a reference to the hash to dump
#	$title - the title of the dump
#	$localDebug - (optional - default set below) If $localdebug is <= the global $debug then dump the hash.  
# 		Set to 0 to force the dump if $localDebug is not defined.
#	$console - (optional) - if true then also log to the console.  Default is 0.
#
sub DumpHash {  
    my %hash = %{$_[0]};
    my $title = $_[1];
    
    my $localDebug = 0;        # the higher this is set, the higher $debug has to be to print
    if( defined( $_[2] ) ) {
        $localDebug = $_[2];
    }
    my $console = 0;
    if( defined( $_[3] ) ) {
    	$console = $_[3];
    }
    if( $PMSConstants::debug >= $localDebug ) {        
        foreach my $key (sort keys %hash) {
            printLog "$title" . "{$key} = $hash{$key}\n";
            if( $console ) {
            	print "$title" . "{$key} = $hash{$key}\n";
            }
        }
    }
} # end of DumpHash


# DumpHashOfArray - Dump the contents of a hash of arrays
#
# PASSED:
#	\%hash - a reference to the hash of arrays
#	$title - the title of the dump
#	$localDebug - (optional - default set below) If $localdebug is <= the global $debug then dump the hash.  
# 		Set to 0 to force the dump if $debug is any non-negative value.
#
sub DumpHashOfArray {   
    my %hash = %{$_[0]};
    my $title = $_[1];
    
    my $localDebug = 3;
    if( defined( $_[2] ) ) {
        $localDebug = $_[2];
    }
        
    if( $PMSConstants::debug >= $localDebug ) {        
        foreach my $key (sort keys %hash) {
            printLog "$title" . "{$key} = ";
            DumpArray( $hash{$key}, "    ", $localDebug );
        }
    }
} # end of DumpHashOfArray



# DumpMacros - dump the values of all the macros that we've defined so far
#
# PASSED:
#	$title - the title of the dump
#	$localDebug - (optional) only dump the note if $PMSConstants::debug >= $localDebug.
#		Default is 0, which means always dump the note.  Required if $console is supplied.
#
sub DumpMacros {    
    my $localDebug = 3;
    if( defined( $_[1] ) ) {
        $localDebug = $_[1];
    }
    
    if( $PMSConstants::debug >= $localDebug ) {
        my $title = $_[0];
        PMSLogging::printLog( "DumpMacros:  $title\n");
        my $macrosRef = PMSStruct::GetMacrosRef();
        
        foreach my $key (sort keys %{$macrosRef}) {
        	my $value = $macrosRef->{$key};
            PMSLogging::printLog( "macros{$key} = $value\n");
        }
    }
} # end of DumpMacros






################ rows



# DumpRowWarning - dump a warning message pertaining to a row of results being processed.
#
# PASSED:
#   $row - a reference to the row with the warning (an array of fields)
#		Set to "" (or 0) if not known.
#	$rowNum - the number of the $row in the result file.  Set to 0 (or "") if not known.
#	$errStr - the WARNING to dump
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
# 
sub DumpRowWarning {
    my $row = $_[0];
    my $rowNum = $_[1];
    my $errStr = $_[2];
    my $console = $_[3];
    my $numErrors = 0;
    my $totalErr;
	if( (($row eq "") || ($row eq "0")) && (($rowNum eq "") || ($rowNum eq "0")) ) {
		$totalErr = "! WARNING: $errStr\n";
	} else {
		$totalErr = "! WARNING: $errStr: [row $rowNum, '@$row']\n";
	}
    PMSLogging::printLog( $totalErr );
	print $totalErr if( $console );
    $numErrors = 1;
    return $numErrors;
} # end of DumpRowWarning



# DumpRowError - dump an error message pertaining to the row of results being processed
#
# PASSED:
#   $row - a reference to the row with the warning (an array of fields)
#		Set to "" (or 0) if not known.
#	$rowNum - the number of the $row in the result file.  Set to 0 (or "") if not known.
#   $errStr - the error message
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
#
sub DumpRowError {
    my $rowRef = $_[0];
    my $rowNum = $_[1];
    my $errStr = $_[2];
    my $console = $_[3];
    my $rowAsString = "?";
    my $totalErr;
    my $numErrors = 0;
	if( (($rowRef eq "") || ($rowRef eq "0")) && (($rowNum eq "") || ($rowNum eq "0")) ) {
        $totalErr = "! ERROR: $errStr\n";
	} else {
		if( ($rowRef eq "") || ($rowRef eq "0") ) {
			$rowAsString = "?";
		} else {
        	($rowAsString, my $numNonEmptyFields) = PMSUtil::CleanAndConvertRowIntoString( $rowRef );
		}
		if( ($rowNum eq "") || ($rowNum eq "0") ) {
			$rowNum = "?";
		}
        $totalErr = "! ERROR: $errStr: [row $rowNum, '$rowAsString']\n";
    }
    PMSLogging::printLog( $totalErr );
	print $totalErr if( $console );
    $numErrors = 1;
    $numErrorsLogged++;
    
    return $numErrors;

} # end of DumpRowError


# DumpFatalRowError - dump an FATAL ERROR message pertaining to the row of results being processed
#
# PASSED:
#   $row - a reference to the row with the warning (an array of fields)
#		Set to "" (or 0) if not known.
#	$rowNum - the number of the $row in the result file.  Set to 0 (or "") if not known.
#   $errStr - the error message
#	$console - (optional) dump to the console, too, if TRUE.  Default to FALSE.
#
# NOTES:
#	Most errors are fatal, but we have some errors that we know are fatal and also need 
#	a more user friendly error message. For those errors use this routine.
# 

sub DumpFatalRowError {
    my $rowRef = $_[0];
    my $rowNum = $_[1];
    my $errStr = $_[2];
    my $console = $_[3];
    my $rowAsString = "?";
    my $totalErr;
    my $numErrors = 0;
	if( (($rowRef eq "") || ($rowRef eq "0")) && (($rowNum eq "") || ($rowNum eq "0")) ) {
        $totalErr = "! FATAL ERROR: $errStr\n";
	} else {
		if( ($rowRef eq "") || ($rowRef eq "0") ) {
			$rowAsString = "?";
		} else {
        	($rowAsString, my $numNonEmptyFields) = PMSUtil::CleanAndConvertRowIntoString( $rowRef );
		}
		if( ($rowNum eq "") || ($rowNum eq "0") ) {
			$rowNum = "?";
		}
        $totalErr = "! FATAL ERROR on row $rowNum: $errStr \n  [row $rowNum, '$rowAsString']\n  -- END FATAL ERROR";
    }
    PrintLog( "", "", $totalErr, $console );
    $numErrors = 1;
    $numErrorsLogged++;
    
    return $numErrors;

} # end of DumpFatalRowError





# DumpSynonyms - dump the synonyms suggested when swimmers are not recognized but whose name is "close" to
#	a known PMS swimmer.
#
# PASSED:
#	$theYearBeingProcessed - used as part of a log
#
# NOTES:
#
# The resulting dump will look like this:
# NOTE: >>>(S):   SYNONYMS:
# >last,first gilson,jeanette		>Gils,Robert		# CAREFUL! NO fuzzy match (-5): 'gilson,jeanette' entered a race with Reg# 3840-06VU5,
    # but that Reg# belongs to 'Gils,Robert' - assume INVALID entry; create synonym to make it valid
    # Races generating the above: 5) 2014 DelValle 2.5K=CAT1.csv
# Dump to the log file (not to the results file)
#
sub DumpSynonyms($) {
	my $theYearBeingProcessed = $_[0];
	my $numSynonyms = 0;		# so far that's all we've seen
	my $dbh = PMS_MySqlSupport::GetMySqlHandle();
	my $query;
	my $yearBeingProcessed = PMSStruct::GetMacrosRef()->{"YearBeingProcessed"};
	
	# get a list of all log types
	my ($sth,$rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, 
		"SELECT MissingDataTypeId, ShortName, LongName, Details from MissingDataType ORDER by " .
		"MissingDataTypeId" );
	while( defined(my $resultHash = $sth->fetchrow_hashref) ) {
		my $shortName = $resultHash->{'ShortName'};
		my %listOfSwimmerIdsSeen = ();
		my $title = "\n\n# NOTE: Some error [$shortName]:\n";
		if( $shortName eq "PMSRegNoName" ) {
			$title = "\n\n# NOTE:   SYNONYMS ([PMSRegNoName] bad fuzzy matches - BE CAREFUL!):\n";
		} elsif( $shortName eq "PMSFuzzyNameWithRegnum" ) {
			$title = "\n\n# NOTE:   SYNONYMS [PMSFuzzyNameWithRegnum] good fuzzy matches:\n";
		} elsif( $shortName eq "PMSBadRegButName" ) {
			$title = "\n\n# NOTE:   RegNum SYNONYMS ([PMSBadRegButName] Valid PAC names but " .
				"reg #'s don't match the corresponding name in the PAC database):\n";
		} elsif( $shortName eq "PMSNoRegNoName" ) {
			$title = "\n\n# NOTE: Neither Name nor Regnum found in the PAC Database [PMSNoRegNoName]:\n";
		} elsif( $shortName eq "PMSNamesButNoRegnum" ) {
			$title = "\n\n# NOTE: Found name in PAC database 2+ times without regnum match [PMSNamesButNoRegnum]:\n";
		}
		
		my ($sth,$rv) = PMS_MySqlSupport::PrepareAndExecute( $dbh, 
			"SELECT Swimmer.FirstName,Swimmer.LastName,Swimmer.MiddleInitial,MissingData.RegNum," .
			"MissingData.SwimmerId,MissingData.EventId,MissingData.Notes, " .
			"MissingData.DataString " .
			"FROM MissingData " .
				"JOIN (Swimmer " .
					"JOIN MissingDataType) " .
					"ON (MissingDataType.MissingDataTypeId = MissingData.MissingDataTypeId " .
					"AND MissingDataType.ShortName = '$shortName' " . 
					"AND MissingData.SwimmerId = Swimmer.SwimmerId) ORDER by Swimmer.LastName" );
		$numSynonyms = 0;
		while( defined(my $resultHash = $sth->fetchrow_hashref) ) {
			if( $numSynonyms == 0 ) {
				PMSLogging::printLog( $title );
			}
			my $swimmerId = $resultHash->{'SwimmerId'};
			my $regNum = $resultHash->{'RegNum'};
			my $firstName = $resultHash->{'FirstName'};
			my $lastName = $resultHash->{'LastName'};
			my $middleInitial = $resultHash->{'MiddleInitial'};
			$numSynonyms++;
			my $USMSSwimmerId = $regNum;
			$USMSSwimmerId =~ s/^.*-//;
			my $eventId = $resultHash->{'EventId'};
			my $notes = $resultHash->{'Notes'};
			my $dataString = $resultHash->{'DataString'};
			# convert the notes into something appropriate for the log file (remove HTML stuff)
			$notes =~ s/<b>//g;
			$notes =~ s/<\/b>//g;
			PMSLogging::printLog( "$notes" .
				"\n\t# $dataString, $theYearBeingProcessed\n" );

			if( $shortName eq "PMSBadRegButName" ) {
				# SPECIAL CASE:  This error also needs a >regnumName synonym
				###...todo
				my $escFirstName = PMS_MySqlSupport::MySqlEscape( $firstName );
				my $escLastName = PMS_MySqlSupport::MySqlEscape( $lastName );
				my $escMiddleInitial = "";
				if( $middleInitial ne "" ) {
					# include middle initial in search
					$escMiddleInitial = " AND MiddleInitial = '" . PMS_MySqlSupport::MySqlEscape( $middleInitial ) .
						"'" ;
				}
				# find the swimmer associated with this regnum in the PMS database
				$query = "SELECT RegNum,FirstName,MiddleInitial,LastName from RSIDN_$yearBeingProcessed " .
					"WHERE FirstName = '$escFirstName' AND LastName = '$escLastName' $escMiddleInitial";
				my ($sth2,$rv2) = PMS_MySqlSupport::PrepareAndExecute( $dbh, $query, "" );
					my $resultHash2 = $sth2->fetchrow_hashref;
					if( !defined( $resultHash2 ) ) {
						# THIS IS ANOTHER SPECIAL CASE!  We probably found this swimmer in the Rsidn file with
						# a slightly different name earlier, so couldn't find an exact match above.  For example,
						# they are registered as 'Peter' '' 'Wagner' but entered the meet with
						# 'Peter' 'J' 'Wagner'.  In this case we generate the synonym to make the entered
						# name the same as the registered name, but since we don't have their PMS reg number
						# we can't generate that, so we'll generate a dumm >regnumName line to be fixed (or
						# ignored) later.
						my $MI = $middleInitial;
						my $MI2;
						if( defined($MI) && ($MI ne "") ) {
							$MI2 = ",$MI";
							$MI = " $MI";
						} else {
							$MI = "";
							$MI2 = "";
						}
						my $fullName = $lastName . "," . $firstName . $MI2;
						PMSLogging::printLog( "#>regnumName \t $regNum \t> $fullName \t> " . "????-?????" .
							" \t # $firstName$MI $lastName, $dataString, $theYearBeingProcessed\n" );
					} else {
						my $MI = $resultHash2->{'MiddleInitial'};
						my $MI2;
						if( defined($MI) && ($MI ne "") ) {
							$MI2 = ",$MI";
							$MI = " $MI";
						} else {
							$MI = "";
							$MI2 = "";
						}
						my $fullName = $resultHash2->{'LastName'} . "," . $resultHash2->{'FirstName'} . $MI2;
						PMSLogging::printLog( ">regnumName \t $regNum \t> $fullName \t> " . $resultHash2->{'RegNum'} .
							" \t # $resultHash2->{'FirstName'}$MI $resultHash2->{'LastName'}, $dataString, $theYearBeingProcessed\n" );
					}
			}			
		} # end of while...
	} # end of foreach
} # end of DumpSynonyms()



1;  # end of module
