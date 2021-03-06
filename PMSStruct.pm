#!/usr/bin/perl -w
# PMSStruct.pm - various structures used by GenerateOWResults.  They are generally used across modules so
#	we define them here.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 


package PMSStruct;

use strict;
use sigtrap;
use warnings;


# The accumulated result file(s) generated by this program make use of "template" files, where specially 
# flagged names in a template file is replaced with the value of a "macro" of the same name.  
# The hash named '%macros' is used to hold all of the macros.
my %macros;
# GetMacrosRef() is used to return a reference to our %macros hash so the caller can look up a macro.
# For example, the following:
#	PMSStruct::GetMacrosRef()->{"teamInitials"}
# will access the macro named "teamInitials".  The value can then be used or set, e.g.
#	PMSStruct::GetMacrosRef()->{"teamInitials"} = "SCAM";
#	my $teamInitials = PMSStruct::GetMacrosRef()->{"teamInitials"};
#
sub GetMacrosRef() {
	return \%macros;
}


###
### General Structures used by our modules
###

# %ListToPlaceIDs is an hash that maps a swimmer's (mysql) id and category
# to an HTML ID used on the Accumulated Points web
# page.  We use this ID to create a link on the Team Points page pointing back to the Accumulated Points page.
# For every swimmer who scored points for a team there will be an entry in the @ListToPlaceIDs array.  For example:
#	@ListToPlaceIDs{"1-123"} = "M_50_54_1_888"
# which means that the swimmer with id = 123 who scored points as a CAT 1 swimmer
# can be linked to via the ID "M_50_54_1_888", e.g.:
#  http://pacificmasters.org/OWPoints/2016PacMastersAccumulatedResults.html?open=M_50_54_1_888
#
my %ListToPlaceIDs = (); 

sub SetListToPlaceIDs( $$ ) {
	my ($swimmerId, $listToPlaceID) = @_;
	$ListToPlaceIDs{$swimmerId} = $listToPlaceID;
}
sub GetListToPlaceIDs( $ ) {
	my $swimmerId = $_[0];
	return $ListToPlaceIDs{$swimmerId};
}





# SynonymFirstLastName - used to handle the >last,first property.  See PMSMacros.pm.  From that file:
# Property file syntax:    >last,first   last,first   >	 Last,First[,Extra]
# where
#	>last,first is in any case
#	last,first (name1) is in any case, can contain spaces, only one allowed (use last one seen), no commas, double quotes.
#	Last,First[,Extra] (name2) is in correct case, and can contain an optional ",Extra"
#	Any time we see a result and we extract the last,first (and extra if there) for the swimmer,
#	we see if it matches name1 (case-
#	insensitive).  If it does, we will use name2 (in Canonical form)
my %SynonymFirstLastName;   # SynonymFirstLastName {last,first in lower case} = a string "Last,First,Extra" not in lower case (,Extra is optional)

# GetSynonymFirstLastName - return the synonym for the passed name
#
# PASSED:
#	$lastFirst - a string of the form "last,first" where "last" and "first" are someone's last and first name,
#		respectively (all lower case.)  If it matches the last,first part of the >last,first property then
#		it will return the name's synonym.
#
# RETURNED:
#	synonym - the synonym for the passed last,first, or undefined.
#
sub GetSynonymFirstLastName($) {
    return $SynonymFirstLastName{$_[0]};
}
sub GetSynonymFirstLastNameRef() {
    return \%SynonymFirstLastName;
}



# SynonymRegNum - used to handle the >regnum and >regnumname properties
# >regnum badRegNum goodRegNum
# 	Reg Number synomyms.  Both reg numbers can be in any case.  Replace the "bad" with the "good", e.g.
# 		>regnum 384D-D414R 384P-0414R
#	The above will use "384P-0414R" in place of "384D-D414R" when "384D-D414R" is found in a result.
#	The synonym will be folded to UPPER CASE.
#
# >regnumName xxxxxx > last,first,middle	> yyyyyyy		where ,middle is optional
#	All fields can be in any case.
# 	'xxxxxx' can be anything but no trailing spaces (spaces prior to '>' will be removed); no >
# 	'yyyyyyy' does not contain spaces or > .   WILL CONVERT TO UPPER CASE!
# 	'last,first,middle' can contain spaces; no commas, no >.  Must be name in PMS db
#
my %SynonymRegNum;			# SynonymRegNum {regnum} = a replacement regnum
							# SynonymRegNum {regNum>fullName} = a replacement regnum for the person named fullName

# GetSynonymRegNum - determine the synonym for the passed regnum (and corresponding name, if passed)
#
# PASSED:
#	$regNum - the regnum whose synonym we're looking for
#	$first - (optional)
#	$last - optional, but required if $first is supplied
#	$middle - (optional)
#
# RETURNED:
#	$result - the synonym, or undefined.
#
sub GetSynonymRegNum {
	my ($regNum, $first, $last, $middle) = @_;
	$regNum = lc($regNum);
	my $result = undef;
	my $fullName = "";
	if( defined( $first ) ) {
		$first = lc($first);
		$last = lc($last);
		if( defined( $middle ) && ($middle ne "") ) {
			$middle = lc($middle);
			$fullName = "$last,$first,$middle";
		} else {
			$fullName = "$last,$first";
		}
    	$result = $SynonymRegNum{"$regNum>$fullName"};
	} else {
    	$result = $SynonymRegNum{$regNum};
	}
	return $result;
}
sub GetSynonymRegNumRef() {
    return \%SynonymRegNum;
}


1;  # end of module
