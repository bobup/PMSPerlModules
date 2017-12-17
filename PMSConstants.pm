#!/usr/bin/perl -w
# PMSConstants.pm - a collection of program-controlling constants and variables.

# Copyright (c) 2016 Bob Upshaw.  This software is covered under the Open Source MIT License 

package PMSConstants;

####################
# programatically-adjusted program options, modified at run time based on input data.  DON'T CHANGE THESE
####################

our $INVALID_REGNUM = "(no reg num)";
our $INVALID_DOB = "1900-01-01";	# A birth date we won't normally find in our database or results

our $DEFAULT_MISSING_DATE = '1940-01-01';	# A result date we won't normally find in our results

our $NO_RESULT_FILE_PROCESSING = 0;		# by default we will initialize the DB and process all result files.  Set to 1 to use the DB as it
										# exists and not process result files.

our $SHOW_ONLY_SWIMMERS_WITH_POINTS = 1;				# true if we display only swimmers who earned points; false if we display all swimmers who swam

 #  need to export this correctly...
our $debug = 0;						# set to > 0 to turn on debugging printouts, usually to the log file (see 
									# LOG file below).  Can be set via script args.
our $MIN_GROUP_AGE;                  # the min age of swimmers who are part of the competition being analyzed, e.g. 18 for masters.

our $RegNumRequired = 0;            # We need a reg #, but we don't give up if we don't get one, because we might be able to match
                                    # their name.

our $MAX_LENGTH_TEAM_ABBREVIATION = 10;		# a team abbreviation is usually something like "MAM", but we allow
									# up to 10 chars.  

our $YEAR_RULE_CHANGE = 2017;		# the year where the rule changed causing the age of every swimmer to
									# calculated based on the swimmer's age at the end of the year, not
									# their age on the day of the swim.
# true constants
our $INVALIDAGE = -2;                # must be less than minUSAAge (and minMastersAge) - used to denote an invalid age for a person (e.g. no age given.)


# define all of the masters age groups
#our @AGEGROUPS_MASTERS = ( "18-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54",
#						  "55-59", "60-64", "65-69", "70-74", "75-79", "80-84", "85-89",
#						  "90-94", "95-99", "100-104" );
our @AGEGROUPS_MASTERS = ('18-24', '25-29', '30-34', '35-39', '40-44', '45-49',
						  '50-54', '55-59', '60-64', '65-69', '70-74', '75-79',
						  '80-84', '85-89', '90-94', '95-99', '100-104', '105-109',
						  '110-114', '115-119' );

# this is a hash of the above array - used to validate age groups
our %AGEGROUPS_MASTERS_HASH;
for( @AGEGROUPS_MASTERS ) {
	$AGEGROUPS_MASTERS_HASH{$_} = 1;
}

# we use results from two different orginazations:
our @arrOfOrg = ('PAC', 'USMS');

# we use results from different courses (short course, long course, open water, etc)
our @arrOfCourse = ('SCY', 'SCM', 'LCM', 'OW', 'SCY Records', 'SCM Records', 'LCM Records');

# Define the points for each place.  If a swimmer finishes in a place > $MAXPLACE they get no points.
our @PLACE = (); 
$PLACE[0] = 0;
$PLACE[1] = 22;
$PLACE[2] = 18;
$PLACE[3] = 16;
$PLACE[4] = 14;
$PLACE[5] = 12;
$PLACE[6] = 10;
$PLACE[7] = 8;
$PLACE[8] = 6;
$PLACE[9] = 4;
$PLACE[10] = 2;
our $MAXPLACE = 10;

# Define which place is represented by what points (mirror of above @PLACE)
our @POINTS = ();
for( my $i = 0; $i <= 10; $i++ ) {
	$POINTS[$PLACE[$i]] = $i;
}


# Define the different colors of the rows in the accumulated results.
our @trColor = ();
#$trColor[0] = "#707070";
#$trColor[1] = "#858585";
#$trColor[2] = "#9A9A9A";
#$trColor[3] = "#AFAFAF";
#$trColor[4] = "#C4C4C4";
#$trColor[5] = "#D9D9D9";
#$trColor[6] = "#EEEEEE";
$trColor[0] = "#D9D9D9";
$trColor[1] = "#FFFFFF";




1;  # end of module
