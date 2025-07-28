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
our $EMPTY_ACCUMULATED_POINTS = 0;		# by default we will NOT produce an "empty" Accumulated Points page, and we
										#	expect data to be available for processing.  If set to 1 we will ignore
										#	any data and generate an empty Accumulated Points page.
our $CANCELLED_ACCUMULATED_POINTS = 0;	# by default we will NOT produce a "cancelled" Accumulated Points page, and we
										#	expect data to be available for processing.  If set to 1 we will ignore
										#	any data and generate a cancelled Accumulated Points page.
our $SHOW_ONLY_SWIMMERS_WITH_POINTS = 1;				# true if we display only swimmers who earned points; false if we display all swimmers who swam

 #  need to export this correctly...
our $debug = 0;						# set to > 0 to turn on debugging printouts, usually to the log file.  Can be set via script args.
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
our @arrOfCourse = ('SCY', 'SCM', 'LCM', 'OW', 'SCY Records', 'SCM Records', 'LCM Records', 'ePostal', 'ePostal Records');

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

# Define the seasons for each course:
# we must consider only those results occuring during the season of interest.  A season spans
# dates that are dependent on the course, so we will define them here.  Examples are in terms
# of the 2016 season:
our %season = ( 
	"SCYstart"	=> "-06-01",		# SCY season is 2015-06-01 through 2016-05-31
	"SCYend"	=> "-05-31",
	"SCMstart"	=> "-01-01",		# SCM season is 2016-01-01 through 2016-12-31
	"SCMend"	=> "-12-31",
	"LCMstart"	=> "-10-01",		# LCM season is 2015-10-01 through 2016-09-30
	"LCMend"	=> "-09-30"
	);
our $SCYStartOfSeasonDay = "June 1";
our $SCYEndOfSeasonDay = "May 31";
our $LCMStartOfSeasonDay = "October 1";
our $LCMEndOfSeasonDay = "September 30";
our $SCMStartOfSeasonDay = "January 1";
our $SCMEndOfSeasonDay = "December 31";


### October, 2021 change by USMS:
# [Specifically, the House of Delegates voted in favor of this rule:]
#  “105.1.2 Deadlines – Times to be considered for records and Top 10 times shall be achieved 
#  and submitted as follows:
#   Long Course Meters times shall be achieved on or before September 30 (table)
# [In addition, the following was added:]
#++  Times to be considered for records and Top 10 times shall be achieved on or 
#++  before October 10, 2021, for Long Course Meters. This provision will be implemented 
#++  immediately following the conclusion of the annual meeting, expire at the conclusion 
#++  of the LCM National Championship meet and will not be included in the 2022 USMS 
#++  Masters Swimming Code of Regulations and Rules of Competition.”

#
# FixSeasonRange - adjust the season begin and/or end dates for every season for the passed
#	year.  See Notes below.
#
# PASSED:
#	$yearBeingProcessed - the year we're processing, and for which the seasons will be
#		adjusted if necessary.
#
# RETURNED:
#	n/a
#
# SIDE EFFECTS:
#	The %season hash (defined above) will be adjusted as necessary.
#
# NOTES:
#	This routine can be called multiple times with no ill effects.
#	See the comments above below the definition of the %season hash. Basically, USMS sometimes
#	changes the date range of seasons during a specific year.  For example, the year 2021 (which
#	also resulted in a change to 2022.) See USMS rule 105.1.2.  As of Dec 3, 2024 you can also
#	look at https://www.usms.org/events/top-10 .
#	Note that most seasons require no changes, so in that case this routine will do nothing.
#
sub FixSeasonRange( $ ) {
	my $yearBeingProcessed = $_[0];
	if( $yearBeingProcessed eq "2021" ) {
		$season{"LCMend"} = "-10-10";
		$LCMEndOfSeasonDay = "October 10";
	} elsif( $yearBeingProcessed eq "2022" ) {
		$season{"LCMstart"} = "-10-11";
		$LCMStartOfSeasonDay = "October 11";
	} elsif( $yearBeingProcessed eq "2024" ) {
		$season{"SCYend"} = "-06-24";
		$SCYEndOfSeasonDay = "June 24";
	} elsif( $yearBeingProcessed eq "2025" ) {
		$season{"SCYstart"} = "-06-25";
		$SCYStartOfSeasonDay = "June 25";
	#	$season{"SCYstart"} = "-06-1";
	#	$SCYStartOfSeasonDay = "June 1";
	}
	
	# the following macros are used to make the defined seasons available to template files:
	PMSStruct::GetMacrosRef()->{"SCYStartOfSeasonDay"} = $PMSConstants::SCYStartOfSeasonDay;
	PMSStruct::GetMacrosRef()->{"SCYEndOfSeasonDay"} = $PMSConstants::SCYEndOfSeasonDay;
	PMSStruct::GetMacrosRef()->{"LCMStartOfSeasonDay"} = $PMSConstants::LCMStartOfSeasonDay;
	PMSStruct::GetMacrosRef()->{"LCMEndOfSeasonDay"} = $PMSConstants::LCMEndOfSeasonDay;
	PMSStruct::GetMacrosRef()->{"SCMStartOfSeasonDay"} = $PMSConstants::SCMStartOfSeasonDay;
	PMSStruct::GetMacrosRef()->{"SCMEndOfSeasonDay"} = $PMSConstants::SCMEndOfSeasonDay;

} # end of FixSeasonRange()

# see the properties.txt file to see how the following is used:
our $NoResultsPath = "NO RESULTS";	# event sanctioned and scheduled; not swum or no results yet
our $NoDetailsYet  = "NO DETAILS";	# event sanctioned by no details on what and when
our $NoSanctionYet = "NO SANCTION";	# event no sanctioned


1;  # end of module
