#!/usr/bin/perl -w
# PMSTemplate.pm - support for processing template files (used to generate HTML).

package PMSTemplate;

use strict;
use sigtrap;
use warnings;

require PMSMacros;



# ProcessHTMLTemplate - process the passed template by substituting macro values for the macro
#	names found in the template file.
#
# PASSED:
#	$templateFileName - the name of the file we need to process
#	$outputFH - File Handle of the output file we write to
#
# RETURNED:
#	n/a
#
# SIDE EFFECTS
#	The file pointed to by the global file handle $outputFH is written.
#
sub ProcessHTMLTemplate( $$ ) {
	my $templateFileName = $_[0];
	my $outputFH = $_[1];
	my $fd;
	open( $fd, "< $templateFileName" ) || die( "ProcessHTMLTemplate():  Can't open $templateFileName: $!" );
	PMSMacros::SetTemplateName( $templateFileName );
	ProcessFile( $fd, $outputFH );
	close( $fd );
} # end of ProcessHTMLTemplate()



# ProcessFile - process the passed template file (does the work for ProcessHTMLTemplate() above.)
#
# PASSED:
#	$fd - handle to the template file
#	$outputFH - File Handle of the output file we write to
#
# RETURNED:
#	n/a
#
# SIDE EFFECTS:
#
sub ProcessFile( $$ ) {	
	my $fd = $_[0];
	my $outputFH = $_[1];
	my $line;
	my $lineNum = 0;
	
	while( ($line = <$fd>) ) {
		chomp $line;
		$lineNum++;
		$line = PMSMacros::ProcessMacros( $line, $lineNum );
		print $outputFH "$line\n";
	}
} # end of ProcessFile()

	

1;  # end of module
