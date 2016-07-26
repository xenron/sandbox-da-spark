#!/usr/bin/perl

use strict;
use LWP::UserAgent;
use XML::XPath;


#my $urlsource="http://feeds.reuters.com/reuters/topNews" ;
my $urlsource="http://feeds.reuters.com/reuters/scienceNews" ;
#my $urlsource="http://feeds.reuters.com/reuters/UShealthcareNews" ;

# get url source content

my  $agent = LWP::UserAgent->new; 

# run an infinite loop to get rss data 

while()
{

  my  $req = HTTP::Request->new(GET => ($urlsource)); 

  $req->header('content-type' => 'application/json');
  $req->header('Accept'       => 'application/json');

  #$req->header('x-auth-token' => 'kfksj48sdfj4jd9d');
  my $resp = $agent->request($req); 

  if ( $resp->is_success ) 
  {
    # get the rss page in xml format
  
    my $xmlpage = $resp -> decoded_content;
  
    # use xpath to extract rss xml details
  
    my $xp = XML::XPath->new( xml => $xmlpage );
    my $nodeset = $xp->find( '/rss/channel/item/title' );
  
    my @titles = () ;
    my $index = 0 ;
  
    foreach my $node ($nodeset->get_nodelist)
    {
      my $xmlstring = XML::XPath::XMLParser::as_string($node) ;
  
      # clean up the rss xml 
  
       $xmlstring =~ s/<title>//g;
       $xmlstring =~ s/<\/title>//g;
       $xmlstring =~ s/"//g;
       $xmlstring =~ s/,//g;
  
       $titles[$index] = $xmlstring ;
       $index = $index + 1 ;
  
       # --- debug 
       # print "FOUND\n\n",$xmlstring,"\n\n" ; 
  
    } # foreach find node
  
  
    my $nodeset = $xp->find( '/rss/channel/item/description' );
  
    my @desc = () ;
    $index = 0 ;
  
    foreach my $node ($nodeset->get_nodelist)
    {
       my $xmlstring = XML::XPath::XMLParser::as_string($node) ;
  
       # clean up the rss xml 
  
       $xmlstring =~ s/<img.+\/img>//g;
       $xmlstring =~ s/href=".+"//g;
       $xmlstring =~ s/src=".+"//g;
       $xmlstring =~ s/src='.+'//g;
       $xmlstring =~ s/<br.+\/>//g;
       $xmlstring =~ s/<\/div>//g;
       $xmlstring =~ s/<\/a>//g;
       $xmlstring =~ s/<a >\n//g;
       $xmlstring =~ s/<img >//g;
       $xmlstring =~ s/<img \/>//g;
       $xmlstring =~ s/<div.+>//g;
       $xmlstring =~ s/<title>//g;
       $xmlstring =~ s/<\/title>//g;
       $xmlstring =~ s/<description>//g;
       $xmlstring =~ s/<\/description>//g;
       $xmlstring =~ s/&lt;.+>//g;
       $xmlstring =~ s/"//g;
       $xmlstring =~ s/,//g;
       $xmlstring =~ s/\r|\n//g;
  
       $desc[$index] = $xmlstring ;
       $index = $index + 1 ;
  
       # --- debug 
       # print "FOUND\n\n",$xmlstring,"\n\n" ; 
  
    } # foreach find node
  
    # Now output the rss news based data in json format 
  
    my $newsitems = $index ;
    $index = 0 ;
  
    for ($index=0; $index < $newsitems; $index++) {
  
      print "{\"category\": \"science\"," 
            . " \"title\": \"" .  $titles[$index] . "\","
            . " \"summary\": \"" .  $desc[$index] . "\""
             . "}\n";
  
    } # for rss items

  } # success ?

  # sleep till next rss get 

  sleep(30) ;


} # while 


