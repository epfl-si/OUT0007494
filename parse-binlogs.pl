#!/usr/bin/env perl

use JSON;
use warnings qw(all);
use strict;

die <<"USAGE" unless $#ARGV == 1;
Usage: $0 <gzipped binlog file> <dbname>

Extracts the binlog records pertaining to <dbname> from <gzipped
binlog file> to standard output, with one JSON record per line.

USAGE

my ($gzipped_binlog_file, $dbname) = @ARGV;

open(STDIN, "gzip -dc '$gzipped_binlog_file' | mysqlbinlog - |") ||
  die "$gzipped_binlog_file: $! (error code $?);";

my $usedb;
while($_ = nextRecord()) {
  my $features = {};
  if (m/^use `(.*)`/m) {
    $usedb = $1;
  }
  $features->{db} = $usedb;

  if (m|^SET TIMESTAMP=(\d+)|m) {
    $features->{timestamp} = $1;
  }

  if (m{^((?:insert|update|delete).*)/[*]![*]/;}ms) {
    $features->{sql} = $1;
  }
  print encode_json($features), "\n" if ($features->{db} || "") eq $dbname;
}

###################################################################

use vars qw($lastLine $eof);
sub peekLine {
  unless (defined($lastLine)) {
    if (! $eof) {
      $lastLine = <STDIN>;
      $eof = 1 unless defined($lastLine);
    }
  }
  return $lastLine;
}

sub consumeLine {
  my $ret = peekLine;
  $lastLine = undef;
  return $ret;
}

sub nextRecord {
  my $record;
  while(1) {
    last if $eof;
    $record .= consumeLine;
    last if (peekLine && peekLine =~ m/^# at /);
  }
  return $record;
}
