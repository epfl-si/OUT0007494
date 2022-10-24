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
  if (m/^use `(.*)`/m) {
    $usedb = $1;
  }
  next unless ($usedb || "") eq $dbname;

  next unless m/\n.*\n/m;                   # Empty record
  next if m|/\* added by mysqlbinlog \*/|;  # Doing whatever meta

  my $features = { db => $usedb, getFeatures($_) };

  warn "----- BEGIN SUSPICIOUS RECORD -----\n$_\n----- END SUSPICIOUS RECORD -----\n" unless $features->{sql};
  print encode_json($features), "\n";
}

sub getFeatures {
  local $_ = shift;
  my @features;

  if (m|^SET TIMESTAMP=(\d+)|m) {
    push(@features, timestamp => $1);
  }

  if (m{^((?:begin|insert|update|delete).*)/[*]![*]/;}msi) {
    push(@features, sql => $1);
  }

  return @features;
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
