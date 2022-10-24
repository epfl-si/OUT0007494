#!/bin/sh

(case "$1" in
     *.gz) gzip -dc < "$1" ;;
     *.zip) unzip -p "$1" -x __MACOSX/'*' ;;
     *) cat "$1" ;;
esac) | \
perl -ne 's/\) ENGINE=.*;/\);/ ; s/AUTO_INCREMENT//; s/enum\(.*?\)/VARCHAR(100)/;
          next if m/LOCK TABLES/; s/DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP//;
          next if /\bKEY\b/ and not /\bPRIMARY KEY\b/; print;' | \
perl -0 -pe 's/,\n\)/)/g; s/\\'\''/'\'''\''/g; '
