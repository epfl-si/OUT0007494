library(tidyverse)
library(RSQLite)
library(jsonlite)
library(stringr)
library(lubridate)

load.sql <- function(gzip_sql_path, sqlite3_db_path) {
    gzip_sql_path <- path.expand(gzip_sql_path)
    if (! file.exists(sqlite3_db_path)) {
        system(paste("./sqliteify.sh ", shQuote(gzip_sql_path),
                     " | sqlite3 ", shQuote(sqlite3_db_path)))
    }
    dbConnect(RSQLite::SQLite(), sqlite3_db_path)  %>% tbl("common")
}

db_7h11 <- load.sql("~/Downloads/transfer_99638_files_7c8eb7a3/mysqldump_cv_20221018_0711.sql.gz", "7h11.db")
db_after_500s <- load.sql("~/Downloads/cv_2022-10-18.sql.zip", "db_after_500s.db")

photo_ts <-
    db_7h11 %>%
    select(sciper, photo_ts) %>%
    collect %>%
    left_join(db_after_500s %>% select(sciper, photo_ts) %>% collect,
              by = c("sciper"),
              suffix=c("_7h11", "_after_500s"))

photo_ts %>% filter(photo_ts_7h11 != photo_ts_after_500s)

load.binlogs <- function(path) {
    tmp_jsonp <- basename(path) %>%
        gsub("\\.gz$", "", .) %>%
        paste(".jsonp", sep = "")
    if (! file.exists(tmp_jsonp)) {
        stopifnot(0 == system(paste(
                           "perl parse-binlogs.pl ", path, "cv",
                           " > ", tmp_jsonp), intern = TRUE))
    }
    stream_in(file(tmp_jsonp))
}

binlogs_dir <- path.expand("~/Downloads/transfer_99638_files_7c8eb7a3")
binlogs <- list.files(path = binlogs_dir, pattern = "mysql-bin.*\\.gz$") %>%
    tibble %>%
    rowwise %>%
    summarise({
        tibble(
            basename = .,
            load.binlogs(paste(binlogs_dir, ., sep="/")))
    }) %>%
    ungroup %>%
    mutate(time = timestamp %>% as.integer %>% as_datetime,
           sql_operation = word(sql, 1))

