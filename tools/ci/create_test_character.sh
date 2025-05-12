#!/bin/bash
set -e

# define bash helper function to help diagnose any sql errors
function mysqlcmd() { mysql $XI_NETWORK_SQL_DATABASE -h$XI_NETWORK_SQL_HOST -u$XI_NETWORK_SQL_LOGIN -p$XI_NETWORK_SQL_PASSWORD --verbose -e "$@"; }
printf "\nPopulating database\n"

# Create an account
PASSWORD_HASH=\$2a\$12\$piFoDKvu80KK68xLgQFpt.ZCqVPTjPmhSUfA31.Yw9n404dTsrR6q
mysqlcmd "INSERT INTO accounts (id, login, password, timecreate, timelastmodify, status, priv)
VALUES(1000, 'admin1', '$PASSWORD_HASH', NOW(), NOW(), 1, 1);
SELECT id, login, content_ids FROM accounts;"

# Create a character
mysqlcmd "INSERT INTO chars (charid, accid, charname, pos_zone, nation, gmlevel)
VALUES(1, 1000, 'Test', 0, 0, 5);
SELECT charid, accid, charname, pos_zone FROM chars;"

# Set char_look (default is 0 and trips up scripting)
mysqlcmd "INSERT INTO char_look (charid, face, race, size, head, body, hands, legs, feet, main, sub, ranged)
VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
SELECT charid, face, race FROM char_look;"

# Populate more char tables with defaults
mysqlcmd "INSERT INTO char_stats (charid, mjob)
VALUES(1, 1);
SELECT charid, mjob FROM char_stats;"
