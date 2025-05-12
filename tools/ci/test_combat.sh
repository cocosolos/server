#!/bin/bash
set -e

# define bash helper function to help diagnose any sql errors
function mysqlcmd() { mysql $XI_NETWORK_SQL_DATABASE -h$XI_NETWORK_SQL_HOST -u$XI_NETWORK_SQL_LOGIN -p$XI_NETWORK_SQL_PASSWORD --verbose -e "$@"; }

# Update character information
# Place near some Robber Crabs in Kuftal Tunnel
mysqlcmd "UPDATE chars
SET
    pos_zone = 174,
    pos_prevzone = 174,
    pos_x = 55,
    pos_y = -9,
    pos_z = -140
WHERE charid = 1;"

mysqlcmd "SELECT charid, accid, charname, pos_zone, pos_x, pos_y, pos_z FROM chars;"
# Set GodMode CharVar = 1
mysqlcmd "INSERT INTO char_vars(charid, varname, value)
VALUES(1, 'GodMode', 1);"

printf "\nRunning HeadlessXI for 60 seconds\n"
python3 << EOF
import time
try:
    from tools.headlessxi.hxiclient import HXIClient
    hxi_client = HXIClient('admin1', 'admin1', 'localhost')
    hxi_client.login()
    print('Sleeping 60s')
    time.sleep(60)
    hxi_client.logout()
    exit(0)
except Exception as e:
    exit(-1)
EOF
hxi_result=$?

# fail if hxi had a non-zero exit code
if [[ "$hxi_result" -ne "0" ]]; then
    echo "hxi exited with code $hxi_result"
    exit $hxi_result
fi
