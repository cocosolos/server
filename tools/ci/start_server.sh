#!/bin/bash
set -e

chmod +x xi_connect
chmod +x xi_map
chmod +x xi_search
chmod +x xi_world
ls -l

printf "\nStart server processes\n"
screen -d -m -S xi_connect ./xi_connect --log login-server.log
screen -d -m -S xi_search ./xi_search --log search-server.log
screen -d -m -S xi_map ./xi_map --log map-server.log
screen -d -m -S xi_world ./xi_world --log world-server.log
