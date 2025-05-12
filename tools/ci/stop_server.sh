#!/bin/bash
set -e

pkill -SIGINT xi_map
pkill -SIGINT xi_search
pkill -SIGINT xi_connect
pkill -SIGINT xi_world
