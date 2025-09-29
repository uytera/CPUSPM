#!/bin/bash

set -x
set -e

gunicorn --bind $UVICORN_HOST:$UVICORN_PORT -w 1 -k main.CustomUvicornWorker main:app
