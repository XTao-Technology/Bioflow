#!/bin/bash
echo "postgresql bioflow"

RUNFLG="/var/lib/pgsql/9.4/data/runflg"

if [ ! -f $RUNFLG ] ; then

    /start_postgresql.sh &

    while ! /bin/psql -U postgres -h 127.0.0.1 -q -c "select true;"; do sleep 1; done

    /bin/psql -U postgres -h 127.0.0.1 -c "CREATE USER bioflow ;"

    /bin/psql -U postgres -h 127.0.0.1 -c "CREATE DATABASE \"bioflow\" OWNER bioflow;"

	/bin/psql -U bioflow -d "bioflow" -h 127.0.0.1 -f /bioflow.sql

    exit 0

fi

echo "PostgreSQL BIOFLOW starting ...."

sudo -u postgres /usr/pgsql-9.4/bin/postgres -D /var/lib/pgsql/9.4/data

echo "PostgreSQL BIOFLOW down ...."

