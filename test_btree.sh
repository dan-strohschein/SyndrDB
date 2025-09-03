#!/bin/bash

# Test B-Tree index creation functionality

cd /Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB

echo "Starting SyndrDB server..."
./bin/server/server &
SERVER_PID=$!
sleep 2

echo "Testing B-Tree index creation..."

# Create bundle and B-Tree index
echo "CREATE BUNDLE \"test_bundle\" TYPE DOCUMENT;" | ./bin/client/client -h 127.0.0.1 -p 1776 -database default -username user -password password
sleep 1

echo "CREATE B-INDEX \"title_idx\" ON BUNDLE \"test_bundle\" WITH FIELDS ({\"title\", false, false});" | ./bin/client/client -h 127.0.0.1 -p 1776 -database default -username user -password password
sleep 1

echo "SHOW INDEXES;" | ./bin/client/client -h 127.0.0.1 -p 1776 -database default -username user -password password

echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Test complete."
