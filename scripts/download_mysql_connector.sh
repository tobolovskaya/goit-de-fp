#!/bin/bash

# Script to download MySQL JDBC connector
echo "Downloading MySQL JDBC Connector..."

# Create directory if it doesn't exist
mkdir -p lib

# Download MySQL connector
wget -O mysql-connector-j-8.0.32.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar

echo "MySQL JDBC Connector downloaded successfully!"
echo "File saved as: mysql-connector-j-8.0.32.jar"