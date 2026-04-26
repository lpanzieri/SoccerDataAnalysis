#!/bin/bash
# Database Import Script
# Imports backup from local machine to remote server
# Usage: ./import-database-remote.sh [backup_file] [remote_host] [remote_user]

BACKUP_FILE="${1:?Backup file required}"
REMOTE_HOST="${2:?Remote host required}"
REMOTE_USER="${3:-root}"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: Backup file '$BACKUP_FILE' not found"
    exit 1
fi

echo "=== Remote Database Import ==="
echo "Backup file: $BACKUP_FILE"
echo "Remote host: $REMOTE_HOST"
echo "Remote user: $REMOTE_USER"
echo ""

# Calculate backup size
BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
echo "Backup size: $BACKUP_SIZE"
echo ""

# Ask for confirmation
read -p "Proceed with import? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Import cancelled"
    exit 1
fi

# Read database credentials
read -p "MySQL user on remote [data_analysis]: " MYSQL_USER
MYSQL_USER="${MYSQL_USER:-data_analysis}"

read -p "MySQL database name on remote [data_analysis]: " MYSQL_DB
MYSQL_DB="${MYSQL_DB:-data_analysis}"

read -sp "MySQL password on remote: " MYSQL_PASSWORD
echo ""

echo ""
echo "Uploading backup to $REMOTE_HOST..."
scp "$BACKUP_FILE" "$REMOTE_USER@$REMOTE_HOST:/tmp/backup_import.sql"

echo "Importing into database on remote server..."
ssh "$REMOTE_USER@$REMOTE_HOST" << EOF
    echo "Importing backup to $MYSQL_DB..."
    mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" < /tmp/backup_import.sql
    
    echo "Verifying import..."
    TABLES=\$(mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$MYSQL_DB';" | tail -1)
    echo "Database has \$TABLES tables"
    
    echo "Cleaning up..."
    rm /tmp/backup_import.sql
    
    echo "Import complete!"
EOF

echo "Database import finished successfully"
