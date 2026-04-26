#!/bin/bash
# Local Database Backup Script
# Exports your local data_analysis database for import to remote server
# Usage: ./backup-database-local.sh [output_file]

OUTPUT_FILE="${1:-backup-$(date +%Y%m%d-%H%M%S).sql}"

echo "=== Local Database Backup ==="
echo "Output file: $OUTPUT_FILE"
echo ""

# Ask for database credentials if not obvious
read -p "MySQL host [localhost]: " MYSQL_HOST
MYSQL_HOST="${MYSQL_HOST:-localhost}"

read -p "MySQL user [root]: " MYSQL_USER
MYSQL_USER="${MYSQL_USER:-root}"

read -p "MySQL database name [data_analysis]: " MYSQL_DB
MYSQL_DB="${MYSQL_DB:-data_analysis}"

read -sp "MySQL password: " MYSQL_PASSWORD
echo ""
echo ""

# Run backup
echo "Exporting database '$MYSQL_DB' from $MYSQL_HOST..."
mysqldump -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
    --single-transaction \
    --quick \
    --lock-tables=false \
    "$MYSQL_DB" > "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    BACKUP_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    TABLE_COUNT=$(grep -c "^CREATE TABLE" "$OUTPUT_FILE")
    
    echo "✓ Backup complete!"
    echo ""
    echo "File:    $OUTPUT_FILE"
    echo "Size:    $BACKUP_SIZE"
    echo "Tables:  $TABLE_COUNT"
    echo ""
    echo "Next step: Import to remote server"
    echo "  ./docs/import-database-remote.sh $OUTPUT_FILE your.server.ip root"
else
    echo "✗ Backup failed"
    exit 1
fi
