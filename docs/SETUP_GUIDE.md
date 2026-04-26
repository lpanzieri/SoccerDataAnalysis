# Automated Server Setup Guide

This guide covers using automated scripts to set up your VPS for the Data-Analysis prediction engine, with your existing database.

## Overview

Two scripts automate the setup process:

1. **setup-server.sh** — Main server initialization (dependencies, Conda, Apache, cron jobs)
2. **import-database-remote.sh** — Import your local database backup to the remote server

## Prerequisites

- SSH access to remote server with sudo privileges
- Local MySQL database backup (exported with `mysqldump`)
- Git repository cloned on local machine

## Quick Start (3 Steps)

### Step 1: Export Your Local Database

Export your existing local database to a file:

```bash
mysqldump -u root -p data_analysis > backup.sql
# Or if using different credentials:
# mysqldump -h localhost -u your_user -p your_password your_database > backup.sql
```

### Step 2: Run Server Setup Script

On your local machine, copy the setup script to the server and execute it:

```bash
scp docs/setup-server.sh user@your.server.ip:/tmp/
ssh user@your.server.ip
chmod +x /tmp/setup-server.sh
/tmp/setup-server.sh
```

The script will:
- ✓ Install system dependencies (Python, MySQL client, Apache2)
- ✓ Set up Conda environment (Python 3.12)
- ✓ Create project directories with correct permissions
- ✓ Create `.cron.env` with your database credentials and API key
- ✓ Configure Apache to serve reports
- ✓ Schedule cron jobs (5-min worker, 30-min health check, 3:20 AM backup)
- ✓ Verify MySQL connectivity (against your restored database)

### Step 3: Import Your Database

On your local machine:

```bash
chmod +x docs/import-database-remote.sh
./docs/import-database-remote.sh backup.sql your.server.ip root
```

The script will:
- ✓ Verify backup file exists
- ✓ Upload backup to remote server
- ✓ Import into MySQL database
- ✓ Verify import success (count tables)
- ✓ Clean up temporary files

## Detailed Walkthrough

### 1. Database Export

```bash
# Standard export
mysqldump -u root -p data_analysis > backup.sql

# With size estimate
ls -lh backup.sql

# Optional: verify backup integrity
mysql -u root -p < backup.sql --dry-run
```

### 2. Server Setup Execution

The setup script is interactive and will ask for:

```
Project directory: [/var/www/data-analysis]
MySQL host: [localhost]
MySQL user: [data_analysis]
MySQL password: [prompt]
Database name: [data_analysis]
API-football API key: [prompt]
```

**Output confirmation points:**
- ✓ MySQL connection successful
- ✓ Database tables verified
- ✓ .cron.env created (permissions 600)
- ✓ Apache configured and restarted
- ✓ Cron jobs scheduled
- ✓ All packages available

### 3. Database Import

The import script handles:
- SSH file transfer (SCP)
- Remote MySQL import
- Verification (table count)
- Cleanup (removes temp file)

Example with custom credentials:

```bash
./docs/import-database-remote.sh backup.sql your.server.ip ubuntu
# Then enter: data_analysis (user), data_analysis (db), password
```

## Post-Setup Verification

After both scripts complete, verify the setup:

### Check on Remote Server

```bash
# Verify Conda environment
/opt/miniconda3/bin/conda run -n data-analysis python --version

# Check MySQL database
mysql -u data_analysis -p -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='data_analysis';"

# Verify Apache
sudo systemctl status apache2

# Check cron jobs
crontab -l

# Check logs
ls -la /var/www/data-analysis/logs/

# View reports directory
ls -la /var/www/data-analysis/reports/
```

### Run Health Check

```bash
ssh user@your.server.ip
source /var/www/data-analysis/.cron.env
/opt/miniconda3/bin/conda run -n data-analysis python /var/www/data-analysis/scripts/maintenance/health_check.py
```

### Access Reports

Open browser to:
```
http://your.server.ip/
```

You should see a directory listing of reports (initially empty until predictions run).

## Troubleshooting

### MySQL Connection Failed

```bash
# Verify credentials in .cron.env
cat /var/www/data-analysis/.cron.env

# Test connection manually
mysql -h localhost -u data_analysis -p
```

### Database Import Hangs

- Check backup file size: `du -h backup.sql`
- On remote, check MySQL is running: `sudo systemctl status mysql`
- Monitor import progress: `ssh user@host "watch 'SELECT COUNT(*) FROM table_name;'"`

### Cron Jobs Not Running

```bash
# Check cron logs
sudo tail -f /var/log/syslog | grep CRON

# Verify .cron.env is readable by cron user
ls -la /var/www/data-analysis/.cron.env

# Test cron command manually
source /var/www/data-analysis/.cron.env
/opt/miniconda3/bin/conda run -n data-analysis python /var/www/data-analysis/scripts/maintenance/health_check.py
```

### Apache Not Serving Reports

```bash
# Check Apache error logs
sudo tail -f /var/www/data-analysis/logs/apache_error.log

# Verify site is enabled
sudo a2ensite data-analysis

# Restart Apache
sudo systemctl restart apache2
```

## Customization

### Changing Project Directory

Edit the `PROJECT_DIR` variable in `setup-server.sh`:

```bash
# Default: /var/www/data-analysis
# Change to (e.g.): /opt/data-analysis
```

### Adding More Cron Jobs

Edit the crontab section in `setup-server.sh` to add additional schedules:

```bash
# Weekly optimization (Sunday 2 AM)
0 2 * * 0 source $CRON_ENV_FILE && $CONDA_ACTIVATE python $SCRIPTS_DIR/maintenance/optimize_tables.py >> $LOGS_DIR/optimize.log 2>&1
```

### Adjusting API Rate Limiting

In `.cron.env`, modify:

```bash
export API_RATE_LIMIT=9        # requests per window
export API_RATE_WINDOW=60      # seconds
```

## File Structure After Setup

```
/var/www/data-analysis/
├── .cron.env                    # Environment variables (secrets, DB credentials)
├── scripts/
│   ├── helpers/                 # Copy your helpers here
│   ├── maintenance/             # Copy your maintenance scripts here
│   └── cron/
│       └── run-with-env.sh      # Wrapper to source .cron.env in cron context
├── reports/                     # Apache serves HTML reports from here
├── logs/
│   ├── worker.log
│   ├── health_check.log
│   ├── backup.log
│   └── apache_*.log
└── [...your project files...]
```

## Security Notes

- `.cron.env` has permissions 600 (readable only by owner)
- Store API keys in `.cron.env`, not in code
- Use strong MySQL passwords
- Consider firewall rules on remote server
- Monitor Apache error logs for issues
- Rotate database backups regularly

## Next Steps

1. **Copy your scripts** to `/var/www/data-analysis/scripts/` (via SCP or Git clone)
2. **Run health check** to verify all connections work
3. **Wait for first cron cycle** (5 minutes) to generate initial reports
4. **Monitor logs** for any issues in first 24 hours
5. **Set up monitoring** alerts for backup job success/failure

---

For detailed deployment reference, see `VPS_Apache_Deployment_Guide.pdf`.
