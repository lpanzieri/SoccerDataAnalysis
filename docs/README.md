# Server Setup & Deployment Documentation

This directory contains everything you need to deploy the Data-Analysis prediction engine to a VPS with Apache.

## Quick Navigation

### 📋 Setup Guides
- **[SETUP_GUIDE.md](SETUP_GUIDE.md)** — Start here! Complete walkthrough for automated server setup with your existing database
- **[VPS_Apache_Deployment_Guide.pdf](VPS_Apache_Deployment_Guide.pdf)** — Detailed reference manual (3 formats: PDF, DOCX, Markdown)

### 🔧 Automated Scripts

Three shell scripts handle database export and server initialization:

1. **backup-database-local.sh** — Export your local database
   ```bash
   ./docs/backup-database-local.sh [output_file]
   ```
   Creates a MySQL dump file for remote import.

2. **setup-server.sh** — Initialize remote VPS
   ```bash
   scp docs/setup-server.sh user@server:/tmp/
   ssh user@server /tmp/setup-server.sh
   ```
   Installs dependencies, configures Conda, sets up Apache, schedules cron jobs.

3. **import-database-remote.sh** — Import backup to remote server
   ```bash
   ./docs/import-database-remote.sh backup.sql your.server.ip root
   ```
   Transfers and imports your database backup via SSH.

## Setup Workflow (3 Steps)

### Step 1: Export Local Database
```bash
cd docs
./backup-database-local.sh
# Creates backup-YYYYMMDD-HHMMSS.sql
```

### Step 2: Set Up Remote Server
```bash
scp setup-server.sh user@your.server.ip:/tmp/
ssh user@your.server.ip chmod +x /tmp/setup-server.sh
ssh user@your.server.ip /tmp/setup-server.sh
# Answer interactive prompts for MySQL, API key, directories
```

### Step 3: Import Database
```bash
./import-database-remote.sh backup-YYYYMMDD-HHMMSS.sql your.server.ip root
# Uploads, imports, verifies, cleans up
```

## What Gets Set Up

After running these scripts:

✅ **System Dependencies**
- Python 3.12 (via Conda)
- MySQL client
- Apache2 with headers module
- Build tools and libraries

✅ **Python Environment**
- Conda environment: `data-analysis`
- Packages: mysql-connector-python, numpy, scipy, requests, pillow

✅ **Project Structure**
- `/var/www/data-analysis/` — Main project directory
- `scripts/` — Your prediction and maintenance scripts
- `reports/` — HTML output (served by Apache)
- `logs/` — Cron job and Apache logs

✅ **Configuration**
- `.cron.env` — Secrets: MySQL credentials, API key, rate limits
- Apache vhost configuration for static report serving
- Cron jobs:
  - Every 5 minutes: Match prediction worker
  - Every 30 minutes: Health check
  - 3:20 AM daily: Database backup

✅ **Security**
- `.cron.env` permissions: 600 (owner only)
- Apache serves static files (no dynamic execution)
- Database credentials isolated from code

## File Descriptions

| File | Purpose | Size |
|------|---------|------|
| SETUP_GUIDE.md | Interactive walkthrough with screenshots/examples | 7 KB |
| VPS_Apache_Deployment_Guide.pdf | Complete reference manual with operational details | 89 KB |
| VPS_Apache_Deployment_Guide.docx | Same content in Word format for editing | 15 KB |
| VPS_Apache_Deployment_Guide.md | Same content in Markdown for version control | 7.4 KB |
| setup-server.sh | Main initialization script | 7.5 KB |
| import-database-remote.sh | Database import utility | 1.8 KB |
| backup-database-local.sh | Local database export helper | 1.4 KB |

## Key Configuration Details

### Directories
```
/var/www/data-analysis/
├── .cron.env              # Secrets (MySQL, API key)
├── scripts/
│   ├── helpers/           # Your helpers/ directory
│   ├── maintenance/       # Your maintenance/ directory
│   └── cron/
│       └── run-with-env.sh
├── reports/               # Apache serves from here
└── logs/
    ├── worker.log
    ├── health_check.log
    └── backup.log
```

### Cron Schedule
```bash
*/5  * * * * — Match prediction worker (every 5 minutes)
*/30 * * * * — Health check (every 30 minutes)
20   3 * * * — Daily backup at 3:20 AM
```

### Apache Configuration
- Document root: `/var/www/data-analysis/reports/`
- Virtual host: `data-analysis` (enabled by script)
- Access: Reports served as static HTML
- Future: Can add reverse proxy for API endpoints

### Environment Variables (.cron.env)
```bash
MYSQL_HOST              # Database server
MYSQL_USER              # Database user
MYSQL_PASSWORD          # Database password
MYSQL_DB                # Database name
API_FOOTBALL_KEY        # API-football API key
PROJECT_DIR             # /var/www/data-analysis
LOGS_DIR                # Project logs directory
REPORTS_DIR             # Reports directory
CONDA_ENV               # data-analysis
API_RATE_LIMIT          # 9 (requests per window)
API_RATE_WINDOW         # 60 (seconds)
```

## Verification Checklist

After setup completes, verify:

- [ ] `mysql -u data_analysis -p` connects successfully
- [ ] Database shows correct table count
- [ ] `sudo systemctl status apache2` shows running
- [ ] `crontab -l` shows 3 scheduled jobs
- [ ] `/opt/miniconda3/bin/conda run -n data-analysis python --version` returns Python 3.12.x
- [ ] `http://your.server.ip/` shows reports directory (initially empty)

## Troubleshooting

See **[SETUP_GUIDE.md → Troubleshooting](SETUP_GUIDE.md#troubleshooting)** for:
- MySQL connection issues
- Database import hangs
- Cron jobs not running
- Apache not serving reports

## Next Steps

1. **Copy your scripts to remote:**
   ```bash
   scp -r scripts/helpers scripts/maintenance user@server:/var/www/data-analysis/scripts/
   scp -r scripts/your_main_scripts user@server:/var/www/data-Analysis/scripts/
   ```

2. **Run health check:**
   ```bash
   ssh user@server
   source /var/www/data-analysis/.cron.env
   /opt/miniconda3/bin/conda run -n data-analysis python /var/www/data-analysis/scripts/maintenance/health_check.py
   ```

3. **Monitor first execution:**
   - Wait 5 minutes for first worker cycle
   - Check logs: `tail -f logs/worker.log`
   - View reports: `http://your.server.ip/`

4. **Set up monitoring/alerts** (optional):
   - Log aggregation for `logs/`
   - Alerts on health check failures
   - Backup verification notifications

## Additional Resources

- **nice-to-have.txt** — Future improvements (Flask API, mod_wsgi integration)
- **VPS_Apache_Deployment_Guide** — Complete operational manual (PDF/DOCX/Markdown)
  - Requirements and runtime dependencies
  - Detailed setup walkthrough
  - Cron job documentation
  - API-football rate limiting
  - Health monitoring
  - Backup procedures
  - Troubleshooting guide
  - Production pitfalls and red flags

## Support

For questions about:
- **Installation** → See SETUP_GUIDE.md
- **Architecture** → See VPS_Apache_Deployment_Guide.pdf
- **Scripts** → Check inline comments in `.sh` files
- **Cron jobs** → See deployment guide section on scheduling
- **Python dependencies** → Check nice-to-have.txt for future enhancements

---

**Last Updated:** April 26, 2026
**Tested On:** Ubuntu 20.04+ with Apache 2.4, MySQL 5.7+, Python 3.12
