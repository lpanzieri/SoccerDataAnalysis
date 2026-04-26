#!/bin/bash
# VPS Apache Setup Script
# Automates server initialization for Data-Analysis prediction engine
# Prerequisites:
#   - MySQL database already restored/copied to server
#   - User has sudo privileges
#   - SSH access configured

set -e

echo "=== Data-Analysis VPS Setup ==="
echo "This script will:"
echo "  1. Install system dependencies"
echo "  2. Set up Python/Conda environment"
echo "  3. Create required directories and permissions"
echo "  4. Configure environment variables"
echo "  5. Schedule cron jobs"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration variables
PROJECT_DIR="/var/www/data-analysis"
SCRIPTS_DIR="$PROJECT_DIR/scripts"
REPORTS_DIR="$PROJECT_DIR/reports"
LOGS_DIR="$PROJECT_DIR/logs"
CRON_ENV_FILE="$PROJECT_DIR/.cron.env"
CONDA_ENV_NAME="data-analysis"

# Ask for configuration
read -p "Enter project directory path [$PROJECT_DIR]: " user_input
PROJECT_DIR="${user_input:-$PROJECT_DIR}"
SCRIPTS_DIR="$PROJECT_DIR/scripts"
REPORTS_DIR="$PROJECT_DIR/reports"
LOGS_DIR="$PROJECT_DIR/logs"
CRON_ENV_FILE="$PROJECT_DIR/.cron.env"

read -p "Enter MySQL host [localhost]: " MYSQL_HOST
MYSQL_HOST="${MYSQL_HOST:-localhost}"

read -p "Enter MySQL user [data_analysis]: " MYSQL_USER
MYSQL_USER="${MYSQL_USER:-data_analysis}"

read -sp "Enter MySQL password: " MYSQL_PASSWORD
echo ""

read -p "Enter MySQL database name [data_analysis]: " MYSQL_DB
MYSQL_DB="${MYSQL_DB:-data_analysis}"

read -p "Enter API-football API key: " API_KEY

# Step 1: Update system packages
echo ""
echo -e "${YELLOW}Step 1: Updating system packages...${NC}"
sudo apt-get update
sudo apt-get install -y \
    python3 python3-pip python3-venv \
    mysql-client \
    git \
    curl wget \
    build-essential libssl-dev libffi-dev
# Note: Apache2 assumed to be pre-installed as part of LAMP stack

# Step 2: Install Conda (if not already installed)
echo ""
echo -e "${YELLOW}Step 2: Installing/Verifying Conda...${NC}"
if ! command -v conda &> /dev/null; then
    echo "Installing Miniconda..."
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p /opt/miniconda3
    /opt/miniconda3/bin/conda init bash
    rm /tmp/miniconda.sh
    export PATH="/opt/miniconda3/bin:$PATH"
else
    echo "Conda already installed"
fi

# Step 3: Create project directories
echo ""
echo -e "${YELLOW}Step 3: Creating project directories...${NC}"
sudo mkdir -p "$PROJECT_DIR"
sudo mkdir -p "$REPORTS_DIR"
sudo mkdir -p "$LOGS_DIR"
sudo chown -R $USER:$USER "$PROJECT_DIR"
chmod 755 "$REPORTS_DIR"

# Step 4: Set up Conda environment
echo ""
echo -e "${YELLOW}Step 4: Setting up Conda environment...${NC}"
/opt/miniconda3/bin/conda create -y -n "$CONDA_ENV_NAME" python=3.12
/opt/miniconda3/bin/conda run -n "$CONDA_ENV_NAME" pip install \
    mysql-connector-python \
    numpy \
    scipy \
    requests \
    pillow

# Step 5: Create .cron.env file
echo ""
echo -e "${YELLOW}Step 5: Creating .cron.env file...${NC}"
cat > "$CRON_ENV_FILE" << EOF
# Environment variables for cron jobs
# Source this file before running prediction scripts

export MYSQL_HOST="$MYSQL_HOST"
export MYSQL_USER="$MYSQL_USER"
export MYSQL_PASSWORD="$MYSQL_PASSWORD"
export MYSQL_DB="$MYSQL_DB"
export API_FOOTBALL_KEY="$API_KEY"
export PROJECT_DIR="$PROJECT_DIR"
export LOGS_DIR="$LOGS_DIR"
export REPORTS_DIR="$REPORTS_DIR"
export CONDA_ENV="$CONDA_ENV_NAME"

# Rate limiting for API-football
export API_RATE_LIMIT=9
export API_RATE_WINDOW=60
EOF

chmod 600 "$CRON_ENV_FILE"
echo -e "${GREEN}✓ .cron.env created${NC}"

# Step 6: Verify MySQL connection
echo ""
echo -e "${YELLOW}Step 6: Verifying MySQL connection...${NC}"
if mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "SELECT 1" &> /dev/null; then
    echo -e "${GREEN}✓ MySQL connection successful${NC}"
    
    # Check if database exists and has tables
    TABLE_COUNT=$(mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" -e "SHOW TABLES" | wc -l)
    if [ $TABLE_COUNT -gt 1 ]; then
        echo -e "${GREEN}✓ Database '$MYSQL_DB' has tables (count: $((TABLE_COUNT - 1)))${NC}"
    else
        echo -e "${YELLOW}⚠ Database '$MYSQL_DB' appears empty. Please import your backup.${NC}"
    fi
else
    echo -e "${RED}✗ MySQL connection failed. Check credentials and server access.${NC}"
    exit 1
fi

# Step 7: Configure Apache
echo ""
echo -e "${YELLOW}Step 7: Configuring Apache...${NC}"
# Apache should already be running as part of LAMP stack
if ! sudo systemctl is-active --quiet apache2; then
    echo -e "${YELLOW}Starting Apache...${NC}"
    sudo systemctl start apache2
fi

# Step 8: Schedule cron jobs
echo ""
echo -e "${YELLOW}Step 8: Scheduling cron jobs...${NC}"

# Create cron script wrapper that sources .cron.env
mkdir -p "$SCRIPTS_DIR/cron"
cat > "$SCRIPTS_DIR/cron/run-with-env.sh" << 'CRON_SCRIPT'
#!/bin/bash
source "$PROJECT_DIR/.cron.env"
/opt/miniconda3/bin/conda run -n "$CONDA_ENV" python "$@"
CRON_SCRIPT
chmod +x "$SCRIPTS_DIR/cron/run-with-env.sh"

# Get the conda activation command
CONDA_ACTIVATE="/opt/miniconda3/bin/conda run -n $CONDA_ENV"

# Add cron jobs (backup existing crontab first)
crontab -l > /tmp/crontab.bak 2>/dev/null || true

# Create new crontab entries
cat >> /tmp/crontab.new << CRONTAB
# Data-Analysis Prediction Engine
# 5-minute match prediction worker
*/5 * * * * source $CRON_ENV_FILE && $CONDA_ACTIVATE python $SCRIPTS_DIR/maintenance/match_predictions_worker.py >> $LOGS_DIR/worker.log 2>&1

# 30-minute status check
*/30 * * * * source $CRON_ENV_FILE && $CONDA_ACTIVATE python $SCRIPTS_DIR/maintenance/health_check.py >> $LOGS_DIR/health_check.log 2>&1

# Daily backup at 3:20 AM
20 3 * * * source $CRON_ENV_FILE && $CONDA_ACTIVATE python $SCRIPTS_DIR/maintenance/backup_database.py >> $LOGS_DIR/backup.log 2>&1
CRONTAB

# Install new crontab
crontab /tmp/crontab.new
echo -e "${GREEN}✓ Cron jobs scheduled${NC}"

# Step 9: Verify setup
echo ""
echo -e "${YELLOW}Step 9: Verifying setup...${NC}"

echo "Checking Python environment:"
$CONDA_ACTIVATE python --version

echo "Checking required packages:"
$CONDA_ACTIVATE python -c "import mysql.connector, numpy, scipy, requests, PIL; print('✓ All packages available')"

echo "Checking directories:"
ls -la "$PROJECT_DIR" | head -5

echo "Checking Apache:"
sudo systemctl is-active apache2 || echo "Apache not running!"

echo ""
echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo "Next steps:"
echo "  1. If you haven't already, import your database backup:"
echo "     mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DB < backup.sql"
echo ""
echo "  2. Copy your scripts to $SCRIPTS_DIR:"
echo "     (maintenance/, helpers/, your main scripts)"
echo ""
echo "  3. Verify cron jobs:"
echo "     crontab -l"
echo ""
echo "  4. Check health:"
echo "     $CONDA_ACTIVATE python $SCRIPTS_DIR/maintenance/health_check.py"
echo ""
echo "  5. View reports at:"
echo "     http://$(hostname -I | awk '{print $1}')/"
echo ""
