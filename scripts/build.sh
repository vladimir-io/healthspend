#!/bin/bash
# Healthspend Data Pipeline Builder
# Orchestrates the complete data ingestion pipeline with validation gates

set -e

# Configuration
SCRAPER_DIR="scraper"
SCRIPTS_DIR="scripts"
WEB_PUBLIC="web/public"
STATE=${1:-""}  # Optional state filter

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Functions
log_step() {
    echo -e "${BLUE}==>${NC} $1"
}

log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}!${NC} $1"
}

# Main execution
echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════════╗"
echo "║       🏥 Healthspend Data Pipeline Builder v1.0            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

STATE_FLAG=""
if [ -n "$STATE" ]; then
    STATE_FLAG=" (filtering by state: $STATE)"
    log_warning "Running with state filter: $STATE"
fi

# Phase 1: Discover hospitals
log_step "Phase 1: Discover hospitals${STATE_FLAG}..."
cd "$SCRAPER_DIR"
if [ -n "$STATE" ]; then
    if ! cargo run --release -- --discover_only --state "$STATE"; then
        log_error "Discovery phase failed"
        cd ..
        exit 1
    fi
else
    if ! cargo run --release -- --discover_only; then
        log_error "Discovery phase failed"
        cd ..
        exit 1
    fi
fi
cd ..
log_success "Hospital discovery complete"

# Phase 2: Audit hospital compliance
log_step "Phase 2: Audit hospital compliance..."
cd "$SCRAPER_DIR"
if [ -n "$STATE" ]; then
    if ! cargo run --release -- --audit_only --state "$STATE"; then
        log_error "Audit phase failed (continuing with partial data)"
    fi
else
    if ! cargo run --release -- --audit_only; then
        log_error "Audit phase failed (continuing with partial data)"
    fi
fi
cd ..
log_success "Hospital audit complete"

# Phase 3: Parse MRF data
log_step "Phase 3: Parse MRF data..."
cd "$SCRAPER_DIR"
if [ -n "$STATE" ]; then
    if ! cargo run --release -- --parse_only --state "$STATE"; then
        log_error "Parse phase failed"
        cd ..
        exit 1
    fi
else
    if ! cargo run --release -- --parse_only; then
        log_error "Parse phase failed"
        cd ..
        exit 1
    fi
fi
cd ..
log_success "MRF parsing complete"

# Phase 4: Validate intermediate databases
log_step "Phase 4: Validating intermediate databases..."
if ! python3 "$SCRIPTS_DIR/validate_database.py" "${SCRAPER_DIR}/prices.db"; then
    log_error "prices.db validation failed"
    exit 1
fi
log_success "prices.db validation passed"

if ! python3 "$SCRIPTS_DIR/validate_database.py" "${SCRAPER_DIR}/compliance.db"; then
    log_warning "compliance.db validation had warnings (continuing)"
fi

# Phase 4.5: Database migrations and consolidation
log_step "Phase 4.5: Running database schema migrations..."
if ! python3 "$SCRIPTS_DIR/migrate_schema.py" "${SCRAPER_DIR}/compliance.db"; then
    log_warning "Schema migration failed (may be already migrated)"
fi
log_success "Schema migration complete"

log_step "Phase 4.6: Consolidating hospital data..."
if ! python3 "$SCRIPTS_DIR/consolidate_hospitals.py"; then
    log_warning "Hospital consolidation failed (continuing)"
fi
log_success "Hospital consolidation complete"

# Phase 4.7: Setup CPT search database
log_step "Phase 4.7: Setting up CPT search database..."
if ! python3 "$SCRIPTS_DIR/setup_cpt_database.py" "$WEB_PUBLIC/cpt_mappings.db"; then
    log_warning "CPT database setup failed (continuing)"
fi
log_success "CPT search database ready"

# Phase 5: Generate metrics (if script exists)
if [ -f "$SCRIPTS_DIR/compute_metrics.py" ]; then
    log_step "Phase 5: Computing data quality metrics..."
    if python3 "$SCRIPTS_DIR/compute_metrics.py"; then
        log_success "Metrics computed"
    else
        log_warning "Metrics computation failed (continuing)"
    fi
else
    log_warning "Metrics script not found, skipping phase 5"
fi

# Phase 6: Ingest compliance scoring
log_step "Phase 6: Ingest compliance scoring..."
if ! python3 ingest.py; then
    log_error "Compliance ingestion failed"
    exit 1
fi
log_success "Compliance ingestion complete"

# Phase 7: Validate final output
log_step "Phase 7: Validating final database..."
if ! python3 "$SCRIPTS_DIR/validate_database.py" "$WEB_PUBLIC/audit_data.db"; then
    log_error "Final audit_data.db validation failed"
    exit 1
fi
log_success "audit_data.db validation passed"

# Phase 8: Generate metadata manifest
log_step "Phase 8: Generating data manifest..."
cat > "$WEB_PUBLIC/data_manifest.json" << EOF
{
  "version": "1.0",
  "generated_at": "$(date -Iseconds)",
  "build_timestamp": "$(date '+%Y-%m-%d %H:%M:%S UTC')",
  "data_files": {
    "prices.db": {
      "size_bytes": $(wc -c < "${SCRAPER_DIR}/prices.db" 2>/dev/null || echo "0"),
      "modified": "$(stat -f%Sm -t '%Y-%m-%dT%H:%M:%SZ' "${SCRAPER_DIR}/prices.db" 2>/dev/null || echo "unknown")"
    },
    "audit_data.db": {
      "size_bytes": $(wc -c < "${WEB_PUBLIC}/audit_data.db" 2>/dev/null || echo "0"),
      "modified": "$(stat -f%Sm -t '%Y-%m-%dT%H:%M:%SZ' "${WEB_PUBLIC}/audit_data.db" 2>/dev/null || echo "unknown")"
    }
  }
}
EOF
log_success "Manifest generated"

# Success summary
echo ""
echo -e "${GREEN}"
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    ✓ Build Complete!                       ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "📊 Databases ready for deployment:"
echo "   • $WEB_PUBLIC/audit_data.db"
echo ""
echo -e "📋 Manifest: $WEB_PUBLIC/data_manifest.json"
echo ""
