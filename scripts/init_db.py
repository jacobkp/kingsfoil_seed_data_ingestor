"""
Database initialization script.
Creates all schemas, tables, indexes, and views.

Usage:
    python -m scripts.init_db
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_settings


# ============================================================
# META SCHEMA DDL
# ============================================================

META_SCHEMA_DDL = """
-- ============================================================
-- SCHEMA: meta
-- Purpose: Configuration and metadata for the data pipeline
-- ============================================================

CREATE SCHEMA IF NOT EXISTS meta;

-- ------------------------------------------------------------
-- Table: meta.data_sources
-- Purpose: Defines the high-level datasets (PFS, NCCI, HCPCS)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.data_sources (
    id SERIAL PRIMARY KEY,
    source_code VARCHAR(50) NOT NULL UNIQUE,
    source_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    description TEXT,
    file_formats TEXT[] DEFAULT ARRAY['csv', 'xlsx', 'txt'],
    update_frequency VARCHAR(50),
    source_url TEXT,
    target_table VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    display_order INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Table: meta.canonical_columns
-- Purpose: Defines the superset of all internal columns the
--          analyzer expects, with semantic documentation
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.canonical_columns (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL REFERENCES meta.data_sources(id) ON DELETE CASCADE,
    internal_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    is_nullable BOOLEAN DEFAULT TRUE,
    is_required BOOLEAN DEFAULT TRUE,
    semantic_context TEXT,
    analyzer_usage TEXT,
    introduced_at DATE,
    deprecated_at DATE,
    display_order INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_canonical_col UNIQUE (source_id, internal_name)
);

-- ------------------------------------------------------------
-- Table: meta.column_mappings
-- Purpose: Maps raw file headers to canonical columns
--          Supports multiple header variations per column
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.column_mappings (
    id SERIAL PRIMARY KEY,
    canonical_column_id INT NOT NULL REFERENCES meta.canonical_columns(id) ON DELETE CASCADE,
    source_headers TEXT[] NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_col_mapping UNIQUE (canonical_column_id)
);

-- ------------------------------------------------------------
-- Table: meta.data_versions
-- Purpose: Tracks every file upload and its status
-- For multi-part files (like NCCI PTP), file_name/hash track the first or only file
-- Additional parts are tracked in data_version_parts
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.data_versions (
    id SERIAL PRIMARY KEY,
    source_id INT NOT NULL REFERENCES meta.data_sources(id) ON DELETE CASCADE,
    version_label VARCHAR(50) NOT NULL,
    variant VARCHAR(50),
    effective_date DATE NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    file_size_bytes BIGINT,
    header_row_index INT,
    record_count INT,
    part_count INT DEFAULT 1,
    is_current BOOLEAN DEFAULT FALSE,
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    imported_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_version UNIQUE (source_id, version_label, variant)
);

-- Index for finding current version quickly
CREATE INDEX IF NOT EXISTS idx_data_versions_current
ON meta.data_versions(source_id, is_current)
WHERE is_current = TRUE;

-- ------------------------------------------------------------
-- Table: meta.data_version_parts
-- Purpose: Tracks individual part files for multi-part uploads (e.g., NCCI PTP)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.data_version_parts (
    id SERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    file_size_bytes BIGINT,
    record_count INT,
    imported_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_version_part UNIQUE (data_version_id, part_number),
    CONSTRAINT uq_version_file UNIQUE (data_version_id, file_hash)
);

-- ------------------------------------------------------------
-- Table: meta.ingestion_logs
-- Purpose: Detailed logging for each ingestion attempt
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.ingestion_logs (
    id SERIAL PRIMARY KEY,
    data_version_id INT REFERENCES meta.data_versions(id) ON DELETE CASCADE,
    log_level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_logs_version
ON meta.ingestion_logs(data_version_id);
"""


# ============================================================
# CMS SCHEMA DDL
# ============================================================

CMS_SCHEMA_DDL = """
-- ============================================================
-- SCHEMA: cms
-- Purpose: Actual CMS regulatory data
-- ============================================================

CREATE SCHEMA IF NOT EXISTS cms;

-- ------------------------------------------------------------
-- Table: cms.pfs_rvu
-- Purpose: Physician Fee Schedule - Relative Value Units
-- Source: PPRRVU files (e.g., PPRRVU25_JAN.csv)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.pfs_rvu (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- Core identifiers
    hcpcs_code VARCHAR(10) NOT NULL,
    modifier VARCHAR(5),

    -- Description
    description TEXT,

    -- Status
    status_code VARCHAR(5),

    -- RVU Components
    work_rvu NUMERIC(10,4),
    non_fac_pe_rvu NUMERIC(10,4),
    facility_pe_rvu NUMERIC(10,4),
    mp_rvu NUMERIC(10,4),

    -- Totals
    non_fac_total NUMERIC(10,4),
    facility_total NUMERIC(10,4),

    -- Policy indicators
    pctc_indicator VARCHAR(5),
    global_days VARCHAR(10),

    -- Conversion factor
    conversion_factor NUMERIC(10,4),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_pfs_rvu UNIQUE (data_version_id, hcpcs_code, modifier)
);

CREATE INDEX IF NOT EXISTS idx_pfs_rvu_hcpcs ON cms.pfs_rvu(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_pfs_rvu_version ON cms.pfs_rvu(data_version_id);

-- ------------------------------------------------------------
-- Table: cms.pfs_gpci
-- Purpose: Geographic Practice Cost Indices
-- Source: GPCI files (e.g., GPCI2025.csv)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.pfs_gpci (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- Location identifiers
    mac_locality VARCHAR(10) NOT NULL,
    locality_name VARCHAR(200),

    -- GPCI values
    work_gpci NUMERIC(10,4),
    pe_gpci NUMERIC(10,4),
    mp_gpci NUMERIC(10,4),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_pfs_gpci UNIQUE (data_version_id, mac_locality)
);

CREATE INDEX IF NOT EXISTS idx_pfs_gpci_locality ON cms.pfs_gpci(mac_locality);

-- ------------------------------------------------------------
-- Table: cms.pfs_locality
-- Purpose: ZIP/County to Locality mapping
-- Source: Locality files (e.g., 25LOCCO.csv)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.pfs_locality (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- Geographic identifiers
    state_code VARCHAR(5),
    county_code VARCHAR(10),
    county_name VARCHAR(200),

    -- Locality mapping
    carrier_number VARCHAR(10),
    locality_code VARCHAR(10),
    mac_locality VARCHAR(10),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pfs_locality_state_county
ON cms.pfs_locality(state_code, county_code);
CREATE INDEX IF NOT EXISTS idx_pfs_locality_mac ON cms.pfs_locality(mac_locality);

-- ------------------------------------------------------------
-- Table: cms.pfs_anes_cf
-- Purpose: Anesthesia Conversion Factors by locality
-- Source: ANES files (e.g., ANES2025.csv)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.pfs_anes_cf (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- Location
    mac_locality VARCHAR(10) NOT NULL,
    locality_name VARCHAR(200),

    -- Conversion factor
    anes_conversion_factor NUMERIC(10,4),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_pfs_anes_cf UNIQUE (data_version_id, mac_locality)
);

-- ------------------------------------------------------------
-- Table: cms.pfs_opps_cap
-- Purpose: OPPS imaging cap amounts
-- Source: OPPSCAP files (e.g., OPPSCAP_JAN.csv)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.pfs_opps_cap (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- Code
    hcpcs_code VARCHAR(10) NOT NULL,

    -- Cap amount
    opps_cap_amount NUMERIC(12,4),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_pfs_opps_cap UNIQUE (data_version_id, hcpcs_code)
);

CREATE INDEX IF NOT EXISTS idx_pfs_opps_cap_hcpcs ON cms.pfs_opps_cap(hcpcs_code);

-- ------------------------------------------------------------
-- Table: cms.hcpcs_codes
-- Purpose: HCPCS Level II code descriptions
-- Source: HCPCS files (e.g., HCPC2024_OCT_ANWEB_v2.xlsx)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.hcpcs_codes (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- Code
    hcpcs_code VARCHAR(10) NOT NULL,

    -- Descriptions
    short_description VARCHAR(100),
    long_description TEXT,

    -- Dates
    add_date DATE,
    effective_date DATE,
    termination_date DATE,

    -- Classification
    betos_code VARCHAR(10),
    coverage_code VARCHAR(10),

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_hcpcs_codes UNIQUE (data_version_id, hcpcs_code)
);

CREATE INDEX IF NOT EXISTS idx_hcpcs_code ON cms.hcpcs_codes(hcpcs_code);

-- ------------------------------------------------------------
-- Table: cms.ncci_ptp
-- Purpose: NCCI Procedure-to-Procedure (bundling) edits
-- Source: NCCI PTP files (Hospital and Practitioner versions)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.ncci_ptp (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- The code pair
    comprehensive_code VARCHAR(10) NOT NULL,
    component_code VARCHAR(10) NOT NULL,

    -- Logic
    modifier_indicator SMALLINT,

    -- Validity period
    effective_date DATE NOT NULL,
    deletion_date DATE,

    -- Context
    rationale TEXT,
    prior_1996_flag BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_ncci_ptp UNIQUE (data_version_id, comprehensive_code, component_code)
);

CREATE INDEX IF NOT EXISTS idx_ncci_ptp_lookup
ON cms.ncci_ptp(data_version_id, comprehensive_code, component_code);
CREATE INDEX IF NOT EXISTS idx_ncci_ptp_component
ON cms.ncci_ptp(data_version_id, component_code);
CREATE INDEX IF NOT EXISTS idx_ncci_ptp_comprehensive
ON cms.ncci_ptp(data_version_id, comprehensive_code);

-- ------------------------------------------------------------
-- Table: cms.ncci_mue
-- Purpose: NCCI Medically Unlikely Edits (quantity limits)
-- Source: NCCI MUE files (DME, Facility, Practitioner versions)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cms.ncci_mue (
    id BIGSERIAL PRIMARY KEY,
    data_version_id INT NOT NULL REFERENCES meta.data_versions(id) ON DELETE CASCADE,

    -- Code
    hcpcs_code VARCHAR(10) NOT NULL,

    -- Limits
    mue_value INT,
    mue_rationale VARCHAR(100),

    -- Adjudication
    mai_id SMALLINT,
    mai_description TEXT,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_ncci_mue UNIQUE (data_version_id, hcpcs_code)
);

CREATE INDEX IF NOT EXISTS idx_ncci_mue_hcpcs ON cms.ncci_mue(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_ncci_mue_lookup ON cms.ncci_mue(data_version_id, hcpcs_code);
"""


# ============================================================
# VIEWS DDL
# ============================================================

VIEWS_DDL = """
-- ============================================================
-- VIEWS: Convenience views for "current" data
-- ============================================================

-- Current PFS RVU data
CREATE OR REPLACE VIEW cms.pfs_rvu_current AS
SELECT r.*
FROM cms.pfs_rvu r
JOIN meta.data_versions v ON r.data_version_id = v.id
WHERE v.is_current = TRUE AND v.source_id = (
    SELECT id FROM meta.data_sources WHERE source_code = 'PFS_RVU'
);

-- Current GPCI data
CREATE OR REPLACE VIEW cms.pfs_gpci_current AS
SELECT g.*
FROM cms.pfs_gpci g
JOIN meta.data_versions v ON g.data_version_id = v.id
WHERE v.is_current = TRUE AND v.source_id = (
    SELECT id FROM meta.data_sources WHERE source_code = 'PFS_GPCI'
);

-- Current Locality data
CREATE OR REPLACE VIEW cms.pfs_locality_current AS
SELECT l.*
FROM cms.pfs_locality l
JOIN meta.data_versions v ON l.data_version_id = v.id
WHERE v.is_current = TRUE AND v.source_id = (
    SELECT id FROM meta.data_sources WHERE source_code = 'PFS_LOCALITY'
);

-- Current Anesthesia CF data
CREATE OR REPLACE VIEW cms.pfs_anes_cf_current AS
SELECT a.*
FROM cms.pfs_anes_cf a
JOIN meta.data_versions v ON a.data_version_id = v.id
WHERE v.is_current = TRUE AND v.source_id = (
    SELECT id FROM meta.data_sources WHERE source_code = 'PFS_ANES_CF'
);

-- Current OPPS Cap data
CREATE OR REPLACE VIEW cms.pfs_opps_cap_current AS
SELECT o.*
FROM cms.pfs_opps_cap o
JOIN meta.data_versions v ON o.data_version_id = v.id
WHERE v.is_current = TRUE AND v.source_id = (
    SELECT id FROM meta.data_sources WHERE source_code = 'PFS_OPPS_CAP'
);

-- Current HCPCS codes
CREATE OR REPLACE VIEW cms.hcpcs_codes_current AS
SELECT h.*
FROM cms.hcpcs_codes h
JOIN meta.data_versions v ON h.data_version_id = v.id
WHERE v.is_current = TRUE AND v.source_id = (
    SELECT id FROM meta.data_sources WHERE source_code = 'HCPCS'
);

-- Current NCCI PTP data (Hospital)
CREATE OR REPLACE VIEW cms.ncci_ptp_hospital_current AS
SELECT p.*
FROM cms.ncci_ptp p
JOIN meta.data_versions v ON p.data_version_id = v.id
WHERE v.is_current = TRUE
  AND v.source_id = (SELECT id FROM meta.data_sources WHERE source_code = 'NCCI_PTP')
  AND v.variant = 'HOSPITAL';

-- Current NCCI PTP data (Practitioner)
CREATE OR REPLACE VIEW cms.ncci_ptp_practitioner_current AS
SELECT p.*
FROM cms.ncci_ptp p
JOIN meta.data_versions v ON p.data_version_id = v.id
WHERE v.is_current = TRUE
  AND v.source_id = (SELECT id FROM meta.data_sources WHERE source_code = 'NCCI_PTP')
  AND v.variant = 'PRACTITIONER';

-- Current MUE data (DME)
CREATE OR REPLACE VIEW cms.ncci_mue_dme_current AS
SELECT m.*
FROM cms.ncci_mue m
JOIN meta.data_versions v ON m.data_version_id = v.id
WHERE v.is_current = TRUE
  AND v.source_id = (SELECT id FROM meta.data_sources WHERE source_code = 'NCCI_MUE_DME');

-- Current MUE data (Practitioner)
CREATE OR REPLACE VIEW cms.ncci_mue_practitioner_current AS
SELECT m.*
FROM cms.ncci_mue m
JOIN meta.data_versions v ON m.data_version_id = v.id
WHERE v.is_current = TRUE
  AND v.source_id = (SELECT id FROM meta.data_sources WHERE source_code = 'NCCI_MUE_PRAC');

-- Current MUE data (Outpatient Hospital)
CREATE OR REPLACE VIEW cms.ncci_mue_hospital_current AS
SELECT m.*
FROM cms.ncci_mue m
JOIN meta.data_versions v ON m.data_version_id = v.id
WHERE v.is_current = TRUE
  AND v.source_id = (SELECT id FROM meta.data_sources WHERE source_code = 'NCCI_MUE_OPH');
"""


async def init_database():
    """Initialize database with all schemas, tables, and views."""
    settings = get_settings()

    print("Connecting to database...")
    conn = await asyncpg.connect(settings.database_url)

    try:
        print("\n" + "=" * 60)
        print("Creating META schema and tables...")
        print("=" * 60)
        await conn.execute(META_SCHEMA_DDL)
        print("  - meta.data_sources")
        print("  - meta.canonical_columns")
        print("  - meta.column_mappings")
        print("  - meta.data_versions")
        print("  - meta.ingestion_logs")
        print("META schema complete.")

        print("\n" + "=" * 60)
        print("Creating CMS schema and tables...")
        print("=" * 60)
        await conn.execute(CMS_SCHEMA_DDL)
        print("  - cms.pfs_rvu")
        print("  - cms.pfs_gpci")
        print("  - cms.pfs_locality")
        print("  - cms.pfs_anes_cf")
        print("  - cms.pfs_opps_cap")
        print("  - cms.hcpcs_codes")
        print("  - cms.ncci_ptp")
        print("  - cms.ncci_mue")
        print("CMS schema complete.")

        print("\n" + "=" * 60)
        print("Creating views...")
        print("=" * 60)
        await conn.execute(VIEWS_DDL)
        print("  - cms.pfs_rvu_current")
        print("  - cms.pfs_gpci_current")
        print("  - cms.pfs_locality_current")
        print("  - cms.pfs_anes_cf_current")
        print("  - cms.pfs_opps_cap_current")
        print("  - cms.hcpcs_codes_current")
        print("  - cms.ncci_ptp_hospital_current")
        print("  - cms.ncci_ptp_practitioner_current")
        print("  - cms.ncci_mue_dme_current")
        print("  - cms.ncci_mue_practitioner_current")
        print("  - cms.ncci_mue_hospital_current")
        print("Views complete.")

        print("\n" + "=" * 60)
        print("DATABASE INITIALIZATION COMPLETE")
        print("=" * 60)

        # Verify by counting tables
        table_count = await conn.fetchval("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema IN ('meta', 'cms')
        """)
        print(f"\nTotal tables created: {table_count}")

    finally:
        await conn.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    asyncio.run(init_database())
