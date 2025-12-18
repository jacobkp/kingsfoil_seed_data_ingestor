# Kingsfoil CMS Data Schema Reference

This document describes the database schema for CMS regulatory data managed by the Kingsfoil Data Pipeline. Use this as a reference when querying data from the Kingsfoil Analyser or building integrations.

---

## Database Structure

The data is organized into two PostgreSQL schemas:

| Schema | Purpose |
|--------|---------|
| `meta` | Configuration, version tracking, column definitions |
| `cms` | Actual CMS regulatory data (versioned) |

Each CMS table has a corresponding `*_current` view that returns only rows from the active version.

---

## Data Sources Overview

| Source Code | Name | Table | Update Frequency |
|-------------|------|-------|------------------|
| `PFS_RVU` | Physician Fee Schedule - RVUs | `cms.pfs_rvu` | Quarterly |
| `PFS_GPCI` | Geographic Practice Cost Index | `cms.pfs_gpci` | Annual |
| `PFS_LOCALITY` | Locality Mapping | `cms.pfs_locality` | Annual |
| `PFS_ANES_CF` | Anesthesia Conversion Factor | `cms.pfs_anes_cf` | Annual |
| `PFS_OPPS_CAP` | OPPS Imaging Cap | `cms.pfs_opps_cap` | Quarterly |
| `HCPCS` | HCPCS Level II Codes | `cms.hcpcs_codes` | Quarterly |
| `NCCI_PTP` | PTP Bundling Edits | `cms.ncci_ptp` | Quarterly |
| `NCCI_MUE_DME` | MUE - DME Supplier | `cms.ncci_mue` | Quarterly |
| `NCCI_MUE_PRAC` | MUE - Practitioner | `cms.ncci_mue` | Quarterly |
| `NCCI_MUE_OPH` | MUE - Outpatient Hospital | `cms.ncci_mue` | Quarterly |

---

## Physician Fee Schedule (PFS)

### PFS RVU (`cms.pfs_rvu`)

Base Relative Value Units for Medicare fee calculation.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `hcpcs_code` | TEXT | Yes | 5-character HCPCS/CPT code |
| `modifier` | TEXT | No | 2-character modifier (26, TC, etc.) |
| `description` | TEXT | No | CMS service description |
| `status_code` | TEXT | No | Payment status: A=Active, B=Bundled, C=Carrier-priced, D=Deleted, E=Excluded |
| `work_rvu` | NUMERIC | No | Physician work component |
| `non_fac_pe_rvu` | NUMERIC | No | Practice expense (non-facility setting) |
| `facility_pe_rvu` | NUMERIC | No | Practice expense (facility setting) |
| `mp_rvu` | NUMERIC | No | Malpractice component |
| `non_fac_total` | NUMERIC | No | Pre-calculated non-facility total RVU |
| `facility_total` | NUMERIC | No | Pre-calculated facility total RVU |
| `pctc_indicator` | TEXT | No | PC/TC indicator: 0=No split, 1=Has PC/TC, 2=PC only, 3=TC only |
| `global_days` | TEXT | No | Global period: 000, 010, 090, XXX, YYY, ZZZ |
| `conversion_factor` | NUMERIC | No | Dollar multiplier for RVU |

**Unique Key:** `hcpcs_code` + `modifier`

**Fee Formula:**
```
Fee = ((Work RVU × Work GPCI) + (PE RVU × PE GPCI) + (MP RVU × MP GPCI)) × CF
```

---

### PFS GPCI (`cms.pfs_gpci`)

Geographic adjustment factors by locality.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `mac_locality` | TEXT | Yes | Combined MAC number + locality code |
| `locality_name` | TEXT | No | Human-readable locality name |
| `work_gpci` | NUMERIC | Yes | Work RVU multiplier (typically 0.8-1.1) |
| `pe_gpci` | NUMERIC | Yes | Practice expense multiplier |
| `mp_gpci` | NUMERIC | Yes | Malpractice multiplier |

**Unique Key:** `mac_locality`

---

### PFS Locality Mapping (`cms.pfs_locality`)

Maps counties to MAC localities for GPCI lookup.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `state_code` | TEXT | Yes | Two-letter state abbreviation |
| `county_code` | TEXT | No | FIPS county code |
| `county_name` | TEXT | No | County name |
| `carrier_number` | TEXT | Yes | MAC/Carrier number |
| `locality_code` | TEXT | Yes | Two-digit locality code |
| `mac_locality` | TEXT | No | Derived: carrier_number + locality_code |

**Unique Key:** `state_code` + `county_code` + `carrier_number` + `locality_code`

---

### PFS Anesthesia CF (`cms.pfs_anes_cf`)

Locality-specific conversion factors for anesthesia services.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `mac_locality` | TEXT | Yes | MAC locality identifier |
| `locality_name` | TEXT | No | Locality name |
| `anes_conversion_factor` | NUMERIC | Yes | Anesthesia-specific CF |

**Unique Key:** `mac_locality`

**Anesthesia Fee Formula:**
```
Fee = (Base Units + Time Units) × Anesthesia CF
```

---

### PFS OPPS Cap (`cms.pfs_opps_cap`)

Maximum amounts for imaging technical component.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `hcpcs_code` | TEXT | Yes | Imaging code subject to cap |
| `opps_cap_amount` | NUMERIC | Yes | Maximum allowed amount for TC |

**Unique Key:** `hcpcs_code`

**Usage:**
```
Final Fee = MIN(Calculated Fee, OPPS Cap)
```

---

## HCPCS Codes (`cms.hcpcs_codes`)

HCPCS Level II code descriptions and metadata.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `hcpcs_code` | TEXT | Yes | HCPCS Level II code (A0000-V9999) |
| `short_description` | TEXT | No | 28-character abbreviated description |
| `long_description` | TEXT | No | Full description |
| `add_date` | DATE | No | Date code was added |
| `effective_date` | DATE | No | Current definition effective date |
| `termination_date` | DATE | No | Date code was retired (NULL if active) |
| `betos_code` | TEXT | No | Type of service classification |
| `coverage_code` | TEXT | No | Medicare coverage indicator |

**Unique Key:** `hcpcs_code`

---

## NCCI Edits

### NCCI PTP Edits (`cms.ncci_ptp`)

Procedure-to-Procedure bundling edits. Available in Hospital and Practitioner variants.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `comprehensive_code` | TEXT | Yes | Parent/major procedure code |
| `component_code` | TEXT | Yes | Child/minor code (bundled into parent) |
| `modifier_indicator` | INTEGER | Yes | 0=Never bill together, 1=Modifier allowed, 9=N/A |
| `effective_date` | DATE | Yes | Edit start date |
| `deletion_date` | DATE | No | Edit end date (NULL = active) |
| `rationale` | TEXT | No | CMS explanation for bundling |
| `prior_1996_flag` | BOOLEAN | No | Edit existed before NCCI program |

**Unique Key:** `comprehensive_code` + `component_code` (within variant)

**Modifier Indicator Values:**
- `0` = Cannot bill together under any circumstance
- `1` = Can bill together with modifier 59 or X{EPSU}
- `9` = Not applicable

**Variants:**
- `HOSPITAL` - Applies to facility claims
- `PRACTITIONER` - Applies to professional claims

---

### NCCI MUE Edits (`cms.ncci_mue`)

Medically Unlikely Edits - maximum units per code per day.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `hcpcs_code` | TEXT | Yes | CPT/HCPCS code |
| `mue_value` | INTEGER | Yes | Maximum units per day (0 = not payable) |
| `mai_id` | INTEGER | No | MUE Adjudication Indicator (1, 2, or 3) |
| `mai_description` | TEXT | Yes | Full MAI description |
| `mue_rationale` | TEXT | No | Reason for the limit |

**Unique Key:** `hcpcs_code` (within source type)

**MAI Values:**
- `1` = Line Edit (check each line individually)
- `2` = Date of Service Edit - Policy (sum lines, hard deny)
- `3` = Date of Service Edit - Clinical (sum lines, appealable)

**Source Types:**
- `NCCI_MUE_DME` - DME Supplier limits
- `NCCI_MUE_PRAC` - Practitioner limits
- `NCCI_MUE_OPH` - Outpatient Hospital limits

---

## Version Tracking

All CMS data is versioned. Each upload creates a new version record.

### Querying Current Data

Use the `*_current` views to get only active version data:

```sql
-- Get current RVU for a code
SELECT * FROM cms.pfs_rvu_current WHERE hcpcs_code = '99213';

-- Get current NCCI PTP edits for Hospital
SELECT * FROM cms.ncci_ptp_hospital_current
WHERE comprehensive_code = '99213';

-- Get current MUE for Practitioner
SELECT * FROM cms.ncci_mue_prac_current WHERE hcpcs_code = '99213';
```

### Version Metadata

```sql
-- List all versions for a source
SELECT version_label, variant, status, record_count, imported_at, is_current
FROM meta.data_versions
WHERE source_id = (SELECT id FROM meta.data_sources WHERE source_code = 'PFS_RVU')
ORDER BY imported_at DESC;
```

---

## Common Queries

### Medicare Fee Calculation

```sql
WITH rvu AS (
    SELECT * FROM cms.pfs_rvu_current WHERE hcpcs_code = '99213' AND modifier IS NULL
),
gpci AS (
    SELECT * FROM cms.pfs_gpci_current WHERE mac_locality = '05102'
)
SELECT
    rvu.hcpcs_code,
    rvu.description,
    ROUND(
        ((rvu.work_rvu * gpci.work_gpci) +
         (rvu.non_fac_pe_rvu * gpci.pe_gpci) +
         (rvu.mp_rvu * gpci.mp_gpci)) * rvu.conversion_factor,
        2
    ) AS non_facility_fee
FROM rvu, gpci;
```

### Check PTP Bundling

```sql
SELECT
    comprehensive_code,
    component_code,
    modifier_indicator,
    rationale
FROM cms.ncci_ptp_practitioner_current
WHERE comprehensive_code = '99213'
  AND deletion_date IS NULL;
```

### Check MUE Limits

```sql
SELECT
    hcpcs_code,
    mue_value,
    mai_id,
    mue_rationale
FROM cms.ncci_mue_prac_current
WHERE hcpcs_code = '99213';
```

---

## File Format Notes

### Expected File Headers

The pipeline automatically detects headers. Common variations are supported:

| Internal Column | Accepted Headers |
|-----------------|------------------|
| `hcpcs_code` | HCPCS, HCPC, CPT, HCPCS CODE, PROCEDURE CODE |
| `modifier` | MOD, MODIFIER |
| `work_rvu` | WORK RVU, WORK_RVU, WRVU |
| `comprehensive_code` | Column 1, COLUMN 1, COMPREHENSIVE CODE |
| `component_code` | Column 2, COLUMN 2, COMPONENT CODE |
| `mue_value` | DME Supplier Services MUE Values, Practitioner Services MUE Values, etc. |

### Special Value Handling

- NCCI PTP `deletion_date`: `*` in source file = NULL (active edit)
- NCCI PTP `prior_1996_flag`: `*` in source file = TRUE
- NCCI MUE `mue_value`: `0` is a valid limit (not payable)
- NCCI MUE `mai_id`: Extracted from `mai_description` (e.g., "3 - Date of Service" → 3)

---

## Related Documentation

- [CMS Physician Fee Schedule](https://www.cms.gov/medicare/payment/fee-schedules/physician)
- [NCCI Edits](https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-edits)
- [HCPCS Codes](https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system)
