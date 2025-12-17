"""
Seed data script.
Populates meta.data_sources, meta.canonical_columns, and meta.column_mappings.

Usage:
    python -m scripts.seed_data
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_settings


# ============================================================
# DATA SOURCES
# ============================================================

DATA_SOURCES = [
    # Physician Fee Schedule files
    {
        "source_code": "PFS_RVU",
        "source_name": "PFS - Relative Value Units",
        "category": "Physician Fee Schedule",
        "description": "Base RVU values, status codes, and policy indicators for HCPCS codes. Primary file for Medicare fee calculation.",
        "target_table": "cms.pfs_rvu",
        "update_frequency": "QUARTERLY",
        "display_order": 10,
    },
    {
        "source_code": "PFS_GPCI",
        "source_name": "PFS - Geographic Practice Cost Index",
        "category": "Physician Fee Schedule",
        "description": "Geographic adjustment factors (Work, PE, MP) by locality. Multiplied against RVUs for location-specific pricing.",
        "target_table": "cms.pfs_gpci",
        "update_frequency": "ANNUAL",
        "display_order": 20,
    },
    {
        "source_code": "PFS_LOCALITY",
        "source_name": "PFS - Locality Mapping",
        "category": "Physician Fee Schedule",
        "description": "Maps counties/states to MAC localities. Used to determine which GPCI values apply based on provider address.",
        "target_table": "cms.pfs_locality",
        "update_frequency": "ANNUAL",
        "display_order": 30,
    },
    {
        "source_code": "PFS_ANES_CF",
        "source_name": "PFS - Anesthesia Conversion Factor",
        "category": "Physician Fee Schedule",
        "description": "Locality-specific conversion factors for anesthesia services. Different from standard CF.",
        "target_table": "cms.pfs_anes_cf",
        "update_frequency": "ANNUAL",
        "display_order": 40,
    },
    {
        "source_code": "PFS_OPPS_CAP",
        "source_name": "PFS - OPPS Imaging Cap",
        "category": "Physician Fee Schedule",
        "description": "Cap amounts for imaging technical component. Fee = MIN(calculated_fee, opps_cap).",
        "target_table": "cms.pfs_opps_cap",
        "update_frequency": "QUARTERLY",
        "display_order": 50,
    },
    # HCPCS
    {
        "source_code": "HCPCS",
        "source_name": "HCPCS Level II Codes",
        "category": "HCPCS",
        "description": "Healthcare Common Procedure Coding System Level II codes. Non-CPT codes for supplies, DME, drugs, etc.",
        "target_table": "cms.hcpcs_codes",
        "update_frequency": "QUARTERLY",
        "display_order": 60,
    },
    # NCCI Edits
    {
        "source_code": "NCCI_PTP",
        "source_name": "NCCI PTP Edits",
        "category": "NCCI Edits",
        "description": "Procedure-to-Procedure bundling edits. Defines code pairs that should not be billed together. Has Hospital and Practitioner variants.",
        "target_table": "cms.ncci_ptp",
        "update_frequency": "QUARTERLY",
        "display_order": 70,
    },
    # NCCI MUE Sources
    {
        "source_code": "NCCI_MUE_DME",
        "source_name": "NCCI MUE - DME Supplier",
        "category": "NCCI Edits",
        "description": "Medically Unlikely Edits for Durable Medical Equipment suppliers. Defines maximum units per code per day.",
        "target_table": "cms.ncci_mue",
        "update_frequency": "QUARTERLY",
        "display_order": 81,
    },
    {
        "source_code": "NCCI_MUE_PRAC",
        "source_name": "NCCI MUE - Practitioner",
        "category": "NCCI Edits",
        "description": "Medically Unlikely Edits for Physician/Practitioner services. Defines maximum units per code per day.",
        "target_table": "cms.ncci_mue",
        "update_frequency": "QUARTERLY",
        "display_order": 82,
    },
    {
        "source_code": "NCCI_MUE_OPH",
        "source_name": "NCCI MUE - Outpatient Hospital",
        "category": "NCCI Edits",
        "description": "Medically Unlikely Edits for Outpatient Hospital services. Defines maximum units per code per day.",
        "target_table": "cms.ncci_mue",
        "update_frequency": "QUARTERLY",
        "display_order": 83,
    },
]


# ============================================================
# CANONICAL COLUMNS
# ============================================================

# Format: (source_code, internal_name, display_name, data_type, is_nullable, is_required, semantic_context, analyzer_usage, display_order)

CANONICAL_COLUMNS = [
    # --------------------------------------------------------
    # PFS_RVU columns
    # --------------------------------------------------------
    ("PFS_RVU", "hcpcs_code", "HCPCS Code", "TEXT", False, True,
     "The 5-character HCPCS/CPT code identifying the medical service or procedure.",
     "Primary join key. Matches against line items on medical bills. Used for all fee lookups.",
     10),
    ("PFS_RVU", "modifier", "Modifier", "TEXT", True, False,
     "Optional 2-character modifier affecting how the code is priced (e.g., 26 for professional component, TC for technical component).",
     "When present, lookup must match both HCPCS + modifier combination. Affects RVU selection.",
     20),
    ("PFS_RVU", "description", "Description", "TEXT", True, False,
     "CMS-provided text description of the service.",
     "Displayed in analysis results. Used for human verification of code matching.",
     30),
    ("PFS_RVU", "status_code", "Status Code", "TEXT", True, False,
     "Indicates payment status: A=Active, B=Bundled, C=Carrier-priced, D=Deleted, E=Excluded, etc.",
     "Critical for validation. Status B/C/D/E codes require special handling in fee calculation.",
     40),
    ("PFS_RVU", "work_rvu", "Work RVU", "NUMERIC", True, False,
     "Relative Value Unit for physician work - the cognitive and physical effort required.",
     "Component of fee formula: (Work RVU × Work GPCI). Zero for technical-only services.",
     50),
    ("PFS_RVU", "non_fac_pe_rvu", "Non-Facility PE RVU", "NUMERIC", True, False,
     "Practice Expense RVU for services performed in non-facility settings (e.g., physician office).",
     "Used when place of service is 11 (Office). Higher than facility PE because includes overhead.",
     60),
    ("PFS_RVU", "facility_pe_rvu", "Facility PE RVU", "NUMERIC", True, False,
     "Practice Expense RVU for services performed in facility settings (e.g., hospital, ASC).",
     "Used when place of service is 21/22/24 (Hospital/ASC). Lower because facility bears overhead.",
     70),
    ("PFS_RVU", "mp_rvu", "Malpractice RVU", "NUMERIC", True, False,
     "Relative Value Unit for professional liability insurance costs.",
     "Component of fee formula: (MP RVU × MP GPCI). Varies by specialty risk.",
     80),
    ("PFS_RVU", "non_fac_total", "Non-Facility Total", "NUMERIC", True, False,
     "Pre-calculated total RVU for non-facility setting.",
     "Can be used for quick lookups but analyzer recalculates for accuracy.",
     90),
    ("PFS_RVU", "facility_total", "Facility Total", "NUMERIC", True, False,
     "Pre-calculated total RVU for facility setting.",
     "Can be used for quick lookups but analyzer recalculates for accuracy.",
     100),
    ("PFS_RVU", "pctc_indicator", "PC/TC Indicator", "TEXT", True, False,
     "Professional/Technical Component indicator: 0=No split, 1=Has PC/TC, 2=PC only, 3=TC only.",
     "Determines if code can be billed with modifier 26 (PC) or TC. Affects imaging analysis.",
     110),
    ("PFS_RVU", "global_days", "Global Period", "TEXT", True, False,
     "Post-operative period: 000=E&M only, 010=10-day, 090=90-day, XXX=not applicable, YYY/ZZZ=special.",
     "Detects unbundling errors for follow-up visits within global period.",
     120),
    ("PFS_RVU", "conversion_factor", "Conversion Factor", "NUMERIC", True, False,
     "Dollar multiplier applied to total RVU. Standard CF or service-specific CF.",
     "Final step in fee formula: Total RVU × CF = Medicare allowed amount.",
     130),

    # --------------------------------------------------------
    # PFS_GPCI columns
    # --------------------------------------------------------
    ("PFS_GPCI", "mac_locality", "MAC Locality", "TEXT", False, True,
     "Combined MAC (Medicare Administrative Contractor) number and locality code. Unique geographic identifier.",
     "Primary key for GPCI lookup. Derived from provider address via locality mapping.",
     10),
    ("PFS_GPCI", "locality_name", "Locality Name", "TEXT", True, False,
     "Human-readable name of the locality (e.g., 'Manhattan, NY', 'Rest of California').",
     "Displayed in analysis results for geographic context.",
     20),
    ("PFS_GPCI", "work_gpci", "Work GPCI", "NUMERIC", False, True,
     "Geographic adjustment for Work RVU. Reflects local wage differences.",
     "Multiplied against Work RVU in fee calculation. Range typically 0.8 to 1.1.",
     30),
    ("PFS_GPCI", "pe_gpci", "PE GPCI", "NUMERIC", False, True,
     "Geographic adjustment for Practice Expense RVU. Reflects local rent, staff costs.",
     "Multiplied against PE RVU in fee calculation. Highest variation by geography.",
     40),
    ("PFS_GPCI", "mp_gpci", "MP GPCI", "NUMERIC", False, True,
     "Geographic adjustment for Malpractice RVU. Reflects local malpractice insurance costs.",
     "Multiplied against MP RVU in fee calculation.",
     50),

    # --------------------------------------------------------
    # PFS_LOCALITY columns
    # --------------------------------------------------------
    ("PFS_LOCALITY", "state_code", "State Code", "TEXT", False, True,
     "Two-letter state abbreviation.",
     "Used with county to lookup locality. Provider address parsing extracts this.",
     10),
    ("PFS_LOCALITY", "county_code", "County Code", "TEXT", True, False,
     "FIPS county code within the state.",
     "Alternative lookup method when county name is ambiguous.",
     20),
    ("PFS_LOCALITY", "county_name", "County Name", "TEXT", True, False,
     "County name as listed by CMS.",
     "Primary lookup field. Fuzzy matching may be needed for variations.",
     30),
    ("PFS_LOCALITY", "carrier_number", "Carrier Number", "TEXT", False, True,
     "MAC/Carrier number (legacy term) for this geographic area.",
     "Combined with locality_code to form mac_locality key.",
     40),
    ("PFS_LOCALITY", "locality_code", "Locality Code", "TEXT", False, True,
     "Two-digit locality code within the carrier region.",
     "Combined with carrier_number to form mac_locality key.",
     50),
    ("PFS_LOCALITY", "mac_locality", "MAC Locality", "TEXT", True, False,
     "Derived field: carrier_number + locality_code. May be pre-populated in some files.",
     "Join key to pfs_gpci table. Calculate if not present: carrier_number || locality_code.",
     60),

    # --------------------------------------------------------
    # PFS_ANES_CF columns
    # --------------------------------------------------------
    ("PFS_ANES_CF", "mac_locality", "MAC Locality", "TEXT", False, True,
     "Combined MAC number and locality code. Same as GPCI locality.",
     "Join key to match anesthesia services to correct conversion factor.",
     10),
    ("PFS_ANES_CF", "locality_name", "Locality Name", "TEXT", True, False,
     "Human-readable locality name.",
     "Display purposes.",
     20),
    ("PFS_ANES_CF", "anes_conversion_factor", "Anesthesia CF", "NUMERIC", False, True,
     "Conversion factor specific to anesthesia services. Different from standard PFS CF.",
     "Used instead of standard CF for codes 00100-01999. Formula: Base Units × Time Units × Anes CF.",
     30),

    # --------------------------------------------------------
    # PFS_OPPS_CAP columns
    # --------------------------------------------------------
    ("PFS_OPPS_CAP", "hcpcs_code", "HCPCS Code", "TEXT", False, True,
     "Imaging code subject to the OPPS cap.",
     "Join key to match against billed imaging codes.",
     10),
    ("PFS_OPPS_CAP", "opps_cap_amount", "OPPS Cap Amount", "NUMERIC", False, True,
     "Maximum allowed amount for the technical component of imaging services.",
     "Fee = MIN(calculated_fee, opps_cap_amount) for TC of applicable imaging codes.",
     20),

    # --------------------------------------------------------
    # HCPCS columns
    # --------------------------------------------------------
    ("HCPCS", "hcpcs_code", "HCPCS Code", "TEXT", False, True,
     "The HCPCS Level II code (A0000-V9999 range).",
     "Primary key for code lookup. Validates codes on bills are real HCPCS codes.",
     10),
    ("HCPCS", "short_description", "Short Description", "TEXT", True, False,
     "Abbreviated description (28 characters max).",
     "Used for compact displays and reports.",
     20),
    ("HCPCS", "long_description", "Long Description", "TEXT", True, False,
     "Full description of the service, supply, or equipment.",
     "Used for detailed analysis output and human review.",
     30),
    ("HCPCS", "add_date", "Add Date", "DATE", True, False,
     "Date the code was added to HCPCS.",
     "Historical context. Codes used before add_date are invalid.",
     40),
    ("HCPCS", "effective_date", "Effective Date", "DATE", True, False,
     "Date the current code definition became effective.",
     "May differ from add_date if code was redefined.",
     50),
    ("HCPCS", "termination_date", "Termination Date", "DATE", True, False,
     "Date the code was terminated. NULL if active.",
     "Codes used after termination_date are invalid.",
     60),
    ("HCPCS", "betos_code", "BETOS Code", "TEXT", True, False,
     "Berenson-Eggers Type of Service classification.",
     "Categorizes services for analysis (E&M, procedures, imaging, etc.).",
     70),
    ("HCPCS", "coverage_code", "Coverage Code", "TEXT", True, False,
     "Medicare coverage status indicator.",
     "Identifies non-covered services.",
     80),

    # --------------------------------------------------------
    # NCCI_PTP columns
    # --------------------------------------------------------
    ("NCCI_PTP", "comprehensive_code", "Comprehensive Code", "TEXT", False, True,
     "The 'parent' or major procedure code. If this code is billed, the component code is bundled into it.",
     "First code in the pair. If present on claim, check for component_code on same claim.",
     10),
    ("NCCI_PTP", "component_code", "Component Code", "TEXT", False, True,
     "The 'child' or minor procedure code that gets denied when billed with the comprehensive code.",
     "Second code in the pair. This is the code that should be denied or flagged.",
     20),
    ("NCCI_PTP", "modifier_indicator", "Modifier Indicator", "INTEGER", False, True,
     "0 = Hard deny (cannot bill together under any circumstance). 1 = Modifier allowed (can bill together with modifier 59/X{EPSU}). 9 = Not applicable.",
     "Critical logic gate. If 0, always flag. If 1, only flag if no appropriate modifier present.",
     30),
    ("NCCI_PTP", "effective_date", "Effective Date", "DATE", False, True,
     "Date this bundling edit became active. Format in source file: YYYYMMDD.",
     "Compare against claim date of service. Edit only applies if DOS >= effective_date.",
     40),
    ("NCCI_PTP", "deletion_date", "Deletion Date", "DATE", True, False,
     "Date this bundling edit was removed. NULL (shown as * in source) means currently active.",
     "Edit does not apply if claim DOS > deletion_date. NULL = still active.",
     50),
    ("NCCI_PTP", "rationale", "PTP Edit Rationale", "TEXT", True, False,
     "CMS-provided explanation for why these codes are bundled.",
     "Included in error messages to explain why codes cannot be billed together.",
     60),
    ("NCCI_PTP", "prior_1996_flag", "Prior to 1996 Flag", "BOOLEAN", True, False,
     "Indicates if this edit existed before NCCI was formally established in 1996.",
     "Historical context only. Does not affect current analysis.",
     70),

    # --------------------------------------------------------
    # NCCI_MUE_DME columns
    # --------------------------------------------------------
    ("NCCI_MUE_DME", "hcpcs_code", "HCPCS Code", "TEXT", False, True,
     "The CPT/HCPCS code subject to the quantity limit.",
     "Primary lookup key. Match against billed codes to check unit limits.",
     10),
    ("NCCI_MUE_DME", "mue_value", "MUE Value", "INTEGER", False, True,
     "Maximum units of service allowed per beneficiary per day. Zero means not payable for this provider type.",
     "Compare against billed units. If billed_units > mue_value, flag for potential overbilling. Zero is a valid limit.",
     20),
    ("NCCI_MUE_DME", "mai_id", "MAI ID", "INTEGER", True, False,
     "MUE Adjudication Indicator (1, 2, or 3). Extracted from the full MAI description.",
     "1=Line Edit (check each line). 2=DOS Edit Policy (sum lines, hard deny). 3=DOS Edit Clinical (sum lines, appealable).",
     30),
    ("NCCI_MUE_DME", "mai_description", "MAI Description", "TEXT", True, True,
     "Full MUE Adjudication Indicator description (e.g., '3 Date of Service Edit: Clinical').",
     "Provides context for denial explanations and appeals guidance.",
     40),
    ("NCCI_MUE_DME", "mue_rationale", "MUE Rationale", "TEXT", True, False,
     "Reason for the unit limit (e.g., 'Nature of Equipment', 'CMS Policy', 'Anatomic Consideration').",
     "Displayed in denial explanations to justify the limit.",
     50),

    # --------------------------------------------------------
    # NCCI_MUE_PRAC columns
    # --------------------------------------------------------
    ("NCCI_MUE_PRAC", "hcpcs_code", "HCPCS Code", "TEXT", False, True,
     "The CPT/HCPCS code subject to the quantity limit.",
     "Primary lookup key. Match against billed codes to check unit limits.",
     10),
    ("NCCI_MUE_PRAC", "mue_value", "MUE Value", "INTEGER", False, True,
     "Maximum units of service allowed per beneficiary per day.",
     "Compare against billed units. If billed_units > mue_value, flag for potential overbilling.",
     20),
    ("NCCI_MUE_PRAC", "mai_id", "MAI ID", "INTEGER", True, False,
     "MUE Adjudication Indicator (1, 2, or 3).",
     "1=Line Edit, 2=DOS Edit Policy, 3=DOS Edit Clinical.",
     30),
    ("NCCI_MUE_PRAC", "mai_description", "MAI Description", "TEXT", True, True,
     "Full MUE Adjudication Indicator description.",
     "Context for denial explanations.",
     40),
    ("NCCI_MUE_PRAC", "mue_rationale", "MUE Rationale", "TEXT", True, False,
     "Reason for the unit limit.",
     "Displayed in denial explanations.",
     50),

    # --------------------------------------------------------
    # NCCI_MUE_OPH columns
    # --------------------------------------------------------
    ("NCCI_MUE_OPH", "hcpcs_code", "HCPCS Code", "TEXT", False, True,
     "The CPT/HCPCS code subject to the quantity limit.",
     "Primary lookup key. Match against billed codes to check unit limits.",
     10),
    ("NCCI_MUE_OPH", "mue_value", "MUE Value", "INTEGER", False, True,
     "Maximum units of service allowed per beneficiary per day.",
     "Compare against billed units. If billed_units > mue_value, flag for potential overbilling.",
     20),
    ("NCCI_MUE_OPH", "mai_id", "MAI ID", "INTEGER", True, False,
     "MUE Adjudication Indicator (1, 2, or 3).",
     "1=Line Edit, 2=DOS Edit Policy, 3=DOS Edit Clinical.",
     30),
    ("NCCI_MUE_OPH", "mai_description", "MAI Description", "TEXT", True, True,
     "Full MUE Adjudication Indicator description.",
     "Context for denial explanations.",
     40),
    ("NCCI_MUE_OPH", "mue_rationale", "MUE Rationale", "TEXT", True, False,
     "Reason for the unit limit.",
     "Displayed in denial explanations.",
     50),
]


# ============================================================
# COLUMN MAPPINGS
# ============================================================

# Format: (source_code, internal_name, source_headers[], notes)

COLUMN_MAPPINGS = [
    # --------------------------------------------------------
    # PFS_RVU mappings
    # --------------------------------------------------------
    ("PFS_RVU", "hcpcs_code", ["HCPCS", "HCPC", "CPT", "HCPCS CODE", "PROCEDURE CODE"], "Standard HCPCS column headers"),
    ("PFS_RVU", "modifier", ["MOD", "MODIFIER", "MOD."], "Modifier column"),
    ("PFS_RVU", "description", ["DESCRIPTION", "DESC", "DESCRIPTOR", "SHORT DESCRIPTION"], "Description field"),
    ("PFS_RVU", "status_code", ["STATUS CODE", "STATUS", "STAT", "STS"], "Status indicator"),
    ("PFS_RVU", "work_rvu", ["WORK RVU", "WORK_RVU", "WRVU", "PHYSICIAN WORK"], "Work RVU"),
    ("PFS_RVU", "non_fac_pe_rvu", ["NON-FAC PE RVU", "NON-FACILITY PE RVU", "NFPE RVU", "NON FAC PE RVU", "FULLY IMPL NON-FAC PE RVUS"], "Non-facility practice expense"),
    ("PFS_RVU", "facility_pe_rvu", ["FAC PE RVU", "FACILITY PE RVU", "FPE RVU", "FAC_PE_RVU", "FULLY IMPL FAC PE RVUS"], "Facility practice expense"),
    ("PFS_RVU", "mp_rvu", ["MP RVU", "MALPRACTICE RVU", "MAL PRAC RVU", "MPRVU", "MALPRACTICE"], "Malpractice RVU"),
    ("PFS_RVU", "non_fac_total", ["NON-FAC TOTAL", "NON-FACILITY TOTAL", "NF TOTAL"], "Pre-calculated non-facility total"),
    ("PFS_RVU", "facility_total", ["FAC TOTAL", "FACILITY TOTAL", "FAC_TOTAL"], "Pre-calculated facility total"),
    ("PFS_RVU", "pctc_indicator", ["PCTC IND", "PC/TC IND", "PCTC INDICATOR", "PC/TC INDICATOR"], "Professional/Technical component indicator"),
    ("PFS_RVU", "global_days", ["GLOB DAYS", "GLOBAL DAYS", "GLOBAL PERIOD", "GLOB"], "Global surgical period"),
    ("PFS_RVU", "conversion_factor", ["CONV FACTOR", "CF", "CONVERSION FACTOR", "GPCI CF"], "Conversion factor"),

    # --------------------------------------------------------
    # PFS_GPCI mappings
    # --------------------------------------------------------
    ("PFS_GPCI", "mac_locality", ["MAC LOCALITY", "LOCALITY", "CARRIER LOCALITY", "MAC/LOCALITY"], "Locality identifier"),
    ("PFS_GPCI", "locality_name", ["LOCALITY NAME", "NAME", "LOCALITY DESCRIPTION"], "Locality name"),
    ("PFS_GPCI", "work_gpci", ["WORK GPCI", "PW GPCI", "WORK", "PHYSICIAN WORK GPCI"], "Work GPCI"),
    ("PFS_GPCI", "pe_gpci", ["PE GPCI", "PRACTICE EXPENSE GPCI", "PE", "PRACTICE EXPENSE"], "Practice Expense GPCI"),
    ("PFS_GPCI", "mp_gpci", ["MP GPCI", "MALPRACTICE GPCI", "MP", "PLI GPCI"], "Malpractice GPCI"),

    # --------------------------------------------------------
    # PFS_LOCALITY mappings
    # --------------------------------------------------------
    ("PFS_LOCALITY", "state_code", ["STATE", "STATE CODE", "ST"], "State code"),
    ("PFS_LOCALITY", "county_code", ["COUNTY CODE", "FIPS", "FIPS CODE"], "County FIPS code"),
    ("PFS_LOCALITY", "county_name", ["COUNTY", "COUNTY NAME"], "County name"),
    ("PFS_LOCALITY", "carrier_number", ["CARRIER", "CARRIER NUMBER", "MAC", "MAC NUMBER"], "Carrier/MAC number"),
    ("PFS_LOCALITY", "locality_code", ["LOCALITY", "LOCALITY CODE", "LOC"], "Locality code"),
    ("PFS_LOCALITY", "mac_locality", ["MAC LOCALITY", "CARRIER LOCALITY"], "Combined MAC locality"),

    # --------------------------------------------------------
    # PFS_ANES_CF mappings
    # --------------------------------------------------------
    ("PFS_ANES_CF", "mac_locality", ["MAC LOCALITY", "LOCALITY", "CARRIER LOCALITY"], "Locality identifier"),
    ("PFS_ANES_CF", "locality_name", ["LOCALITY NAME", "NAME"], "Locality name"),
    ("PFS_ANES_CF", "anes_conversion_factor", ["ANESTHESIA CF", "ANES CF", "CONVERSION FACTOR", "CF"], "Anesthesia conversion factor"),

    # --------------------------------------------------------
    # PFS_OPPS_CAP mappings
    # --------------------------------------------------------
    ("PFS_OPPS_CAP", "hcpcs_code", ["HCPCS", "HCPC", "HCPCS CODE", "CODE"], "HCPCS code"),
    ("PFS_OPPS_CAP", "opps_cap_amount", ["OPPS CAP", "CAP AMOUNT", "OPPS CAP AMOUNT", "CAP"], "OPPS cap amount"),

    # --------------------------------------------------------
    # HCPCS mappings
    # --------------------------------------------------------
    ("HCPCS", "hcpcs_code", ["HCPC", "HCPCS", "HCPCS CODE", "CODE"], "HCPCS code"),
    ("HCPCS", "short_description", ["SHORT DESCRIPTION", "SHORT DESC", "SHORTDESCRIPTION"], "Short description"),
    ("HCPCS", "long_description", ["LONG DESCRIPTION", "LONG DESC", "LONGDESCRIPTION", "DESCRIPTION"], "Long description"),
    ("HCPCS", "add_date", ["ADD DT", "ADD DATE", "ADDED DATE"], "Date code was added"),
    ("HCPCS", "effective_date", ["ACT EFF DT", "EFFECTIVE DATE", "EFF DATE", "ACTION EFFECTIVE DATE"], "Effective date"),
    ("HCPCS", "termination_date", ["TERM DT", "TERMINATION DATE", "TERM DATE", "END DATE"], "Termination date"),
    ("HCPCS", "betos_code", ["BETOS", "BETOS CODE", "TOS"], "Type of service classification"),
    ("HCPCS", "coverage_code", ["COV", "COVERAGE", "COV CODE", "COVERAGE CODE"], "Coverage indicator"),

    # --------------------------------------------------------
    # NCCI_PTP mappings
    # --------------------------------------------------------
    ("NCCI_PTP", "comprehensive_code", ["Column 1", "Column1", "COLUMN 1", "CODE 1", "COMPREHENSIVE CODE"], "Comprehensive/parent code"),
    ("NCCI_PTP", "component_code", ["Column 2", "Column2", "COLUMN 2", "CODE 2", "COMPONENT CODE"], "Component/child code"),
    ("NCCI_PTP", "effective_date", ["Effective Date", "EffectiveDate", "EFFECTIVE DATE", "EFF DATE", "EFF_DATE"], "Edit effective date"),
    ("NCCI_PTP", "deletion_date", ["Deletion Date", "DeletionDate", "DELETION DATE", "DEL DATE", "DEL_DATE", "END DATE"], "Edit deletion date"),
    ("NCCI_PTP", "modifier_indicator", ["Modifier", "MODIFIER", "MOD IND", "MODIFIER INDICATOR", "Modifier 0=not allowed"], "Modifier indicator"),
    ("NCCI_PTP", "rationale", ["PTP Edit Rationale", "Rationale", "RATIONALE", "PTP RATIONALE", "EDIT RATIONALE"], "Reason for bundling"),
    ("NCCI_PTP", "prior_1996_flag", ["*=in existence prior to 1996", "*=IN EXISTENCE", "PRIOR 1996", "PRE-1996"], "Historical flag"),

    # --------------------------------------------------------
    # NCCI_MUE_DME mappings
    # --------------------------------------------------------
    ("NCCI_MUE_DME", "hcpcs_code", ["HCPCS/CPT Code", "HCPCS Code", "CPT/HCPCS Code", "HCPCS", "CPT Code"], "HCPCS code"),
    ("NCCI_MUE_DME", "mue_value", ["DME Supplier Services MUE Values", "DME MUE Values", "MUE Values", "DME Supplier MUE"], "DME-specific MUE value"),
    ("NCCI_MUE_DME", "mai_description", ["MUE Adjudication Indicator", "MAI", "Adjudication Indicator"], "MAI description"),
    ("NCCI_MUE_DME", "mue_rationale", ["MUE Rationale", "Rationale", "MUE Rationale Code"], "Reason for limit"),

    # --------------------------------------------------------
    # NCCI_MUE_PRAC mappings
    # --------------------------------------------------------
    ("NCCI_MUE_PRAC", "hcpcs_code", ["HCPCS/CPT Code", "HCPCS Code", "CPT/HCPCS Code", "HCPCS", "CPT Code"], "HCPCS code"),
    ("NCCI_MUE_PRAC", "mue_value", ["Practitioner Services MUE Values", "Practitioner MUE Values", "MUE Values", "Practitioner MUE"], "Practitioner-specific MUE value"),
    ("NCCI_MUE_PRAC", "mai_description", ["MUE Adjudication Indicator", "MAI", "Adjudication Indicator"], "MAI description"),
    ("NCCI_MUE_PRAC", "mue_rationale", ["MUE Rationale", "Rationale", "MUE Rationale Code"], "Reason for limit"),

    # --------------------------------------------------------
    # NCCI_MUE_OPH mappings
    # --------------------------------------------------------
    ("NCCI_MUE_OPH", "hcpcs_code", ["HCPCS/CPT Code", "HCPCS Code", "CPT/HCPCS Code", "HCPCS", "CPT Code"], "HCPCS code"),
    ("NCCI_MUE_OPH", "mue_value", ["Outpatient Hospital Services MUE Values", "Outpatient Hospital MUE Values", "Hospital MUE Values", "MUE Values"], "Outpatient Hospital-specific MUE value"),
    ("NCCI_MUE_OPH", "mai_description", ["MUE Adjudication Indicator", "MAI", "Adjudication Indicator"], "MAI description"),
    ("NCCI_MUE_OPH", "mue_rationale", ["MUE Rationale", "Rationale", "MUE Rationale Code"], "Reason for limit"),
]


async def seed_database():
    """Seed the database with metadata."""
    settings = get_settings()

    print("Connecting to database...")
    conn = await asyncpg.connect(settings.database_url)

    try:
        # ============================================================
        # SEED DATA SOURCES
        # ============================================================
        print("\n" + "=" * 60)
        print("Seeding data sources...")
        print("=" * 60)

        for source in DATA_SOURCES:
            await conn.execute("""
                INSERT INTO meta.data_sources
                (source_code, source_name, category, description, target_table, update_frequency, display_order)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (source_code) DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    category = EXCLUDED.category,
                    description = EXCLUDED.description,
                    target_table = EXCLUDED.target_table,
                    update_frequency = EXCLUDED.update_frequency,
                    display_order = EXCLUDED.display_order,
                    updated_at = NOW()
            """, source["source_code"], source["source_name"], source["category"],
                source["description"], source["target_table"], source["update_frequency"],
                source["display_order"])
            print(f"  - {source['source_code']}: {source['source_name']}")

        source_count = await conn.fetchval("SELECT COUNT(*) FROM meta.data_sources")
        print(f"\nTotal data sources: {source_count}")

        # ============================================================
        # SEED CANONICAL COLUMNS
        # ============================================================
        print("\n" + "=" * 60)
        print("Seeding canonical columns...")
        print("=" * 60)

        # Build source_code -> id mapping
        source_ids = {}
        rows = await conn.fetch("SELECT id, source_code FROM meta.data_sources")
        for row in rows:
            source_ids[row["source_code"]] = row["id"]

        column_count = 0
        for col in CANONICAL_COLUMNS:
            source_code, internal_name, display_name, data_type, is_nullable, is_required, semantic_context, analyzer_usage, display_order = col
            source_id = source_ids.get(source_code)

            if not source_id:
                print(f"  WARNING: Source {source_code} not found, skipping column {internal_name}")
                continue

            await conn.execute("""
                INSERT INTO meta.canonical_columns
                (source_id, internal_name, display_name, data_type, is_nullable, is_required, semantic_context, analyzer_usage, display_order)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (source_id, internal_name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    data_type = EXCLUDED.data_type,
                    is_nullable = EXCLUDED.is_nullable,
                    is_required = EXCLUDED.is_required,
                    semantic_context = EXCLUDED.semantic_context,
                    analyzer_usage = EXCLUDED.analyzer_usage,
                    display_order = EXCLUDED.display_order
            """, source_id, internal_name, display_name, data_type, is_nullable,
                is_required, semantic_context, analyzer_usage, display_order)
            column_count += 1

        print(f"  Processed {column_count} canonical columns")

        total_columns = await conn.fetchval("SELECT COUNT(*) FROM meta.canonical_columns")
        print(f"Total canonical columns: {total_columns}")

        # ============================================================
        # SEED COLUMN MAPPINGS
        # ============================================================
        print("\n" + "=" * 60)
        print("Seeding column mappings...")
        print("=" * 60)

        mapping_count = 0
        for mapping in COLUMN_MAPPINGS:
            source_code, internal_name, source_headers, notes = mapping
            source_id = source_ids.get(source_code)

            if not source_id:
                print(f"  WARNING: Source {source_code} not found, skipping mapping for {internal_name}")
                continue

            # Get canonical_column_id
            canonical_id = await conn.fetchval("""
                SELECT id FROM meta.canonical_columns
                WHERE source_id = $1 AND internal_name = $2
            """, source_id, internal_name)

            if not canonical_id:
                print(f"  WARNING: Column {source_code}.{internal_name} not found, skipping mapping")
                continue

            await conn.execute("""
                INSERT INTO meta.column_mappings
                (canonical_column_id, source_headers, notes)
                VALUES ($1, $2, $3)
                ON CONFLICT (canonical_column_id) DO UPDATE SET
                    source_headers = EXCLUDED.source_headers,
                    notes = EXCLUDED.notes
            """, canonical_id, source_headers, notes)
            mapping_count += 1

        print(f"  Processed {mapping_count} column mappings")

        total_mappings = await conn.fetchval("SELECT COUNT(*) FROM meta.column_mappings")
        print(f"Total column mappings: {total_mappings}")

        # ============================================================
        # SUMMARY
        # ============================================================
        print("\n" + "=" * 60)
        print("SEED DATA COMPLETE")
        print("=" * 60)
        print(f"  Data sources: {source_count}")
        print(f"  Canonical columns: {total_columns}")
        print(f"  Column mappings: {total_mappings}")

    finally:
        await conn.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    asyncio.run(seed_database())
