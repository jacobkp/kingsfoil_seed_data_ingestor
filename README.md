# Kingsfoil Data Pipeline

A FastAPI-based data ingestion pipeline for CMS healthcare datasets. Upload, validate, and manage versioned data with support for multi-part files.

> **Early Version** - Core functionality is working. The architecture is designed to be extendable to handle any tabular data source.

## Features

- **File Upload & Validation** - Upload Excel/CSV files with automatic header detection and column mapping
- **Version Management** - Track data versions with status (pending, processing, completed, failed)
- **Multi-Part File Support** - Handle large datasets split across multiple files (e.g., NCCI PTP)
- **Extensible Sources** - Add new data sources via database configuration
- **Works with Kingsfoil Analyser** - Pairs with the analyser app for data querying and analysis

## Quick Start

### 1. Clone & Setup

```bash
git clone <your-repo-url>
cd kingsfoil-pipeline

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Database

Create a `.env` file from the example:

```bash
cp .env.example .env
```

Edit `.env` with your PostgreSQL connection string:

```
DATABASE_URL=postgresql://user:password@host:5432/database?sslmode=require
```

Works with **Neon**, **Supabase**, **Railway**, or any PostgreSQL database.

### 3. Initialize Database

```bash
python -m scripts.init_db
```

This creates the required schemas (`meta`, `cms`) and tables.

### 4. Run the Server

```bash
uvicorn app.main:app --reload
```

Open [http://localhost:8000](http://localhost:8000) in your browser.

## Project Structure

```
kingsfoil-pipeline/
├── app/
│   ├── main.py              # FastAPI application
│   ├── config.py            # Settings management
│   ├── routes/              # API endpoints
│   ├── services/            # Business logic (ingestor, validator)
│   └── templates/           # Jinja2 HTML templates
├── scripts/
│   ├── init_db.py           # Database initialization
│   └── migrate_*.py         # Migration scripts
├── requirements.txt
└── .env.example
```

## Schema Reference

See [SCHEMA.md](SCHEMA.md) for complete documentation of:
- All CMS data tables and columns
- Data types and relationships
- Common SQL queries
- File format notes and header mappings

## Adding New Data Sources

Data sources are configured in the `meta.data_sources` table. Each source defines:
- Column mappings (file columns → database columns)
- Expected file format and header row
- Variants (if applicable)
- Multi-part file support

## Database Migrations

If you encounter schema errors after updates, check the `scripts/` folder for migration scripts:

```bash
python -m scripts.migrate_add_part_count
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `DEBUG` | Enable debug mode | `false` |
| `MAX_UPLOAD_SIZE_MB` | Max file upload size | `100` |

