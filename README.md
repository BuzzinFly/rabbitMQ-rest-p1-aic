# Academico ERP REST Facade

Small FastAPI service that wraps the ERP Academico API for PingOne AIC. It fetches an OAuth token, calls ERP endpoints, and normalizes responses.

## Requirements

- Python 3.9+
- Access to ERP token endpoint + ERP API base (or the included mock)

## Configure

Copy the template and fill in real values:
```
cp .env.example .env
```

Key variables:
- `ERP_TOKEN_URL`
- `ERP_API_BASE`
- `ERP_USERNAME`
- `ERP_PASSWORD`
- `ERP_CLIENT_ID`
- `ERP_CLIENT_SECRET`
- `ERP_HTTP_TIMEOUT_SECONDS` (optional)
- `ERP_TOKEN_SAFETY_SECONDS` (optional)

## Build / Install Dependencies

### macOS / Linux
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows (PowerShell)
```
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

### Run the facade
```
uvicorn academicoErpRestInterface:app --host 0.0.0.0 --port 8080
```

### (Optional) Run the mock ERP API

The mock server is useful for local testing and matches the default `.env.example` URLs.
```
uvicorn mockAcademicoApi:app --host 0.0.0.0 --port 8088
```

## API Endpoints

Base URL: `http://localhost:8080`

### GET /health
```
curl http://localhost:8080/health
```

### GET /aic/alumnos
Returns a normalized alumno record.

Query params:
- `idAlumno` (required, numeric)
- `itemsPerPage` (default 10, max 200)

Example:
```
curl "http://localhost:8080/aic/alumnos?idAlumno=988224&itemsPerPage=10"
```

### GET /aic/matriculas
Returns normalized matricula records.

Query params:
- `idAlumno` (required)
- `onlyActive` (default true)
- `itemsPerPage` (default 50, max 500)
- `pageIndex` (default 1)
- `alumnoId` (deprecated alias for `idAlumno`)

Example:
```
curl "http://localhost:8080/aic/matriculas?idAlumno=988224&onlyActive=true&itemsPerPage=50&pageIndex=1"
```
