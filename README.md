# IRIS Viewer

Public Streamlit viewer for IRIS R&D notices.

## Structure

- `IRIS`
  - `Notice`: all notices
  - `Summary`: notice-level summary
  - `Opportunity`: opportunity-level rows
- `Other Crawlers`
  - reserved for future sources

## Required Secrets

Use Streamlit secrets or environment variables for:

- `GOOGLE_SHEET_ID`
- `gcp_service_account`

Optional:

- `GOOGLE_CREDENTIALS_JSON`
- `GOOGLE_CREDENTIALS_JSON_CONTENT`

## Local Run

```bash
pip install -r requirements.txt
streamlit run app.py
```
iris_analysis_viewer
