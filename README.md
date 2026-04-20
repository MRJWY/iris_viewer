# IRIS Viewer

Public Streamlit viewer for IRIS R&D notices.

## Structure

- `IRIS`
  - `Notice`: all notices
  - `Summary`: notice-level summary
  - `Opportunity`: opportunity-level rows
- `중소기업벤처부`
  - `MSS_CURRENT`: active/scheduled notices
  - `MSS_PAST`: closed notices
- `NIPA`
  - `NIPA_CURRENT`: active/scheduled notices
  - `NIPA_PAST`: closed notices
- `Other Crawlers`
  - reserved for future sources

## Required Secrets

Use Streamlit secrets or environment variables for:

- `GOOGLE_SHEET_ID`
- `gcp_service_account`

Optional:

- `GOOGLE_CREDENTIALS_JSON`
- `GOOGLE_CREDENTIALS_JSON_CONTENT`
- `MSS_CURRENT_SHEET` / `MSS_PAST_SHEET`
- `NIPA_CURRENT_SHEET` / `NIPA_PAST_SHEET`

## Local Run

```bash
pip install -r requirements.txt
streamlit run app.py
```
iris_analysis_viewer
