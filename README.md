# IRIS Viewer

Streamlit viewer for IRIS R&D notices and selected external crawler sources.
This folder is prepared as a standalone viewer bundle that shares the same UI
structure and routing model as the admin app.

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
- `관심 공고`
  - unified view for notices whose review status is `관심공고`
- `shared_app.py`
  - shared renderer and routing logic copied from the main app
- `app.py`
  - thin viewer entrypoint that runs `shared_app.main(app_mode="viewer")`
- `app_config.py`
  - mode/source/page config used by the shared app

Notice detail pages support review status updates and comment history through Google Sheets.

## Required Secrets

Use Streamlit secrets or environment variables for:

- `GOOGLE_SHEET_ID`
- `gcp_service_account`

Optional:

- `GOOGLE_CREDENTIALS_JSON`
- `GOOGLE_CREDENTIALS_JSON_CONTENT`
- `MSS_CURRENT_SHEET` / `MSS_PAST_SHEET`
- `NIPA_CURRENT_SHEET` / `NIPA_PAST_SHEET`
- `NOTICE_COMMENT_SHEET`

## Local Run

```bash
pip install -r requirements.txt
streamlit run app.py
```
