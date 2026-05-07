# IRIS Viewer

Streamlit viewer for IRIS R&D notices and selected external crawler sources.
This folder is prepared as a standalone viewer bundle that shares the same UI
structure and routing model as the admin app.

## Structure

- `IRIS`
  - `Notice`: all notices
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
  - default admin entrypoint that runs `shared_app.main(app_mode="admin")`
- `viewer_app.py`
  - viewer entrypoint that runs `shared_app.main(app_mode="viewer")`
- `app_config.py`
  - mode/source/page config used by the shared app

Notice detail pages support review status updates and comment history through Google Sheets.
Viewer login is enabled by default. Users can request signup from the viewer login
screen; requests are stored in `APP_USER_ACCOUNTS` with `pending` status and can
be approved from the admin app.

## Required Secrets

Use Streamlit secrets or environment variables for:

- `GOOGLE_SHEET_ID`
- `gcp_service_account`

Optional:

- `APP_USERS`: bootstrap accounts, for example `{"admin":"password"}`
- `APP_ADMINS`: comma-separated bootstrap admin IDs, for example `admin`
- `APP_ALLOWED_EMAIL_DOMAINS`: comma-separated company email domains allowed to request signup
- `APP_USER_ACCOUNT_SHEET`: defaults to `APP_USER_ACCOUNTS`
- `NOTICE_USER_REVIEW_SHEET`: defaults to `NOTICE_USER_REVIEWS`
- `USER_SCOPED_OPERATIONS`: defaults to `1`; accounts with the same email domain share operations, favorites, review status, and comments
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

Viewer only:

```bash
streamlit run viewer_app.py
```
