# IRIS Viewer

Streamlit viewer for IRIS R&D notices and selected external crawler sources.
This folder is prepared as a standalone public viewer bundle.

## Structure

- `IRIS`
  - `Notice`: all notices
  - `Opportunity`: opportunity-level rows
- `MSS`
  - `MSS_CURRENT`: active/scheduled notices
  - `MSS_PAST`: closed notices
- `NIPA`
  - `NIPA_CURRENT`: active/scheduled notices
  - `NIPA_PAST`: closed notices
- `Favorites`
  - unified view for notices whose review status is `관심공고`
- `public_viewer_app.py`
  - public viewer body that boots the app runtime and renders the admin-aligned viewer shell
- `viewer_body.py`
  - public viewer body renderers for RFP Queue, Notice Queue, Summary, and detail pages
- `shared_app.py`
  - shared helper layer for login, Google Sheets access, comments, favorites, and domain-scoped collaboration state
- `app.py`
  - default public viewer entrypoint that runs `public_viewer_app.main()`
- `viewer_app.py`
  - alias entrypoint that runs `public_viewer_app.main()`
- `app_config.py`
  - viewer source/page config used by the shared app

Notice detail pages support review status updates and comment history through Google Sheets.
Viewer login is enabled by default. Users can request signup from the viewer
login screen. Signup approval and account provisioning are handled in a separate
private admin app, while users with the same email domain share comments,
favorites, and review status within the same workspace scope.

## Required Secrets

Use Streamlit secrets or environment variables for:

- `GOOGLE_SHEET_ID`
- `gcp_service_account`

Optional:

- `APP_USERS`: bootstrap viewer accounts, for example `{"viewer":"password"}`
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
Alternate viewer entrypoint:
```bash
streamlit run viewer_app.py
```
