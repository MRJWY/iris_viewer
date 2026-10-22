This repository is PUBLIC_VIEWER_REPO.

## Supabase discovery deployment

The deployed `app.py` calls `public_viewer_app.main`. With a configured database,
the existing viewer login guard runs before the Supabase discovery workspace.
The home page shows four cards (latest notices, recommended RFPs, closing notices,
and all notices) and search. The all-notices card opens the existing full search,
including closed and archived notices.
Notice titles are joined from the notice master, never inferred from filenames.

Configure this app's own Streamlit Secrets (separate from the admin app):

```toml
SUPABASE_DB_URL = "your-existing-database-connection"
```

Put these keys above any TOML section headers. Never commit real credentials.
The default backend is `auto`; `DISCOVERY_DATA_BACKEND="supabase"` is optional.
The discovery repository uses read-only transactions. Account verification,
favorites and comments retain their existing storage and permissions; comments
load on request. Existing legacy workflow links remain available.

The bundled discovery components/services match the admin repository's discovery
modules; `pages/discovery_workspace.py` adapts the public viewer's existing auth
and detail interfaces. This repository runs independently on Streamlit Cloud.
The older sibling-runtime notes below describe local compatibility wrappers only.

# IRIS Viewer Shim Repo

This repository is now officially treated as a shim repo for the viewer runtime.
The active Streamlit behavior lives in the sibling `iris_crawling` repository,
and this repo keeps compatibility entrypoints so existing viewer commands still
work without duplicating business logic in two places.

Source of truth:
- runtime behavior: `../app.py` in the sibling `iris_crawling` repo
- this repo: shim entrypoints, compatibility wrappers, and viewer-specific docs

Operational rule:
- if a change should affect the live viewer behavior, update `iris_crawling/app.py`
- do not add new queue/detail/dashboard logic to `iris_viewer_repo/shared_app.py`
- keep files here thin and explicit about their delegation behavior

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
  - compatibility entrypoint; delegates to the sibling `iris_crawling/app.py`
- `viewer_body.py`
  - compatibility wrapper that forwards legacy viewer body calls to the sibling runtime
- `shared_app.py`
  - legacy local shared layer retained only for compatibility; not the source of truth
- `app.py`
  - default shim entrypoint; runs the sibling viewer runtime
- `viewer_app.py`
  - alias shim entrypoint; runs the sibling viewer runtime
- `root_app_proxy.py`
  - loader that imports `../app.py` from the sibling `iris_crawling` repo and runs viewer mode
- `app_config.py`
  - legacy viewer config kept only for compatibility with remaining local wrappers

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

This repo no longer launches independently. It expects the sibling
`iris_crawling` repository to exist next to it.

```bash
pip install -r requirements.txt
streamlit run app.py
```

Alternate viewer entrypoint:
```bash
streamlit run viewer_app.py
```
