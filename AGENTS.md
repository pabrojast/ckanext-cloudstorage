# Repository Guidelines

## Project Structure & Module Organization
- `ckanext/cloudstorage/` holds the plugin code: `plugin/`, `logic/`, `views/`, `templates/`, and `fanstatic/` assets.
- `ckanext/__init__.py` defines the namespace package.
- Root files: `setup.py`, `requirements.txt`, and `README.md`.
- `ckanext-blob-storage/` and `ckanext-schemingdcat/` exist but are empty placeholders in this repo.

## Build, Test, and Development Commands
- `pip install -r requirements.txt` installs runtime dependencies.
- `pip install -e .` installs the extension in editable mode.
- Enable the plugin in your CKAN ini: `ckan.plugins = ... cloudstorage`.
- `paster cloudstorage initdb -c /etc/ckan/default/production.ini` creates multipart tables (required for secure URLs/multipart).
- `paster cloudstorage migrate <ckan.storage_path>/resources -c /path/to/ini` migrates existing uploads.

## Coding Style & Naming Conventions
- Python code uses 4-space indentation and PEP8-style formatting; follow the existing patterns.
- Functions/vars: `snake_case`; classes: `CamelCase`; module filenames: lowercase.
- Keep logging consistent with `log = logging.getLogger(__name__)` and favor explicit error handling.

## Testing Guidelines
- No dedicated test suite is included here.
- Validate changes against a local CKAN instance by uploading/downloading resources and checking secure URL and multipart flows.
- If you add tests, create a `ckanext/cloudstorage/tests/` package and document how to run them in the PR.

## Commit & Pull Request Guidelines
- Recent commit messages are short, imperative, and scoped (for example, "Enhance ResourceCloudStorage...", "Refactor ...", "Update ...").
- Prefer clear verb-first messages; avoid vague "fix" when you can be specific.
- PRs should describe behavior changes, config impacts, and any migration steps; include repro steps or logs for storage issues.

## Configuration & Security Tips
- Minimum config: `ckanext.cloudstorage.driver`, `ckanext.cloudstorage.container_name`, `ckanext.cloudstorage.driver_options`.
- For private resources, set `ckanext.cloudstorage.use_secure_urls = 1` and ensure the provider SDK is installed (Azure or AWS S3).
