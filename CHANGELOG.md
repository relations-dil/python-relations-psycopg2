# Changelog

All notable changes to python-relations-psycopg2 are recorded here, newest first. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [SemVer](https://semver.org/).

## [Unreleased]

### Changed
- `make new` works on the ticket branch you're on: it writes VERSION and the changelog section there and commits, makes no branch of its own, and refuses on main, since we branch from the ticket. `B=` and `BRANCH_PREFIX` are gone (relations-dil/python-relations#98).

## [0.6.17] - 2026-10-26

### Added
- Jenkins releases on a merge to `main`: a `release` stage (only on `main`) runs `make release`, which does nothing for a version that is already released and otherwise does the test release, the release and the tag. It pushes the tag with the `github` credential, as Leroy Jenkins, one build at a time, with a two hour timeout (relations-dil/python-relations#80). The package builds run in a copy with only the files they use mounted read-only, so nothing root-owned is left in the workspace, and `make tag` stops on a failure instead of ignoring it. The release scripts work with the older git on the Jenkins host.
- `make help` lists every target with what it does (each target has a `## description` after it), and is what a bare `make` runs.
- Release tooling, the same as relations-dil's (relations-dil/python-relations#77): `make new` starts a release, `make version` sets the version, `make release-check` says where a release is up to and what would stop it, `make release` does what's left (checks, TestPyPI and a clean install from it, PyPI, the tag), `make dist-check` builds and checks the distributions, and `make test-release` tests the scripts against a throwaway repo and fake indexes. The work is in `scripts/`.

### Changed
- Requires relations-sql 0.6.11 and relations-postgresql 0.6.5, so a str field's `length` is honored in DDL (`VARCHAR(n)`) and a change to only its length is a column change (relations-dil/python-relations-postgresql#15, relations-dil/python-relations-sql#30).
- `make release-check` and `make release` don't run `build`, `test`, `lint` and `setup` again: the Jenkins stages have just run them before the release stage, so it was the same work twice. Run them yourself before checking by hand, as `make release-check` is only the checks and `dist-check` now (relations-dil/python-relations#90).
- Every `docker run` in the `Makefile` uses `--rm`, and a replaced database container is removed with its volume (`docker rm --force -v`), so builds and test runs leave no stopped containers or volumes behind.
- `testpypi` and `pypi` run in the current `python` image with `build` and `twine` installed on the fly, so the custom `gaf3/pypi` image is no longer used.

Shipping as 0.6.13, with relations-dil 0.6.16.

### Added
- A `VERSION` file as the one place the version is set: the `Makefile` reads it and mounts it, `testpypi` and `pypi` pass it as `BUILD_VERSION`, and `setup.py` reads `BUILD_VERSION` or the file.

### Changed
- A key stored in a dict field (`child_inject` in relations-dil 0.6.16) is queried by its path in that field, instead of a column that doesn't exist: new `retrieve_record` hands the record to `retrieve_field`, which looks an injected field up in the field it's stored in (as the extracted column if it's been extracted), and `sort` does the same. Counting, retrieving, updating and deleting by the key work, and mass updates through it are refused.

## [0.6.16] - 2026-10-14

### Changed
- A duplicate (Postgres sqlstate 23505, which a repeated primary key is too) raises `relations.UniqueError` from create and update, with the driver's `psycopg2.errors.UniqueViolation` as its cause, through the new `Source.write`. Any other integrity error is left as it is. The transaction stays the caller's: outside autocommit it has to be rolled back before a retry, as with any psycopg2 error.
- "none retrieved" and "more than one retrieved" are `relations.NotFoundError` and `relations.MultipleError` (same messages, still `ModelError`s). Needs relations-dil 0.6.19 (relations-dil/python-relations#78).

## [0.6.15] - 2026-10-14

### Fixed
- `psycopg2` is a floor, `psycopg2>=2.8.6`, not the exact `psycopg2==2.8.6`. That release doesn't compile on current Python (3.14), so `pip install relations-psycopg2` failed there. Tested against psycopg2 2.9.9 (`requirements.txt`).

### Changed
- `make setup` runs on the latest `python` image with `pip install .`, not the Python 3.8.5 Alpine image with `python setup.py install`, which ignores a package's Python requirement and so picked a psycopg2 too new for 3.8. The Alpine build packages are no longer needed.

## [0.6.14] - 2026-10-14

### Changed
- Needs relations-dil 0.6.18 and relations-postgresql 0.6.4: a string id (caller supplied, a non-int id) is created, read, updated, deleted and tied with its primary key. The source needed no change, the work is in relations-dil, relations-sql and relations-postgresql.

## [0.6.12] - 2026-06-08

- Fixed count and retrieve for models joined through tie attributes: when `_distinct` is set, counts use `COUNT(DISTINCT id)` and retrieve selects `DISTINCT` model rows.
- Bumped the relations-dil and relations-sql requirements.

## [0.6.11] - 2026-06-07

- Isolated the CI test Postgres container per build by suffixing its name with the network and removing it after the run.
- Bumped the version.

## [0.6.10] - 2026-06-06

- Added many-to-many support: `Source` now extends `relations_sql.SOURCE`, creates ties after create and update, and deletes them when models are deleted.
- Added updating and deleting ties by query, and ties are now included in count and retrieve.
- Updated for renamed relation attributes such as `child_parent_ref` and `parent_id`, and updated the Dockerfile, Makefile and requirements.

## [0.6.9] - 2022-11-25

- Switched the requirements to `relations-dil` and `relations-postgresql`, with minimum versions in setup.py.

## [0.6.8] - 2022-08-07

- Fixed the README and PyPI example to use `PsycoPg2Source` as the source name.

## [0.6.7] - 2022-08-07

- Fixed the project URL in setup.py to point to python-relations-psycopg2.

## [0.6.6] - 2022-08-07

- Prepared the package for PyPI by adding `LICENSE.txt`, `PYPI.md` and `testpypi` and `pypi` Makefile targets.
- Updated the README and requirements.

## [0.6.5] - 2022-03-14

- Bumped the relations, relations-sql and relations-postgresql requirements to fix migration issues.
- Combined the Makefile dependency installs into a single `pip install`.

## [0.6.4] - 2022-03-13

- Bumped the relations, relations-sql and relations-postgresql requirements to add familial access.

## [0.6.3] - 2022-02-18

- Renamed labels to titles: `labels` became `titles`, `_label` became `_titles`, and the result is now a `relations.Titles`.
- Bumped the relations, relations-sql and relations-postgresql requirements.

## [0.6.2] - 2021-11-21

- Moved the project to the relations-dil GitHub organization and renamed the package to `python-relations-psycopg2` in preparation for PyPI.
- Updated the dependency requirements to the new repository locations.

## [0.6.1] - 2021-11-13

- Renamed the `Source` methods to the shorter general interface: `init`, `define`, `create`, `count`, `retrieve`, `labels`, `update` and `delete`, with `retrieve_field` and `update_field` for per-field work, and `definition` and `migration` for converting files.
- Bumped the relations, relations-sql and relations-postgresql requirements.

## [0.6.0] - 2021-11-12

- Rebuilt the source on `relations-sql` and `relations-postgresql`, replacing hand-written SQL strings with their `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `TABLE`, `LIKE`, `IN` and `OR` classes.
- Models now use `STORE` and `SCHEMA` instead of `TABLE`, `DATABASE`, `QUERY` and `DEFINITION`; added `execute`, `model_define` and `create_query`.
- Updated the requirements to the new relations, relations-sql and relations-postgresql versions.

## [0.5.7] - 2021-08-27

- Added the `set` field kind, stored as a JSONB list with values sorted before querying.
- Fixed comparisons on JSON fields so non-scalar values are cast to `JSONB`.

## [0.5.6] - 2021-08-22

- Added list operators `has`, `any` and `all` for JSONB fields, using `@>` containment and `jsonb_array_length`.
- Bumped the relations requirement.

## [0.5.5] - 2021-08-16

- Renamed the migration tracking table to `_relations_migration` with a `stamp` column.
- Bumped the relations requirement.

## [0.5.4] - 2021-08-12

- Added `load` and `list` methods to run migration SQL files and list migration files by stamp and kind.
- Bumped the relations requirement.

## [0.5.3] - 2021-08-08

- Added migrations support: the source reports `KIND = "postgresql"` and defines tables, columns, indexes and extract columns from plain definition dicts through `column_define` and `extract_define`.
- Added `table_names` and `index` helpers for schema-qualified names, and corrected the class docstring to PsycoPg2.

## [0.5.2] - 2021-06-17

- Fixed empty lists: `in` with no values matches nothing, `not in` with no values matches everything, and label searches handle parents with no matches.

## [0.5.1] - 2021-06-16

- Bumped the relations requirement to pick up apply and access support; no library code changed.

## [0.5.0] - 2021-06-14

- Moved extract to the source: each key of a field's `extract` dict becomes its own generated column named `<field>__<path>` with a declared type.
- Filters and label searches on an extracted path now use that column instead of the JSON expression.

## [0.4.8] - 2021-06-13

- Switched to the simpler record interface: `auto` replaces `readonly`, and creates and updates use `record.create`, `record.update` and `record.mass`.
- Updates now skip the SQL statement when no fields changed.

## [0.4.7] - 2021-06-13

- Update values are now always taken from `field.export()`.

## [0.4.6] - 2021-06-02

- Added `model_count`, which returns the total of matching rows using `COUNT(*)`, applying filters and like searches but not the limit.

## [0.4.5] - 2021-05-31

- Added support for inject fields, which are skipped when creating and updating.
- Object values are now serialized to JSON on update through `field.export()`.

## [0.4.4] - 2021-05-30

- Added extracted fields: a field with `extract` is defined as a stored generated column derived from a JSON path.
- JSON path queries now cast boolean values, and the path helper was renamed `walk`.

## [0.4.3] - 2021-05-29

- Changed any non-scalar field kind to be stored as JSON, with an empty-object default.
- Label searches can now target a path inside a JSON field.

## [0.4.2] - 2021-05-27

- Changed list and dict columns from JSON to `JSONB`.
- Added searching inside JSON fields using `__` paths, with typed casts for int and float values.
- Added the `notlike` and `null` operators.

## [0.4.1] - 2021-05-11

- Added `model_labels`, which builds a `relations.Labels` structure from a retrieve.
- Bumped the relations requirement and made the Docker build use `--no-cache`.

## [0.4.0] - 2021-05-07

- Added searching with `like`, matched case-insensitively across the model's label fields, including labels of parent models.
- Renamed the `ge`/`le` operators to `gte`/`lte`, and quoted sort field names.
- Added `overflow` tracking when a limit is reached, and split retrieve into `model_like`, `model_sort` and `model_limit` helpers.

## [0.3.4] - 2021-03-28

- Fixed query construction so the limit is applied after the where clauses.

## [0.3.3] - 2021-02-23

- Added `limit` and `offset` support on retrieve.
- Bumped the relations requirement.

## [0.3.2] - 2021-02-21

- Switched multi-row inserts to `psycopg2.extras.execute_values` for faster bulk create.

## [0.3.1] - 2021-02-21

- Added sorting on retrieve from `_sort` or `_order`, producing `ORDER BY` with ascending and descending fields.
- Bumped the relations requirement.

## [0.3.0] - 2021-02-20

- Added bulk create support: with `_bulk` set, rows are inserted without fetching ids or updating the models.
- Bumped the relations requirement.

## [0.2.9] - 2021-02-20

- Fixed `Source` cleanup so a missing connection is not closed on deletion.

## [0.2.8] - 2021-02-05

- Updated for the renamed `thy()` model method and bumped the relations requirement.

## [0.2.7] - 2021-01-26

- Added `FLOAT` column support.

## [0.2.6] - 2021-01-24

- Callable field defaults are no longer written into column `DEFAULT` clauses.

## [0.2.5] - 2021-01-19

- Bumped the relations requirement and version; no library code changed.

## [0.2.3] - 2021-01-18

- Added list and dict fields, stored as JSON columns and encoded with `json.dumps` on create and update.
- Index names are now prefixed with the table name to avoid collisions.
- Updated the Jenkinsfile, Makefile and relations requirement.

## [0.2.2] - 2021-01-18

- Added `BOOLEAN` column support.
- Fields with `replace` set are reset to their default on update when not changed.
- Bumped the relations requirement.

## [0.2.1] - 2021-01-17

- Changed NOT NULL handling to follow the field's `none` setting.
- Added creation of unique and plain indexes from the model's `_unique` and `_index`; `model_define` now returns a list of statements.
- Bumped the relations requirement.

## [0.1.3] - 2021-01-07

- Changed integer columns without `serial` to be defined as `INT` instead of `SMALLINT`.

## [0.1.2] - 2021-01-07

- Changed the `schema` argument to default to `None`, so tables are no longer qualified with the `public` schema unless one is set.

## [0.1.1] - 2021-01-07

- Fixed the connection so it connects to the configured `database` (passed as `dbname`).

## [0.1.0] - 2021-01-04

- Initial release of the PostgreSQL Source for relations: a `Source` class in `relations_psycopg2` built on psycopg2 with `RealDictCursor`, supporting optional schema and an existing `connection`.
- Included DDL generation for tables (`field_define`/`model_define` with INT, VARCHAR, serial and primary key columns) and create with `RETURNING` for serial ids, plus retrieve operators (`eq`, `gt`, `ge`, `lt`, `le`).
- Added the project scaffolding: README, Dockerfile, Jenkinsfile, Makefile, `postgres.sh`, setup.py and a unit test suite.
