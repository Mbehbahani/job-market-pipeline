# AGENTS.md

## Repository identity

- Recommended public repository name: `job-market-pipeline`
- Recommended product description: a reusable Python pipeline for scraping, filtering, enriching, deduplicating, and exporting job postings.
- Domain focus in this repository is an example profile for Optimization and Operations Research roles, not a hard product limitation.

## Agent goals

Agents working in this repository should keep the project public-safe, reproducible, and general-purpose.

Priority order:

1. Preserve data pipeline correctness.
2. Keep secrets and private infrastructure details out of version control.
3. Prefer generic naming over company-specific or project-specific naming.
4. Document behavior clearly in README.md.
5. Avoid adding features that are not already supported by the codebase.

## Working rules

- Do not commit real credentials, URLs with embedded secrets, account IDs, database dumps, or internal prompts.
- Use `.env.example` as the source of truth for required configuration. Never recreate a real `.env`.
- Treat generated SQLite databases, backups, logs, and run artifacts as disposable outputs.
- Keep existing pipeline stages conceptually intact:
  - scrape
  - preprocess
  - enrich and standardize
  - deduplicate
  - export
- When changing terminology, prefer `job`, `pipeline`, `profile`, `source`, and `export` over business-specific wording.

## Current codebase constraints

- The scraper logic is currently tuned to an Optimization / Operations Research example profile.
- Any generalization should preserve the current profile as a documented example, not remove it without replacement.
- The two-stage filtering design should remain explainable as:
  - broad retrieval for recall
  - stronger validation for precision
- Avoid major refactors unless they clearly improve public maintainability.

## Directory guidance

Preferred public structure over time:

```text
src/
  analysis/
  config/
  db/
  export/
  models/
  orchestrate/
pipelines/
  scrape/
  preprocess/
  enrich/
  deduplicate/
docs/
```

Until a larger refactor is requested, keep compatibility with the current numbered stage directories.

## Documentation expectations

When updating documentation:

- describe supported workflow only
- mark optional infrastructure as optional
- state that Optimization / Operations Research is an example usage profile
- explain recall and precision in plain language before using technical terms
- prefer concise, implementation-grounded documentation

## Safe change policy

Acceptable changes:

- README improvements
- AGENTS guidance
- `.gitignore` hardening
- `.env.example` maintenance
- removal of generated artifacts and internal-only files
- small naming and comment cleanup

Changes that require extra care:

- schema changes
- export behavior changes
- search/filter logic changes affecting job inclusion
- infrastructure-specific deployment files
