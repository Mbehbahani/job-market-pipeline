# GitHub Copilot instructions for job-market-pipeline

## Project context

`job-market-pipeline` is a production-oriented Python pipeline for scraping, filtering, standardizing, deduplicating, and exporting job postings.

The repository currently demonstrates a practical job-market data workflow with:

- source scraping and country-based retrieval
- recall/precision-aware filtering logic
- preprocessing and normalization stages
- enrichment and taxonomy standardization
- deduplication before downstream export
- optional Supabase export and AWS snapshot support

## Engineering expectations

When suggesting or generating changes, prefer:

- small, reviewable diffs
- explicit handling of retries, partial failures, and restartability
- preserving the pipeline stage boundaries already present in the repository
- structured, deterministic processing over hidden or prompt-only behavior
- maintainable data flow and configuration clarity
- changes that keep the public/open-source version safe and reproducible

## Pipeline guardrails

### Scraping and retrieval

- Keep broad-recall retrieval logic understandable and configurable.
- Do not weaken negative filtering without clear justification.
- Preserve practical protections around noisy job titles, low-quality postings, and irrelevant role matches.
- Be careful with rate limiting, retries, and source-specific assumptions.

### Data processing and export

- Avoid changes that silently alter schema expectations or exported field meanings.
- Prefer explicit normalization and documented transformation steps.
- Keep deduplication logic transparent and operationally safe.
- Preserve separation between local stage storage, enrichment logic, and external export targets.

### Secrets and infrastructure

- Never suggest committing real credentials or local `.env` files.
- Flag any code that could expose Supabase keys, AWS settings, or internal endpoints.
- Prefer environment-variable driven configuration over hardcoded deployment values.

## Code review priorities

Pay extra attention to:

- filtering quality and false-positive risk
- data integrity across pipeline stages
- duplicate prevention and idempotency
- export correctness and schema drift
- AWS / Supabase configuration safety
- maintainability of orchestration scripts and stage boundaries

## Style preferences

- Be concise and practical in code comments.
- Prefer operational clarity over clever abstractions.
- Match the repository's existing structure and naming.
- Suggest changes that are easy to test and reason about.
