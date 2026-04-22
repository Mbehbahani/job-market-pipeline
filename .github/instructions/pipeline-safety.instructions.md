---
applyTo: "src/**/*.py,1- Scrapped Data/**/*.py,2- Preprocessed/**/*.py,3- Enrichment + Standardization/**/*.py,4- Deduplicate/**/*.py"
---

# Copilot review instructions for pipeline safety

When reviewing code in these files, prioritize:

- preserving pipeline-stage boundaries
- preventing silent data loss or duplicate creation
- retry, restart, and partial-failure safety
- precision/recall impacts in filtering logic
- explicit schema and field handling
- avoiding accidental hardcoding of secrets, local paths, or deployment-specific values

Flag changes that:

- weaken negative filtering or validation without explanation
- mix unrelated scraping, enrichment, and export responsibilities together
- make deduplication or export behavior harder to reason about
- introduce hidden side effects in data processing steps

Prefer suggestions that keep the pipeline deterministic, reviewable, and operationally safe.
