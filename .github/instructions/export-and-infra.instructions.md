---
applyTo: "*.py,src/orchestrate/**/*.py,src/export/**/*.py,.github/workflows/**/*.yml,Dockerfile"
---

# Copilot review instructions for export and infrastructure changes

When reviewing these files, focus on:

- environment-variable correctness
- AWS and Supabase configuration safety
- batch execution and scheduler assumptions
- container/runtime reproducibility
- clear operational logging and failure visibility

Flag changes that:

- expose secrets or encourage unsafe local credential handling
- make scheduled runs harder to debug
- blur the difference between local runs and deployment runs
- introduce brittle infrastructure assumptions without documentation

Prefer suggestions that improve observability, reproducibility, and safe deployment behavior.
