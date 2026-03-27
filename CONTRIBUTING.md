# Contributing to FFEngine Community

## Scope

This repository accepts changes for the public Community product and shared
contracts only. Enterprise-only runtime capabilities must not be added here.

## Workflow

1. Create a branch from `main`.
2. Open a pull request.
3. Ensure CI passes.
4. Obtain review before merge.

## Rules

- Do not push directly to `main`.
- Keep Community and Enterprise boundaries intact.
- Add or update tests for behavior changes.
- Prefer small, reviewable pull requests.
- Before push, review tracked files and keep local/runtime artifacts out of Git.
- Do not commit local virtualenvs, `.env` files, caches, generated `*.egg-info`,
  root-level binary documents, or local archive bundles.
- If an ignored file was already tracked, remove it from the Git index with
  `git rm --cached` and keep the working copy locally.

## Repository Boundaries

- Allowed here: Community runtime, shared interfaces, public docs references
- Not allowed here: Enterprise queue runtime, bulk-only private adapters, commercial secrets

## Security

Do not commit credentials, tokens, production URLs with secrets, or customer data.
Report vulnerabilities using the process in `SECURITY.md`.
