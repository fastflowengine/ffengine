# FFEngine GitHub Organization Runbook

## Purpose

This document operationalizes the FFEngine GitHub setup for the product launch.
It is the repository-local source of truth for moving this codebase into the
`ffengine.com` organization model.

## Target State

- GitHub organization: `ffengine` if available, otherwise `ffenginehq`, then `ffengine-dev`
- Owners: `caglar.sahin@ffengine.com`, `burcu.sahin@ffengine.com`
- Public repository: `ffengine`
- Private repository: `ffengine-enterprise`
- Public repository: `ffengine-docs`
- Private repository: `ffengine-www`
- Shared organization templates: separate `.github` repository at org level

## Manual Tasks Outside This Repository

The following actions must be completed in GitHub and cannot be executed from
this repository alone:

1. Create or update the individual GitHub accounts for Caglar and Burcu.
2. Create the GitHub organization.
3. Verify the `ffengine.com` domain in GitHub.
4. Enforce organization 2FA.
5. Transfer this repository into the organization and rename it to `ffengine`.
6. Create the sibling repositories listed in the target state.

## Migration Sequence

1. Prepare the two owner accounts.
2. Create the organization with Caglar as the initial owner.
3. Add Burcu as a second owner immediately.
4. Verify `ffengine.com` and apply email/domain restrictions if available.
5. Transfer the current repository into the organization.
6. Rename the transferred repository to `ffengine`.
7. Create `ffengine-enterprise`, `ffengine-docs`, and `ffengine-www`.
8. Apply branch protections and team permissions.
9. Validate PR flow with a test branch and review.

## Repository Boundaries

### `ffengine` (public)

- Community runtime
- Shared public contracts
- Public issue tracker and discussions
- No Enterprise-only runtime code

### `ffengine-enterprise` (private)

- Queue runtime
- Native bulk adapters
- DLQ, retry, multi-lane, delivery policy
- Depends on released `ffengine` versions

### `ffengine-docs` (public)

- Technical product docs
- Versioned documentation
- Public quickstart and examples

### `ffengine-www` (private)

- Marketing site
- Web deployment configuration
- Commercial content and operational secrets

## Required Organization Controls

- Require 2FA for all members
- Set default repository permission to `Read`
- Use teams for write/admin separation
- Prefer GitHub Apps over shared bot users
- Protect `main` with PR reviews and status checks

## Acceptance Checklist

- Two verified owner accounts exist
- Organization is created
- Domain verification is complete
- This repository is public under the org as `ffengine`
- `ffengine-enterprise` exists as private
- Branch protection is active on `main`
- CODEOWNERS and security/contact files are present
- Test PR opened by one owner and merged by the other
- Git hygiene rules are documented and enforced before first public push
