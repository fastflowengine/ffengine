# FFEngine - Agent Authority and Execution Rules

This file is the single canonical authority for agent behavior in this repository.

## Purpose
Define mandatory rules, precedence, and execution flow for all agents (Claude, Gemini, and others) working on FFEngine.

## Canonical Authority
Conflict resolution must follow this order:
1. `AGENTS.md` (this file)
2. Relevant `wbs/*.md` (wave plan, gates, delivery)
3. Relevant `context/*.md` (epic scope and constraints)
4. `reference/*.md` (technical contracts and policies)
5. `.agent/workflows/*.md` (operational protocols)
6. `skills/*.md` (implementation patterns)
7. Legacy code examples or historical outputs

`CLAUDE.md` and `GEMINI.md` are entry wrappers only. They cannot override this file.

## Mandatory Rules
- Decide scope first: `Common`, `Community`, or `Enterprise`.
- Do not break wave order. Do not implement a task before prior gate criteria pass.
- Community scope uses Python engine only (`fetchmany + executemany`, DBAPI-based path).
- Do not leak Enterprise-only capabilities into Community (`COPY`, `BCP`, `OCI_BATCH`, `ack/nack`, `DLQ`).
- Write tests for every change. Do not ship without tests.
- Use structured logging. Do not use `print()`.
- Every delivery must include review result and handoff/checkpoint update.

## Workflow Requirements
Use workflow docs when relevant:
- New epic start: `.agent/workflows/start-epic.md`
- Config generation: `.agent/workflows/generate-config.md`
- Mapping generation: `.agent/workflows/generate-mapping.md`
- Test/gate execution: `.agent/workflows/run-tests.md`
- Incident recovery: `.agent/workflows/error-recovery.md`

## Working Model
1. Lock scope and wave from WBS.
2. Read epic context and related contracts.
3. Implement with skills and coding standards.
4. Run required tests and gate checks.
5. Produce review + handoff/checkpoint artifacts.

## Project Status Baseline
- Active baseline: Community GA waves.
- Enterprise waves open only after Community GA acceptance criteria are met.

## Document Roles
- `wbs/`: planning, dependencies, gate checks, release progression
- `context/`: epic-level implementation boundaries
- `reference/`: stable technical contracts and policies
- `skills/`: coding patterns and reusable implementation templates
