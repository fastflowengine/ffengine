# FFEngine - Claude Entry Wrapper

This file is the Claude-specific entry point.

## Rule Source
All binding rules and authority decisions are defined in:
- `handbook/AGENTS.md`

If this file conflicts with `AGENTS.md`, `AGENTS.md` wins.

## Claude Notes
- Start each session by reading `handbook/AGENTS.md` first.
- Then load only the required `wbs/`, `context/`, `reference/`, workflow, and skill files for the active task.
- Keep decisions aligned with the canonical authority order in `AGENTS.md`.
