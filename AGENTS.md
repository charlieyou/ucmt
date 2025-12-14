## Development with uv

```bash
uv sync                  # Install dependencies
uv run pytest            # Run tests
uv run ucmt              # Run CLI
uv add <package>         # Add dependency
uv run ruff check .      # Lint
uv run ruff format .     # Format
```

## Issue Tracking with bd (beads)

Use **bd** for ALL issue tracking. No markdown TODOs.

### Commands

```bash
bd ready --json                    # Find unblocked work
bd create "Title" -t task -p 2 --json
bd update <id> --status in_progress --json
bd close <id> --reason "Done" --json
```

### Workflow

1. `bd ready` → find work
2. `bd update <id> --status in_progress` → claim
3. Create worktree: `git worktree add ../ucmt-wt/<branch-name> -b <branch-name> origin/main`
4. Work in the worktree
5. Use oracle to review your changes
6. Push and create PR: `git push -u origin <branch-name> && gh pr create`
    a. Include Amp thread URL and beads ID in description
    b. Include how I can verify that the work is completed, a command to run to test new functionality
7. `bd close <id>` → complete
8. Commit `.beads/issues.jsonl` with code changes
