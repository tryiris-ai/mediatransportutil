# CLAUDE.md — mediatransportutil

## ai.knowledge

Run `/iris-load` at every session start and after every context condensation.
If `/iris-load` is unavailable, run the bash block from AGENTS.md.

## Critical Rules (always active)

**Bias towards action.** Default to action. Complete the job end-to-end. Never pause
to ask permission for routine steps. Only prompt for destructive/dangerous operations.

**Respect engineer availability.** When the engineer is away, don't block on questions.
Make reasonable decisions and push forward. Prefer conservative-but-complete over
stopping halfway.

**Escalate when stuck.** After 2-3 failed attempts: reload ai.knowledge indexes
(`/iris-load`), consult Codex (`/codex`), go multimodal (screenshots, DOM dumps),
frame-shift. Stop guessing, start observing.

**Completed job definition.** A job is not done until: code implemented, tests written
and passing, self-review against rules, PR opened with cross-repo links, CI monitored,
review feedback addressed, comment threads resolved.

**CLI tool path resolution.** Never report a CLI tool as "not found" without checking:
Homebrew (`/opt/homebrew/bin/`), nvm (`~/.nvm/versions/node/*/bin/`), and npm global
prefix. Source nvm before running node/npm/codex commands:
`export NVM_DIR="$HOME/.nvm" && [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"`

Full rules: `~/iris/ai.knowledge/rules/global/*.md`

## Skill Routing

When the user's request matches a skill, invoke it using the Skill tool as your
FIRST action. Do NOT answer directly first.

- Specs, "new feature", "spec this" → `/iris-spec`
- "Add a rule", agent mistake → `/iris-rule`
- "Document this", "the agent should know" → `/iris-learn`
- "Hand off to engineering" → `/iris-handoff`
- "Set up my machine", "onboard" → `/iris-onboard`
- "Sync", "submit rules" → `/iris-sync`
- "Install AGENTS.md", "install CLAUDE.md" → `/iris-install`
- "Audit", "review my changes" → `/iris-audit`
- "Clone", "get the repo" → `/iris-clone`
- "Run iris", "build from source" → `/iris-run`
- Bugs, errors, "why is this broken" → `/investigate`
- Ship, deploy, push, create PR → `/iris-ship`
- QA, test the site, find bugs → `/qa`
- Code review, check my diff → `/review`
- Performance, benchmarks → `/benchmark`

## Branching

Work in a branch. Only open a PR when asked. Never push to main.
