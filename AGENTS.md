# AGENTS.md — mediatransportutil

## ai.knowledge — Shared Rules & Knowledge

This repo uses organizational rules and knowledge from `~/iris/ai.knowledge`.
Run this block at **every session start** and after **every context reset/condensation**.

```bash
AI_KNOWLEDGE="$HOME/iris/ai.knowledge"
if [ ! -d "$AI_KNOWLEDGE/.git" ]; then
  echo "STATUS=missing"
else
  cd "$AI_KNOWLEDGE"
  CURRENT_BRANCH=$(git branch --show-current)
  git fetch origin --quiet 2>/dev/null
  if [ "$CURRENT_BRANCH" = "main" ]; then
    BEHIND=$(git rev-list HEAD..origin/main --count 2>/dev/null || echo "0")
    [ "$BEHIND" -gt 0 ] && git pull --ff-only --quiet 2>/dev/null && echo "STATUS=updated COMMITS=$BEHIND" || echo "STATUS=current"
  else
    PENDING=$(git rev-list origin/main..HEAD --count 2>/dev/null || echo "0")
    echo "STATUS=contributions PENDING=$PENDING BRANCH=$CURRENT_BRANCH"
  fi
  cd - > /dev/null
fi
```

- `STATUS=missing` — warn: "ai.knowledge not found. Run `/iris-onboard`."
- `STATUS=contributions PENDING={N>0}` — nag once: "Run `/iris-sync` to submit."

After startup, **read these index files** (rules are hard constraints, knowledge is context):
```
~/iris/ai.knowledge/rules/global/INDEX.md
~/iris/ai.knowledge/rules/mediatransportutil/INDEX.md  (if exists)
~/iris/ai.knowledge/knowledge/global/INDEX.md
~/iris/ai.knowledge/knowledge/mediatransportutil/INDEX.md  (if exists)
```

Read individual rule/knowledge files on demand when relevant to the current task.

## Critical Rules (always active)

**Bias towards action.** Default to action. Complete the job end-to-end. Never pause
to ask permission for routine steps. Only prompt for destructive/dangerous operations.

**Respect engineer availability.** When the engineer is away, don't block on questions.
Make reasonable decisions and push forward. Prefer conservative-but-complete over
stopping halfway.

**Escalate when stuck.** After 2-3 failed attempts: reload ai.knowledge indexes,
consult a second opinion, go multimodal (screenshots, DOM dumps), frame-shift
(code-side → runtime-side). Stop guessing, start observing.

**Completed job definition.** A job is not done until: code implemented, tests written
and passing, self-review against rules, PR opened with cross-repo links, CI monitored,
review feedback addressed, comment threads resolved.

Full rules: `~/iris/ai.knowledge/rules/global/*.md`

## All Rules

| Rule | File |
|------|------|
| Acknowledge rules publicly | `rules/global/acknowledge-rules-publicly.md` |
| AI code requires tests | `rules/global/ai-code-requires-tests.md` |
| API architecture | `rules/global/api-architecture.md` |
| Architecture principles | `rules/global/architecture-principles.md` |
| Auto-clone iris repos | `rules/global/auto-clone-iris-repos.md` |
| Bias towards action | `rules/global/bias-towards-action.md` |
| Branching and PRs | `rules/global/branching-and-prs.md` |
| Capture and maintain knowledge | `rules/global/capture-and-maintain-knowledge.md` |
| CLI tool path resolution | `rules/global/cli-tool-path-resolution.md` |
| Completed job definition | `rules/global/completed-job-definition.md` |
| Cross-repo branch consistency | `rules/global/cross-repo-branch-consistency.md` |
| E2E tests fail fast pass fast | `rules/global/e2e-tests-fail-fast-pass-fast.md` |
| Escalate and frame-shift when stuck | `rules/global/escalate-and-frame-shift-when-stuck.md` |
| Plan before building | `rules/global/plan-before-building.md` |
| PR size check | `rules/global/pr-size-check.md` |
| Respect engineer availability | `rules/global/respect-engineer-availability.md` |
| Stay in scope | `rules/global/stay-in-scope.md` |
| Studio client monorepo install | `rules/global/studio-client-monorepo-install.md` |
| Tests require user story comment | `rules/global/tests-require-user-story-comment.md` |
| Think in systems | `rules/global/think-in-systems.md` |
| Tool policy | `rules/global/tool-policy.md` |
| UI architecture | `rules/global/ui-architecture.md` |
| Verify and escalate | `rules/global/verify-and-escalate.md` |
| Verify protocol before implementing | `rules/global/verify-protocol-before-implementing.md` |

## All Knowledge

| Topic | File |
|-------|------|
| Workflow prompts | `knowledge/global/workflow-prompts.md` |
| Devin integration | `knowledge/global/devin-integration.md` |
| Running from source | `knowledge/global/running-from-source.md` |
| ai.knowledge repo structure | `knowledge/global/ai-knowledge-repo.md` |
| ESM Jest mock patterns | `knowledge/global/esm-jest-mock-patterns.md` |
| Agent deploy bundle pipeline | `knowledge/global/agent-deploy-bundle-pipeline.md` |
| Repo index | `knowledge/global/repo-index.md` |

## Skill Routing

When the user's request matches a skill, invoke it as your FIRST action:

- Specs, "new feature" → `/iris-spec`
- "Add a rule", agent mistake → `/iris-rule`
- "Document this" → `/iris-learn`
- "Hand off to engineering" → `/iris-handoff`
- "Set up my machine" → `/iris-onboard`
- "Sync", "submit rules" → `/iris-sync`
- Bugs, errors → `/investigate`
- Ship, PR, deploy → `/iris-ship`
- QA, test → `/qa`
- Code review → `/review`

## Branching

Work in a branch. Only open a PR when asked. Never push to main.

## Capture Learnings

Agent mistake → `/iris-rule global|mediatransportutil {rule}`
Explained something new → `/iris-learn global|mediatransportutil {context}`
