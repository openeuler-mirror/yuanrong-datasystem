# Playbooks

`playbooks/` is the task-oriented layer of `.repo_context/`.

Use a playbook when the question is not “what is this module?” but “how should I work in this repo for this task type?”

## Structure

```text
playbooks/
├── README.md
├── operations/
│   ├── README.md
│   ├── incident-triage.md
│   └── performance-investigation.md
└── features/
    ├── README.md
    └── infra/
        ├── README.md
        └── logging/
            ├── README.md
            └── implementation.md
```

## Standardization Rules

- use `.repo_context/templates/feature-playbook-template.md` for new feature playbooks;
- each non-trivial feature should either reuse an existing feature playbook or create/update a matching one;
- if a feature changes compatibility-sensitive behavior, shared infra behavior, or cross-module contracts, the playbook is required rather than optional;
- playbooks should record both the implementation steps and the risk gates that decide when to stop and escalate.

## Current Status

This layer is now partially seeded. General workflow still lives in:

- `maintenance.md`
- `modules/quality/tests-and-reproduction.md`
- `modules/quality/build-test-debug.md`

Task-oriented guidance now starts in:

- `operations/README.md`
- `operations/incident-triage.md`
- `operations/performance-investigation.md`
- `features/README.md`
- `features/infra/README.md`
- `features/infra/logging/README.md`
- `features/infra/logging/implementation.md`

## Planned Playbooks

- `bugfix-reproduction.md`
- `code-review-checklist.md`
- `context-update-rules.md`
- more `features/*.md` playbooks for compatibility-sensitive modules
- more `operations/*.md` playbooks for deployment, rollout, and recovery workflows

## Rule Of Thumb

- module docs answer ownership and structure questions
- playbooks answer workflow and execution questions
