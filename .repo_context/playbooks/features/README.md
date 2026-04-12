# Feature Playbooks

`playbooks/features/` contains low-risk implementation workflows for concrete feature classes.

Use a feature playbook when the task is not only "where should I edit?" but also "what should I verify before and after I edit so the change stays accurate and low risk?"

Rules:

- prefer one playbook per recurring feature class or compatibility-sensitive change area;
- each non-trivial feature should either reuse an existing playbook or create/update a matching one;
- if a feature crosses module boundaries or changes compatibility-sensitive behavior, treat the playbook as required, not optional.

Current docs:

- `infra/README.md`
