**What type of PR is this?**

MUST be one of the following:

/kind fix (A bug fix)
/kind feat (A new feature)
/kind perf (A code change that improves performance)
/kind chore (A code change that modifies non-business-critical code)
/kind style (A code change that improves the meaning of the code)
/kind revert (A code change that rolls back specific PR modifications)
/kind refactor (A code change that neither fixes a bug nor adds a feature)
/kind ci (Changes to our CI configuration files and scripts)
/kind test (Adding missing tests or correcting existing tests)
/kind docs (Documentation only changes)
/kind build (Changes that affect the build system or external dependencies)

----

## 1. Background / Symptom

<!--
  Feature/Refactor: Describe the problem to solve, existing defects, or requirement source. Reference related issues or prior PRs.
  Bugfix: Describe specific test names / error logs / failure rates / impact scope. List multiple issues separately.
-->



## 2. Design / Solution

<!--
  Describe the technical approach. Mermaid class diagrams or sequence diagrams are REQUIRED when multiple components interact.
  Include a file change checklist table.
  Bugfix: clearly state whether the issue is PR-introduced or pre-existing.
-->

**File Change Checklist:**

| File | Description |
|------|-------------|
| | |

## 3. Verification Plan

<!-- Must cover three dimensions -->

- **Build**: bazel + cmake both pass (remote environment, jobs configuration)
- **Deploy**: Specific steps to deploy to test environment (dscli / config files / ETCD address), verify basic connectivity
- **Test**: New test cases + regression matrix (choose mode combinations as needed), specify baseline comparison (master baseline vs PR HEAD)

## 4. Verification Results

<!-- Build output, test result tables, post-deploy status check screenshots, connectivity verification output. MUST include concrete numbers. "Verified OK" alone is not acceptable. -->

## 5. Follow-up Items (if any)

<!-- Work not included in this PR, planned for subsequent PRs. Bugfixes without leftovers may delete this section. -->

## 6. Self-Checklist

<!--
  Check off each applicable item below.
  Remove dimensions or items that do not apply — do not leave them in the PR description.
  Code conventions (English comments, functions ≤50 lines, include order, chrono truncation, orphan braces, no Co-Authored-By)
  are checked during local review and do NOT appear here.
-->

### Design Review
- [ ] Design has been approved by Maintainer, all review feedback addressed

### Build
- [ ] bazel build passes (remote environment, `--config=test`)
- [ ] cmake build passes (remote environment, `-j` configured)
- [ ] New BUILD.bazel + CMakeLists.txt files have been added

### Correctness
- [ ] New functionality verified item by item (happy path + error path + edge cases)
- [ ] Changes cover all call sites / subclass implementations
- [ ] Crash consistency: partial write / dirty data recovery paths verified
- [ ] Idempotency: repeated operations produce no side effects
- [ ] Failover: state correctly recovered after failover
- [ ] New UT/ST test cases committed with this PR (explain if none)

### Memory
- [ ] New memory allocation strategy is reasonable (leak / pool / RAII)
- [ ] No double-free / dangling pointers / double reset

### Concurrency
- [ ] Lock ordering is consistent, no deadlock risk
- [ ] Reference/pointer member lifetimes are safe (no dangling references)
- [ ] No pthread mutex on critical paths (prefer bthread::Mutex / std::mutex / lock-free structures)

### Performance
- [ ] No noticeable regression on read/write/query critical paths (benchmark vs master baseline)
- [ ] No abnormal memory / FD / thread count growth
- [ ] Resource-constrained scenarios tested (OOM / FD exhaustion / connection limit)

### Security
- [ ] External inputs validated (length, range, format)
- [ ] No sensitive data written to logs (keys, tokens, user data)
- [ ] New dependencies have no known CVEs

### Observability
- [ ] New metrics and alert rules added for new functionality
- [ ] Key error paths have ERROR logs with diagnostic context

### Logging
- [ ] ERROR/WARNING logs include context for diagnostics
- [ ] No LOG(FATAL) causing process exit (use ERROR + graceful degradation on cleanup paths)

### Forward Compatibility
- [ ] client ↔ worker: cross-version compatibility
- [ ] worker ↔ worker: cross-version compatibility
- [ ] worker ↔ coordinator: cross-version compatibility

### API / Interface Changes
- [ ] C++ SDK: No change / Has change (list + supplement docs)
- [ ] Python SDK: No change / Has change (list + supplement docs)
- [ ] Java SDK: No change / Has change (list + supplement docs)
- [ ] Environment variables: No change / Has change (list)
- [ ] Worker deploy parameters (dscli start args): No change / Has change (list)
- [ ] build.sh compile parameters: No change / Has change (list)
- [ ] bazel compile parameters: No change / Has change (list)
- [ ] Documentation to supplement: None / Has (list doc paths)

----

Fixes #

----

## Mermaid Diagram Notes

GitCode mermaid version limitations:

**Forbidden syntax**:
- `note for` / `Note over` — not supported; use `<<stereotype>>` instead (class diagrams) or use messages inside alt branches (sequence diagrams)
- `::` in participant names — parser misinterprets as namespace qualifier; replace with space
- `()` in alt/else labels — parser misinterprets as function call; replace with `-`

**Message text restrictions**:
- Forbidden: `()` `{}` `<>` `[]` bracket characters
- Forbidden: em dash `—` and other Unicode special characters; use `-` or `:` instead
- Keep messages short and in English

**Arrow syntax**:
- Prefer `->>` solid arrows; `-->>` dashed arrows may render incorrectly in older mermaid versions
- Class diagram relations: `--|>` (inheritance) and `..|>` (implementation)
- No generic type parameters in class diagrams; describe with text instead

----
