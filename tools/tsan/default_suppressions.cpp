/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Compiled-in ThreadSanitizer default suppressions.
 *
 * Why a compiled-in hook instead of only a runtime suppression file:
 *   - The installed datasystem_worker / datasystem_coordinator binaries run
 *     under TSAN on hosts where the repo's tools/tsan/brpc_suppressions.txt
 *     is not present (e.g. wheel-installed packages). A compiled-in default
 *     guarantees the third-party (brpc / bthread) suppression travels with
 *     the binary.
 *   - The TSAN runtime calls __tsan_default_suppressions() exactly once at
 *     process startup if it is defined and weakly linked. Under non-TSAN
 *     builds the symbol is simply unused (no TSAN runtime to call it), so
 *     linking this translation unit is harmless.
 *
 * What is suppressed:
 *   Only third-party (brpc / bthread) init-time and M:N-scheduler races
 *   that TSAN's happens-before model cannot precisely model but which are
 *   safe under brpc's own atomicity discipline. See brpc_suppressions.txt
 *   for the same entries in runtime-file form with full justification.
 *
 * What is NOT suppressed:
 *   Anything in the datasystem::* namespace. Real races in datasystem
 *   code remain reported by TSAN.
 *
 * Extending:
 *   Add a new "race:..." line both here and in brpc_suppressions.txt so
 *   the file-based and compiled-in paths stay in sync.
 */

// Symbol name is mandated by the TSAN runtime (looked up at process startup
// via weak-symbol/dlsym). It cannot be renamed despite G.EXP.01-CPP
// (reserved identifier) and G.NAM.03-CPP (CamelCase) false positives.
extern "C" const char *__tsan_default_suppressions()  // NOLINT
{
    // NOTE: TSAN parses the returned buffer line-by-line. Entries use the
    // same syntax as the suppressions file. Do not add C++ comments inside
    // the returned string — TSAN does not strip them.
    return
        "race:bthread::TaskGroup::ready_to_run_remote\n"
        "race:bthread::TaskGroup::*\n"
        "race:bthread::TaskControl::*\n"
        "race:bthread::TaskMeta::*\n";
}
