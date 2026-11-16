# AGENTS.md — Repository Agent Instructions (Source of Truth)

This file defines the canonical coding directives for this repository. Shared guidance is adapted from the [script-project template](https://github.com/birkin/birkin_coding_tools/blob/main/script_project/AGENTS.md), with repository-specific details below.

Keep this `AGENTS.md` file at no more than 300 lines, counting blank lines and the final `---`. When adding guidance, shorten or remove repeated material first; keep repository-specific instructions and the project index useful.

General coding directives are appropriate here when they help different developers and agents follow consistent conventions, since contributors may not share global instructions. Keep useful shared guidance within the 300-line limit.

If other instruction files exist (Copilot, IDE rules, contributor docs) and conflict with this file, follow this file and treat the others as stale.


## Table of contents

- [Project basics](#project-basics)
- [How to run code](#how-to-run-code)
- [Coding directives (Python)](#coding-directives-python)
- [Tests](#tests)
- [Change workflow expectations](#change-workflow-expectations)
- [Privacy and publication](#privacy-and-publication)
- [If instructions are missing or ambiguous](#if-instructions-are-missing-or-ambiguous)
- [Agent project index](#agent-project-index)


## Project basics

- Purpose: builds Solr update documents from repository objects and processes indexing jobs through RQ/Redis.
- Primary language: Python. This is a script and worker application, with no Django application or browser interface.
- Target runtime: Python 3.8; `pyproject.toml` requires `>=3.8.9,<3.9`.
- Dependency / execution tool: `uv`; keep `uv.lock` consistent with `pyproject.toml`.
- Project root: the directory containing this file, `.git/`, and `pyproject.toml`. The enclosing stuff directory is not the code root.


## How to run code

- Assume user is in the project-root directory.
- Do not use `python` to run scripts.
- Run a script via: `uv run ./path_to_script.py --help`
- Install the locked local dependencies with `uv sync --locked --group local`.
- Run tests with `uv run ./run_tests.py`. The runner supplies test settings, creates temporary directories, and discovers the full `unittest` suite; it does not currently accept individual test names.
- For a standalone Python helper with a missing package, use `uv run --no-project --with PACKAGE python SCRIPT ARGS`. Do not add temporary validation packages to the application dependencies or install them globally.


## Coding directives (Python)

### Type hints and imports

- Use Python 3.8-compatible type hints for functions and important variables, following the version in `pyproject.toml`.
- Use `typing.List` / `typing.Dict` instead of builtin generics such as `list[str]` / `dict[str, int]`.
- Use `Optional` / `Union` instead of PEP 604 unions such as `str | None`; import from `typing` as needed.
- Keep these Python 3.8 conventions when a linter suggests newer syntax. Report conflicts rather than suppressing rules or changing the runtime requirement.

### Script structure

- Structure runnable modules as:
  - `def main() -> None: ...`
  - `if __name__ == '__main__': main()`
- Keep `main()` simple: parse args / orchestrate calls only.
- Put real logic into top-level helper functions and modules (no nested function definitions).
- Rarely use more than three levels of hierarchy: main() can call helper_A() which can call helper(B) which can, if necessary, can call helper(C) -- but that's it.

### Functions and control flow

- Prefer single-return functions (use local variables and a final return).
- Do not define functions inside other functions.
- Favor clarity and explicitness over cleverness.

### Logging

- When adding a log statement, when possible, format variable values as a label, followed by a comma and a space, with the value enclosed in double backticks.
- Prefer a label that matches the variable name. For example: ```log.debug(f'branch_and_commit, ``{branch_and_commit}``')```

### HTTP and networking

- Use `httpx` for all HTTP calls.
- Do not introduce alternate HTTP libraries (e.g., `requests`, `aiohttp`) unless the repository already depends on them and there is a documented reason.

### Docstrings

- Use triple-quoted docstrings.
- Write docstrings in present tense, with triple-quotes on their own lines.
  - Good:
    ```
    """
    Parses ...
    """
    ```
  - Avoid: `"""Parse ..."""`
- The last line of non-test function-docstrings should be: `Called by: the_caller_function()` (or, if in another class/module, `Called by: module.Class.the_caller_function()`)
- Start test-function docstring-text with "Checks..."
- For header-comments, in functions, start the comment with two hashes (e.g., `## does this`).

### Formatting and checks

- Read the repository-root `ruff.toml`: Python 3.8, 125-character lines, four-space indentation, and single quotes.
- Check the project environment and `command -v ruff` before downloading Ruff. If needed, discover the installed VS Code Ruff extension's bundled executable and run it from the project root with this repository's configuration.
- Before finishing Python changes, check every changed Python file with Pylance using the project interpreter and type-checking settings. Use Pyright if Pylance is unavailable. Fix errors with simple annotations and explicit checks for `None` or the expected type; do not suppress diagnostics.
- Local searches may use `rg`. Commands suggested for older servers must use standard tools such as `grep` or `find`.

### Markdown formatting

- Do not use hard line-breaks in markdown files; let paragraphs wrap naturally.
- When creating a Markdown file with more than three top-level `##` headings, add a table of contents near the top with links to those `##` headings.


## Tests

- Use the standard library `unittest` framework, not pytest.
- Use test substitutes or disposable local services with sample data for Redis and HTTP checks. Do not run maintenance scripts against existing jobs as a test.
- New behavior should usually come with a focused test covering:
  - the happy path
  - at least one failure / edge case


## Change workflow expectations

When implementing a change (especially from an issue/task):

1. Read relevant surrounding code and match existing conventions.
2. Make the smallest correct change that satisfies the request.
3. Update tests and run: `uv run ./run_tests.py`
4. If you cannot run tests in your environment, still write/adjust tests and state what you would run.

### Issue-based work and review

- Work directly from the current user request. Issues, formal templates, labels, preliminary discussions, and decision comments are not prerequisites for authorized local work.
- When issue-based work is authorized, organize each issue around one clear outcome and create a branch for its file changes. Include the issue number and a short description in the branch name, and record it in work reports. A request to change local files does not by itself authorize creating an issue or posting comments.
- Save requested plans, documentation, and code changes locally, leaving them uncommitted for the user's review unless the user explicitly requests a commit. Preserve the user's manual edits during revisions.
- Report the files changed, checks actually performed, and anything needing review. Distinguish work ready for review from work accepted by the user, and distinguish local, committed, and pushed changes. Link existing issues, commits, and pull requests when relevant.
- Keep the issue open for review and iteration. A finished draft or implementation report does not mean the user has accepted the work or wants the issue closed.

### GitHub attribution

- Every GitHub post or text update must visibly identify Codex as the agent that created or edited it. This includes issue descriptions, pull-request descriptions, comments, reviews, and discussions; do not rely on the displayed account name to convey authorship.
- Begin new issue descriptions with `Created by Codex at the user's request.` Keep this attribution separate from the user's prompt. For other posts or edits, use an accurate visible attribution such as `Posted by Codex` or `Edited by Codex`; begin issue comments with `Codex response` as described below.
- Distinguish who posted the material from who wrote it: identify quoted prompts as the user's words, and identify Codex's summaries, proposals, and reports as Codex's work.

### Issue descriptions

- For authorized repository posts, write a fresh, minimal summary of the requested behavior, changes, and checks. Do not paste private conversation excerpts, raw logs, tracebacks, or configuration.
- When asked to draft an issue, use **Goal**, **Context**, and **Tasks** where helpful. Add **Completion criteria** only when observable checks would clarify the outcome. Keep the structure proportional to the work.
- Use a structured body argument when available, or a temporary file with `--body-file` when using `gh`. After posting or editing, fetch the issue to verify the text and visible attribution, then return its link.

### GitHub issue comments

- Post a comment only when the user asks or has already authorized it. Authorization to maintain prompt and work records for an issue can cover later updates within that scope. A request to implement a change does not by itself authorize a comment, and a request to comment does not by itself authorize implementation or commits.
- Before posting, read the target issue, all its comments, and applicable `AGENTS.md` files. Address the current request within its stated scope; use newer maintainer guidance to resolve older conflicting comments.
- Begin comments with `Codex response` and identify the response type, such as **answer**, **advice**, **proposal**, **prompt record**, or **implementation report**. Clearly distinguish an agent proposal from an accepted maintainer decision.
- Use authorized comments to summarize substantive work at useful milestones; every local exchange does not need a GitHub update. Implementation reports should describe what changed, what was verified, any remaining work or review, and whether changes are local, committed, or pushed. Posting a report does not authorize a commit or issue closure.
- Use a structured comment-body argument when available. If using `gh`, put multiline Markdown in a temporary file and pass it with `--body-file`. Verify the posted text and return its direct link. If a posting attempt has an uncertain result, check existing comments before retrying to avoid duplicates.

### Commit authorization

- Create or amend a commit only when the user explicitly asks Codex to commit the changes in question. This applies to Git commands and equivalent tools or APIs. A request to develop a plan, implement a change, save files, create a branch, post a summary, or finish the work does not authorize a commit.
- Review approval, a suggested commit message, or the user saying they might commit the work is not an instruction for Codex to commit. Commit-message conventions describe how to write an authorized commit; they do not grant permission to make one.
- Apply an explicit commit instruction only to its stated changes and scope. Permission for an earlier task or commit does not automatically cover later revisions. Do not ask again when the current changes are already covered by clear authorization.
- If commit authorization is absent or unclear, finish the authorized local work and report that it is ready for review and uncommitted. Do not delay that work to ask whether to commit.
- Permission to commit does not by itself authorize pushing, creating or merging a pull request, or closing an issue. Follow the user's instructions for each action separately.

### Issue closure

- Only the user closes issues unless the user specifically asks Codex to close an identified issue. Keep issues open by default, even after requested work, tests, review, commits, pushes, or merges are complete. A request to finish the task or approval of a plan is not permission to close the issue.
- Without that specific request, do not close issues through the UI, CLI, API, tools, or a comment-and-close action. Do not arrange automatic closure through commit messages, pull-request descriptions, links, or automation.
- Use ordinary references such as `Refs #123` or an issue URL unless closure is authorized. Do not use closing keywords such as `Closes`, `Fixes`, or `Resolves` with an issue reference or add links that close the issue when merged. Before an authorized merge, check for existing automatic closure instructions and links; remove them if authorized or leave the merge pending if it would close an issue without permission.
- For planning work, develop and save the plan locally, post a summary if authorized, and leave the changes uncommitted and the issue open for review. Committing, continuing revisions, and closing the issue are separate decisions.

### Commit messages

- Apply these conventions only after the user has authorized a commit under [Commit authorization](#commit-authorization).
- Group related files into logical, focused commits; do not require a separate commit for every file.
- Keep each commit message brief, with no more than fifteen words.
- Write messages in the present tense so they complete the phrase "This commit..." Begin with a fitting verb such as "Adds," "Implements," or "Updates."


## Privacy and publication

- Apply these rules to public and private repositories, including tracked files, agent notes, issue titles and bodies, comments, pull requests, commit messages, and attachments. Permission to investigate using conversation, local files, or tool output is not permission to publish that information.
- Do not publish explicit server names, hostnames, server IP addresses, credentials, tokens, private endpoints, personal information, cookies, session data, or unreviewed browser artifacts. Use generic descriptions and relative paths or variable names instead of full local or server filesystem paths.
- Keep sensitive working notes out of tracked files. Do not publish known or suspected vulnerabilities, affected live systems, exploit steps, or details that could help someone exploit a weakness. Discuss findings privately with the user; describe repository updates in terms of the general improvement and safe validation results.
- Before every repository post or edit, review the exact outgoing text, examples, links, screenshots, and attachments for sensitive information. Check combinations of details as well as individual values. Information already present in source code or an earlier post is not automatic permission to repeat it.
- When posting is authorized and the complete content is clearly safe to publish, proceed without another approval request. If sensitivity is uncertain, prepare sanitized wording, show it in the private conversation, explain the uncertainty without repeating sensitive values, and wait for confirmation of that exact text before posting. Never use an issue or comment to ask whether sensitive information is safe to disclose.
- Keep full server filesystem paths out of documentation, examples, and agent notes. Keep all server-deployment documentation, including any mention of deployment caller scripts, outside READMEs.


## If instructions are missing or ambiguous

- Do not ask questions unless absolutely necessary to proceed.
- Make reasonable assumptions, state them explicitly, then implement.
- Do not assume permission to commit or close an issue. When that permission is absent or unclear, complete the authorized local work, leave it uncommitted, and keep the issue open as described above.
- If blocked, provide:
  - what you tried
  - what you found in the repo
  - a concrete next step (command, file to edit, or minimal decision needed)


## Agent project index

- `queue_solrize.py`: parses PIDs, action, and priority; queues jobs and stops further submissions when the total failure count exceeds fifty.
- `bdr_solrizer/queues.py`: selects the high, medium, or low queue and enqueues `bdr_solrizer.solrizer.solrize` with a 2,880-second timeout.
- `start_workers.py`: loads settings and starts five workers. Create each worker and its Redis connection inside the child process, after the fork; include construction in the Redis-loading retry.
- `bdr_solrizer/failed_jobs.py`: discovers per-queue failure registries, counts failures, and supports review, requeueing, and deletion. `process_failed_queue.py` prompts for actions; `reque_failed_queue.py` requeues current failures.
- RQ compatibility: RQ 1.16.2 uses `FailedJobRegistry`; do not restore `get_failed_queue` or a global `FAILED_Q`. Old-format failed jobs are counted and reported but require separate migration. Deleting a current job also removes its separate result history.
- `clear_zip_queue.py`: empties the queue named by `BDR_INDEXER__REDIS_QUEUE_NAME`; it changes Redis data.
- `bdr_solrizer/solrizer.py`: handles indexing actions, posts updates to Solr, and queues related-object work.
- `bdr_solrizer/solrdocbuilder.py`: contains `StorageObject` for reading object data, `SolrDocBuilder` for assembling fields, and `ZipIndexer` for ZIP contents.
- `bdr_solrizer/indexers/`: metadata-specific readers for MODS, Darwin Core, TEI, FITS, rights, relationships, collections, and related formats. Shared constants and helpers are in `indexers/common.py`.
- Metadata fallback: some descriptive metadata can come from related objects. Image accessibility alt text must come from the object being indexed; preserve the direct-source checks in `solrdocbuilder.py`.
- `bdr_solrizer/settings.py` reads required environment variables during import. `bdr_solrizer/logger.py` creates file handlers during import. Load settings before importing dependent application modules; use `run_tests.py` for the test environment.
- `csv_to_sqlite.py`: creates the resource-type lookup database. `bdr_solrizer/utils.py` handles dates and Solr date strings.
- `tests/unit/`: indexer and Solrizer tests; `test_data.py` contains sample metadata. `test_rq_compatibility.py` covers failure handling, submission limits, and worker startup.
- `pyproject.toml` and `uv.lock` define dependencies. `.github/workflows/` defines CI setup and tests. Existing `PLAN__*.md` files provide background; verify current behavior in the code before relying on them.

---
