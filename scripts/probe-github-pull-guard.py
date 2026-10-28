#!/usr/bin/env python3
"""Probe atomic guards using a PR's synthetic ref or same-repo head branch.

Creates and conditionally deletes two temporary branches in the selected repo.
The PR ref is included only as a no-op (beforeOid equals afterOid). GitHub may
reject it: this experiment does not authorize landing or test reviews, branch
protection, App permissions, webhook delivery, or fork repository behavior.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import sys
import time
import uuid

_spec = importlib.util.spec_from_file_location("sluss_cas_probe", Path(__file__).with_name("probe-github-cas.py"))
_cas = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cas)
api = _cas.api
QUERY = _cas.QUERY
ZERO = "0" * 40


def read(repo, path):
    response = api([f"repos/{repo}/git/ref/{path}"])
    if response.returncode:
        if "404" in response.stderr:
            return None
        raise RuntimeError(f"Failed reading {path}: {response.stderr.strip()}")
    data = json.loads(response.stdout)
    if data.get("ref") != f"refs/{path}" or data.get("object", {}).get("type") != "commit":
        raise RuntimeError(f"Unexpected ref response for {path}")
    value = data["object"].get("sha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", value) or value == ZERO:
        raise RuntimeError(f"Invalid commit for {path}")
    return value


def confirm_deleted(repo, path):
    # A ref API read can briefly lag an acknowledged GraphQL delete. Do not
    # resend the mutation; wait for two consecutive not-visible observations.
    absent = 0
    for attempt in range(6):
        absent = absent + 1 if read(repo, path) is None else 0
        if absent == 2:
            return True
        if attempt < 5:
            time.sleep(0.5)
    return False


def mutate(repo_id, updates, label):
    body = {"query": QUERY, "variables": {"input": {
        "repositoryId": repo_id, "clientMutationId": label, "refUpdates": updates,
    }}}
    response = api(["graphql", "--input", "-"], body)
    try:
        payload = json.loads(response.stdout)
    except json.JSONDecodeError:
        payload = {}
    ack = (response.returncode == 0 and not payload.get("errors") and
           ((payload.get("data") or {}).get("updateRefs") or {}).get("clientMutationId") == label)
    errors = payload.get("errors")
    messages = ([error.get("message", "")[:250] for error in errors[:3] if isinstance(error, dict)]
                if isinstance(errors, list) else [])
    return {"acknowledged": ack, "graphql_error": bool(errors), "messages": messages}


def run(repo, pull, base, candidate, guard="pull"):
    response = api([f"repos/{repo}"])
    if response.returncode:
        raise RuntimeError("Repository identity unavailable")
    repository = json.loads(response.stdout)
    if repository["full_name"].lower() != repo.lower():
        raise RuntimeError("Repository identity mismatch")
    repo_id = repository["node_id"]
    comparison = api([f"repos/{repo}/compare/{base}...{candidate}?per_page=1"])
    if comparison.returncode:
        raise RuntimeError("Could not check candidate ancestry")
    data = json.loads(comparison.stdout)
    if (data.get("status") != "ahead" or data.get("ahead_by", 0) <= 0 or
        data.get("base_commit", {}).get("sha") != base or
        data.get("merge_base_commit", {}).get("sha") != base):
        raise RuntimeError("Candidate is not a verified descendant of base")
    pull_path = f"pull/{pull}/head"
    head = read(repo, pull_path)
    if head is None:
        raise RuntimeError("Pull-request head is not visible")
    guard_path = pull_path
    if guard == "branch":
        response = api([f"repos/{repo}/pulls/{pull}"])
        if response.returncode:
            raise RuntimeError("Could not inspect the pull request")
        details = json.loads(response.stdout)
        source = details.get("head") or {}
        source_repo = source.get("repo") or {}
        target_repo = (details.get("base") or {}).get("repo") or {}
        branch = source.get("ref")
        if (details.get("number") != pull or details.get("state") != "open" or
                not isinstance(repository.get("id"), int) or source_repo.get("id") != repository["id"] or
                target_repo.get("id") != repository["id"] or source.get("sha") != head or
                not isinstance(branch, str) or len(branch) > 200 or
                not re.fullmatch(r"[A-Za-z0-9_./-]+", branch) or ".." in branch or "//" in branch or
                any(not part or part.startswith(".") or part.endswith(".lock") for part in branch.split("/")) or
                branch.endswith(".")):
            raise RuntimeError("PR head is not a stable same-repository branch")
        guard_path = f"heads/{branch}"
        if read(repo, guard_path) != head:
            raise RuntimeError("PR head and source branch do not agree")
    name = "sluss-pull-guard-probe-" + uuid.uuid4().hex
    path = f"heads/{name}"
    control = f"heads/{name}-control"
    try:
        for item in (path, control):
            created = api(["-X", "POST", f"repos/{repo}/git/refs", "--input", "-"],
                          {"ref": f"refs/{item}", "sha": base})
            if created.returncode or read(repo, item) != base:
                raise RuntimeError("Temporary branch creation was not confirmed")
        check = {"name": f"refs/{control}", "beforeOid": base, "afterOid": candidate, "force": False}
        control_result = mutate(repo_id, [check], f"{name}-control")
        if (not control_result["acknowledged"] or read(repo, control) != candidate or
                read(repo, path) != base or read(repo, pull_path) != head or read(repo, guard_path) != head):
            raise RuntimeError(f"Ordinary-ref control was not confirmed: {control_result}")
        print("PASS: ordinary branch compare-and-update worked with the same commits")
        target_update = {"name": f"refs/{path}", "beforeOid": base, "afterOid": candidate, "force": False}
        # ZERO-to-ZERO is a no-op even if the PR ref disappears concurrently.
        mismatch = {"name": f"refs/{guard_path}", "beforeOid": ZERO, "afterOid": ZERO, "force": False}
        wrong = mutate(repo_id, [target_update, mismatch], f"{name}-wrong")
        after_wrong = (read(repo, path), read(repo, pull_path), read(repo, guard_path))
        if wrong["acknowledged"] or not wrong["graphql_error"] or after_wrong != (base, head, head):
            raise RuntimeError(f"Wrong-head test was inconclusive: {wrong}, refs={after_wrong}")
        print("PASS: missing-ref precondition left target and PR head unchanged")
        # Both values are identical, so even a concurrently changed branch
        # could not be modified by this probe. The target is disposable.
        stale = base if head != base else candidate
        stale_guard = {"name": f"refs/{guard_path}", "beforeOid": stale, "afterOid": stale, "force": False}
        stale_result = mutate(repo_id, [target_update, stale_guard], f"{name}-stale")
        after_stale = (read(repo, path), read(repo, pull_path), read(repo, guard_path))
        if stale_result["acknowledged"] or not stale_result["graphql_error"] or after_stale != (base, head, head):
            raise RuntimeError(f"Stale-head test was inconclusive: {stale_result}, refs={after_stale}")
        print("PASS: stale head SHA left target and PR head unchanged")
        matching = {"name": f"refs/{guard_path}", "beforeOid": head, "afterOid": head, "force": False}
        result = mutate(repo_id, [target_update, matching], f"{name}-matching")
        final = (read(repo, path), read(repo, pull_path), read(repo, guard_path))
        if result["acknowledged"] and final == (candidate, head, head):
            print("PASS: target advanced with the expected PR head included as a no-op")
            return True
        print("INCONCLUSIVE: matching PR-ref guard did not produce a confirmed atomic update:",
              json.dumps({"result": result, "refs": final}))
        return False
    finally:
        failures = []
        for index, item in enumerate((path, control)):
            try:
                current = read(repo, item)
                if current is not None:
                    if current not in (base, candidate):
                        raise RuntimeError("unexpected branch value; refusing deletion")
                    deletion = {"name": f"refs/{item}", "beforeOid": current, "afterOid": ZERO, "force": True}
                    mutate(repo_id, [deletion], f"{name}-delete-{index}")
                if not confirm_deleted(repo, item):
                    raise RuntimeError("conditional deletion not confirmed")
                print("Cleanup confirmed:", item)
            except Exception as error:
                failures.append((item, str(error)))
        if failures:
            print("Cleanup incomplete:", json.dumps(failures), file=sys.stderr)
            raise RuntimeError("Inspect the temporary branches manually")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--pull", required=True, type=int)
    parser.add_argument("--guard", choices=("pull", "branch"), default="pull",
                        help="branch is limited to PRs whose source is in the target repository")
    parser.add_argument("--base", required=True)
    parser.add_argument("--candidate", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repo) or any(
        part in (".", "..") for part in args.repo.split("/")
    ):
        parser.error("invalid repository")
    if not 0 < args.pull < 2**31:
        parser.error("invalid pull request number")
    if any(not re.fullmatch(r"[0-9a-f]{40}", sha) or sha == ZERO for sha in (args.base, args.candidate)) or args.base == args.candidate:
        parser.error("commits must be distinct full lowercase SHA-1 IDs")
    if not run(args.repo, args.pull, args.base, args.candidate, args.guard):
        sys.exit(1)


if __name__ == "__main__":
    main()
