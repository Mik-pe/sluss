#!/usr/bin/env python3
"""Probe GitHub's concurrent three-ref compare-and-update on disposable branches.

Requires the GitHub CLI and a credential with permission to create/delete refs.
This changes the specified repository temporarily and may trigger workflows.
It is provider evidence only; it does not test App bypass, reviews, or landing.
"""
import argparse
import concurrent.futures
import json
import re
import subprocess
import sys
import threading
import uuid

QUERY = "mutation($input: UpdateRefsInput!) { updateRefs(input: $input) { clientMutationId } }"


def api(arguments, body=None):
    return subprocess.run(
        ["gh", "api", *arguments],
        input=None if body is None else json.dumps(body),
        text=True,
        capture_output=True,
        timeout=35,
        check=False,
    )


def get_ref(repo, name):
    response = api([f"repos/{repo}/git/ref/heads/{name}"])
    if response.returncode:
        if "404" in response.stderr:
            return None
        raise RuntimeError(f"Could not read {name}: {response.stderr.strip()}")
    return json.loads(response.stdout)["object"]["sha"]


def update(repo_id, prefix, target, stage, marker, base, candidate, label, barrier):
    mutation_id = f"{prefix}-{label}"
    request = {
        "query": QUERY,
        "variables": {
            "input": {
                "repositoryId": repo_id,
                "clientMutationId": mutation_id,
                "refUpdates": [
                    {"name": f"refs/heads/{target}", "beforeOid": base,
                     "afterOid": candidate, "force": False},
                    {"name": f"refs/heads/{stage}", "beforeOid": candidate,
                     "afterOid": candidate, "force": False},
                    {"name": f"refs/heads/{marker}", "beforeOid": base,
                     "afterOid": candidate, "force": False},
                ],
            }
        },
    }
    barrier.wait(timeout=10)
    response = api(["graphql", "--input", "-"], request)
    try:
        payload = json.loads(response.stdout)
    except json.JSONDecodeError:
        payload = {}
    update_result = (payload.get("data") or {}).get("updateRefs") or {}
    return {
        "label": label,
        "acknowledged": response.returncode == 0 and update_result.get("clientMutationId") == mutation_id,
        "graphql_error": bool(payload.get("errors")),
        "candidate": candidate,
    }


def run(repo, base, first, second):
    repository = api([f"repos/{repo}"])
    if repository.returncode:
        raise RuntimeError(f"Could not read repository: {repository.stderr.strip()}")
    repo_info = json.loads(repository.stdout)
    if repo_info["full_name"].lower() != repo.lower():
        raise RuntimeError("Repository identity differs from the requested repository")
    repo_id = repo_info["node_id"]
    # Both contenders must be valid fast-forwards. Otherwise a losing update
    # might simply have been rejected for ancestry rather than the stale CAS.
    for candidate in (first, second):
        comparison = api([f"repos/{repo}/compare/{base}...{candidate}?per_page=1"])
        if comparison.returncode:
            raise RuntimeError(f"Could not check ancestry: {comparison.stderr.strip()}")
        data = json.loads(comparison.stdout)
        if (data.get("status") != "ahead" or data.get("ahead_by", 0) <= 0
                or data.get("base_commit", {}).get("sha") != base
                or data.get("merge_base_commit", {}).get("sha") != base):
            raise RuntimeError(f"{candidate} is not a verified descendant of {base}")
    prefix = "sluss-concurrent-probe-" + uuid.uuid4().hex
    target, stage_a, stage_b, marker_a, marker_b = (f"{prefix}-{suffix}" for suffix in
        ("target", "stage-a", "stage-b", "marker-a", "marker-b"))
    names = (target, stage_a, stage_b, marker_a, marker_b)
    allowed = {target: {base, first, second}, stage_a: {first}, stage_b: {second},
               marker_a: {base, first}, marker_b: {base, second}}
    cleanup_failed = []
    try:
        for name, sha in zip(names, (base, first, second, base, base)):
            result = api(["-X", "POST", f"repos/{repo}/git/refs", "--input", "-"],
                         {"ref": f"refs/heads/{name}", "sha": sha})
            if result.returncode:
                raise RuntimeError(f"Creation of {name} was not confirmed: {result.stderr.strip()}")
            if get_ref(repo, name) != sha:
                raise RuntimeError(f"Initial value of {name} differs")
        # A wrong marker precondition must reject the whole mutation, including
        # the otherwise valid target update. A GraphQL error alone is not proof.
        wrong_marker = {"query": QUERY, "variables": {"input": {
            "repositoryId": repo_id, "clientMutationId": f"{prefix}-wrong-marker",
            "refUpdates": [
                {"name": f"refs/heads/{target}", "beforeOid": base, "afterOid": first, "force": False},
                {"name": f"refs/heads/{stage_a}", "beforeOid": first, "afterOid": first, "force": False},
                {"name": f"refs/heads/{marker_a}", "beforeOid": second, "afterOid": first, "force": False},
            ],
        }}}
        rejected = api(["graphql", "--input", "-"], wrong_marker)
        try:
            rejection = json.loads(rejected.stdout)
        except json.JSONDecodeError:
            rejection = {}
        if not rejection.get("errors") or [get_ref(repo, name) for name in names] != [base, first, second, base, base]:
            raise RuntimeError("Wrong marker precondition did not produce an atomic rejection")
        print("PASS: wrong marker precondition left all five refs unchanged")
        barrier = threading.Barrier(2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            tasks = [
                executor.submit(update, repo_id, prefix, target, stage_a, marker_a, base, first, "a", barrier),
                executor.submit(update, repo_id, prefix, target, stage_b, marker_b, base, second, "b", barrier),
            ]
            results = [task.result(timeout=45) for task in tasks]
        refs = [get_ref(repo, name) for name in names]
        print("Results:", json.dumps(results))
        print("Final refs:", json.dumps(refs))
        winners = [result for result in results if result["acknowledged"]]
        if len(winners) != 1:
            raise RuntimeError("Expected exactly one acknowledged update")
        expected = ([first, first, second, first, base] if winners[0]["label"] == "a"
                    else [second, first, second, base, second])
        if refs != expected:
            raise RuntimeError("Target, staging refs or per-operation markers differ from the atomic result")
        loser = next(result for result in results if result is not winners[0])
        if not loser["graphql_error"]:
            raise RuntimeError("The other result was not a GraphQL error; outcome is uncertain")
        print("PASS: exact winner SHA and marker; loser marker and both staging refs unchanged")
    finally:
        for name in names:
            try:
                current = get_ref(repo, name)
                if current is None:
                    continue
                if current not in allowed[name]:
                    raise RuntimeError("Ref changed unexpectedly; refusing deletion")
                # Delete only if the ref still has the value just observed.
                # GitHub's REST delete endpoint has no expected-old-value field.
                deletion = {"query": QUERY, "variables": {"input": {
                    "repositoryId": repo_id,
                    "clientMutationId": f"{prefix}-delete-{name.removeprefix(prefix + '-')}",
                    "refUpdates": [{"name": f"refs/heads/{name}", "beforeOid": current,
                                    "afterOid": "0" * 40, "force": True}],
                }}}
                response = api(["graphql", "--input", "-"], deletion)
                remaining = get_ref(repo, name)
                if remaining is not None:
                    raise RuntimeError(f"Deletion not confirmed; ref now at {remaining}; {response.stderr.strip()}")
                print("Cleanup confirmed:", name)
            except Exception as error:
                cleanup_failed.append((name, str(error)))
        if cleanup_failed:
            print("Cleanup incomplete:", json.dumps(cleanup_failed), file=sys.stderr)
            raise RuntimeError("Inspect the listed refs before cleaning them up")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="GitHub repository as owner/name")
    parser.add_argument("--base", required=True, help="Full base commit SHA")
    parser.add_argument("--first", required=True, help="Full descendant commit SHA")
    parser.add_argument("--second", required=True, help="Another full descendant commit SHA")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repo):
        parser.error("invalid repository")
    if any(part in (".", "..") for part in args.repo.split("/")):
        parser.error("invalid repository")
    if any(not re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (args.base, args.first, args.second)):
        parser.error("commits must be full lowercase SHA-1 IDs")
    if len({args.base, args.first, args.second}) != 3:
        parser.error("the three commits must be distinct")
    run(args.repo, args.base, args.first, args.second)


if __name__ == "__main__":
    main()
