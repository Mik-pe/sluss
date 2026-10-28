"""No provider calls: exercise the disposable pull-ref guard probe's safeguards."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("sluss_pull_probe", Path(__file__).parents[1] / "scripts" / "probe-github-pull-guard.py")
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
BASE, CANDIDATE, HEAD, FOREIGN = (letter * 40 for letter in "abcd")


class FakeGitHub:
    def __init__(self, *, reject_matching=False, wrong_ancestry=False, change_before_cleanup=False, stale_after_delete=False, fail_control=False, fork=False):
        self.refs = {"pull/12/head": HEAD, "heads/feature": HEAD}
        self.fork = fork
        self.reject_matching = reject_matching
        self.wrong_ancestry = wrong_ancestry
        self.fail_control = fail_control
        self.change_before_cleanup = change_before_cleanup
        self.guard_updates = []
        self.deletions = []
        self.stale_after_delete = stale_after_delete
        self.stale = {}

    @staticmethod
    def result(body, code=0, stderr=""):
        return SimpleNamespace(stdout=json.dumps(body), stderr=stderr, returncode=code)

    def __call__(self, args, body=None):
        endpoint = next((a for a in args if a.startswith("repos/") or a == "graphql"), "")
        if endpoint == "repos/owner/repo":
            return self.result({"full_name": "owner/repo", "node_id": "R_repo", "id": 1})
        if endpoint == "repos/owner/repo/pulls/12":
            return self.result({"number": 12, "state": "open", "head": {"sha": HEAD, "ref": "feature", "repo": {"id": 2 if self.fork else 1}}, "base": {"repo": {"id": 1}}})
        if "/compare/" in endpoint:
            return self.result({"status": "diverged" if self.wrong_ancestry else "ahead", "ahead_by": 1,
                                "base_commit": {"sha": BASE}, "merge_base_commit": {"sha": BASE}})
        if endpoint.endswith("/git/refs"):
            self.refs[body["ref"].removeprefix("refs/")] = body["sha"]
            return self.result({})
        if "/git/ref/" in endpoint:
            key = endpoint.split("/git/ref/", 1)[1]
            if key in self.stale:
                value = self.stale.pop(key)
                return self.result({"ref": f"refs/{key}", "object": {"type": "commit", "sha": value}})
            if key not in self.refs:
                return self.result({}, 1, "HTTP 404")
            return self.result({"ref": f"refs/{key}", "object": {"type": "commit", "sha": self.refs[key]}})
        if endpoint == "graphql":
            item = body["variables"]["input"]
            updates = item["refUpdates"]
            for change in updates:
                key = change["name"].removeprefix("refs/")
                if key.startswith("pull/") or key == "heads/feature":
                    self.guard_updates.append(change.copy())
                    assert change["beforeOid"] == change["afterOid"], "a PR guard must never change the PR ref"
            if self.change_before_cleanup and len(updates) == 1 and updates[0]["afterOid"] == probe.ZERO:
                key = updates[0]["name"].removeprefix("refs/")
                self.refs[key] = FOREIGN
            mismatch = any(self.refs.get(u["name"].removeprefix("refs/"), probe.ZERO) != u["beforeOid"] for u in updates)
            if mismatch or (self.reject_matching and len(updates) > 1) or (self.fail_control and len(updates) == 1 and updates[0]["afterOid"] != probe.ZERO):
                return self.result({"data": {"updateRefs": None}, "errors": [{"message": "rejected"}]}, 1)
            for update in updates:
                key = update["name"].removeprefix("refs/")
                if update["afterOid"] == probe.ZERO:
                    self.deletions.append(key)
                    if self.stale_after_delete and key in self.refs:
                        self.stale[key] = self.refs[key]
                    self.refs.pop(key, None)
                else:
                    self.refs[key] = update["afterOid"]
            return self.result({"data": {"updateRefs": {"clientMutationId": item["clientMutationId"]}}})
        raise AssertionError(args)


class ProbeTests(unittest.TestCase):
    def execute(self, fake, guard="pull"):
        with patch.object(probe, "api", fake), patch.object(probe.time, "sleep"), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return probe.run("owner/repo", 12, BASE, CANDIDATE, guard)

    def test_matching_guard_and_cleanup(self):
        fake = FakeGitHub()
        self.assertTrue(self.execute(fake))
        self.assertEqual(fake.refs, {"pull/12/head": HEAD, "heads/feature": HEAD})
        self.assertEqual(len(fake.guard_updates), 3)
        self.assertTrue(all(item["beforeOid"] == item["afterOid"] for item in fake.guard_updates))

    def test_same_repository_branch_can_be_guarded_without_mutating_it(self):
        fake = FakeGitHub()
        self.assertTrue(self.execute(fake, "branch"))
        self.assertEqual(fake.refs, {"pull/12/head": HEAD, "heads/feature": HEAD})
        self.assertEqual(len(fake.guard_updates), 3)
        self.assertTrue(all(item["name"] == "refs/heads/feature" for item in fake.guard_updates))

    def test_fork_is_rejected_before_creating_temporary_refs(self):
        fake = FakeGitHub(fork=True)
        with self.assertRaisesRegex(RuntimeError, "same-repository"):
            self.execute(fake, "branch")
        self.assertEqual(fake.refs, {"pull/12/head": HEAD, "heads/feature": HEAD})
        self.assertEqual(fake.guard_updates, [])

    def test_delayed_deletion_readback_does_not_resend_the_delete(self):
        fake = FakeGitHub(stale_after_delete=True)
        self.assertTrue(self.execute(fake))
        self.assertEqual(fake.refs, {"pull/12/head": HEAD, "heads/feature": HEAD})
        self.assertEqual(fake.stale, {})
        self.assertEqual(len(fake.deletions), 2)
        self.assertEqual(len(set(fake.deletions)), 2)

    def test_rejected_matching_guard_is_inconclusive_and_cleans_up(self):
        fake = FakeGitHub(reject_matching=True)
        self.assertFalse(self.execute(fake))
        self.assertEqual(fake.refs, {"pull/12/head": HEAD, "heads/feature": HEAD})

    def test_failed_control_stops_before_any_pull_ref_mutation(self):
        fake = FakeGitHub(fail_control=True)
        with self.assertRaisesRegex(RuntimeError, "control was not confirmed"):
            self.execute(fake)
        self.assertEqual(fake.refs, {"pull/12/head": HEAD, "heads/feature": HEAD})
        self.assertEqual(fake.guard_updates, [])

    def test_unsafe_candidate_never_creates_a_branch(self):
        fake = FakeGitHub(wrong_ancestry=True)
        with self.assertRaisesRegex(RuntimeError, "descendant"):
            self.execute(fake)
        self.assertEqual(fake.refs, {"pull/12/head": HEAD, "heads/feature": HEAD})

    def test_cleanup_refuses_racing_change(self):
        fake = FakeGitHub(change_before_cleanup=True)
        with self.assertRaisesRegex(RuntimeError, "Inspect the temporary branches"):
            self.execute(fake)
        self.assertIn(FOREIGN, fake.refs.values())
        self.assertEqual(fake.refs["pull/12/head"], HEAD)


if __name__ == "__main__":
    unittest.main()
