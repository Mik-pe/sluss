"""Safety tests for the opt-in GitHub CAS probe; no provider calls are made."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("sluss_cas_probe", Path(__file__).parents[1] / "scripts" / "probe-github-cas.py")
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
BASE, FIRST, SECOND, OTHER = (letter * 40 for letter in "abcd")


class FakeGitHub:
    def __init__(self, *, bad_ancestry=False, foreign_ref=False, ambiguous_create=False, race_on_delete=False, partial_reject=False):
        self.lock = threading.Lock()
        self.refs = {}
        self.deleted = []
        self.bad_ancestry = bad_ancestry
        self.foreign_ref = foreign_ref
        self.ambiguous_create = ambiguous_create
        self.race_on_delete = race_on_delete
        self.partial_reject = partial_reject
        self.changed = False

    def __call__(self, args, body=None):
        with self.lock:
            endpoint = next((arg for arg in args if arg.startswith("repos/") or arg == "graphql"), "")
            if endpoint == "repos/owner/repo":
                return self.response({"full_name": "owner/repo", "node_id": "R_repo"})
            if "/compare/" in endpoint:
                return self.response({"status": "diverged" if self.bad_ancestry else "ahead", "ahead_by": 1,
                                      "base_commit": {"sha": BASE}, "merge_base_commit": {"sha": BASE}})
            if endpoint == "graphql":
                update = body["variables"]["input"]
                changes = update["refUpdates"]
                target = changes[0]
                name = target["name"].removeprefix("refs/heads/")
                if target["afterOid"] == "0" * 40 and self.race_on_delete and name.endswith("-stage-b"):
                    self.refs[name] = OTHER
                    self.changed = True
                if any(self.refs[change["name"].removeprefix("refs/heads/")] != change["beforeOid"] for change in changes):
                    if self.partial_reject:
                        self.refs[target["name"].removeprefix("refs/heads/")] = target["afterOid"]
                    return self.response({"data": {"updateRefs": None}, "errors": [{"message": "rejected"}]}, 1)
                for change in changes:
                    name = change["name"].removeprefix("refs/heads/")
                    if change["afterOid"] == "0" * 40:
                        self.deleted.append(name)
                        del self.refs[name]
                    else:
                        self.refs[name] = change["afterOid"]
                return self.response({"data": {"updateRefs": {"clientMutationId": update["clientMutationId"]}}})
            if endpoint.endswith("/git/refs"):
                name = body["ref"].removeprefix("refs/heads/")
                self.refs[name] = body["sha"]
                if self.ambiguous_create and name.endswith("-stage-a"):
                    return self.response({}, 1, "connection lost")
                return self.response({})
            if "/git/ref/heads/" in endpoint:
                name = endpoint.split("/git/ref/heads/", 1)[1]
                # Alter one staging ref after the concurrent updates, never at creation.
                if self.foreign_ref and self.refs.get(name) == SECOND and name.endswith("-stage-b") and any(
                    key.endswith("-target") and value != BASE for key, value in self.refs.items()
                ):
                    self.refs[name] = OTHER
                    self.changed = True
                if name not in self.refs:
                    return self.response({}, 1, "HTTP 404")
                return self.response({"object": {"sha": self.refs[name]}})
            raise AssertionError(f"Unexpected request: {args}")

    @staticmethod
    def response(body, code=0, stderr=""):
        return SimpleNamespace(returncode=code, stdout=json.dumps(body), stderr=stderr)


class ProbeSafetyTests(unittest.TestCase):
    def run_probe(self, fake):
        with patch.object(probe, "api", fake), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            probe.run("owner/repo", BASE, FIRST, SECOND)

    def test_one_winner_and_confirmed_cleanup(self):
        fake = FakeGitHub()
        self.run_probe(fake)
        self.assertEqual(len(fake.deleted), 5)
        self.assertEqual(fake.refs, {})

    def test_failed_marker_precondition_does_not_advance_target(self):
        fake = FakeGitHub()
        fake.refs = {"target": BASE, "stage": FIRST, "marker": OTHER}
        update = {"variables": {"input": {"clientMutationId": "operation", "refUpdates": [
            {"name": "refs/heads/target", "beforeOid": BASE, "afterOid": FIRST, "force": False},
            {"name": "refs/heads/stage", "beforeOid": FIRST, "afterOid": FIRST, "force": False},
            {"name": "refs/heads/marker", "beforeOid": BASE, "afterOid": FIRST, "force": False},
        ]}}}
        response = fake(["graphql"], update)
        self.assertNotEqual(response.returncode, 0)
        self.assertEqual(fake.refs, {"target": BASE, "stage": FIRST, "marker": OTHER})

    def test_partial_target_write_after_failed_marker_aborts_probe(self):
        fake = FakeGitHub(partial_reject=True)
        with self.assertRaisesRegex(RuntimeError, "atomic rejection"):
            self.run_probe(fake)
        self.assertEqual(fake.refs, {})
        self.assertEqual(len(fake.deleted), 5)

    def test_unrelated_ancestry_fails_before_creating_refs(self):
        fake = FakeGitHub(bad_ancestry=True)
        with self.assertRaisesRegex(RuntimeError, "not a verified descendant"):
            self.run_probe(fake)
        self.assertEqual(fake.refs, {})
        self.assertEqual(fake.deleted, [])

    def test_changed_ref_is_never_deleted(self):
        fake = FakeGitHub(foreign_ref=True)
        with self.assertRaisesRegex(RuntimeError, "Inspect the listed refs"):
            self.run_probe(fake)
        self.assertTrue(fake.changed)
        self.assertEqual(list(fake.refs.values()), [OTHER])
        self.assertEqual(len(fake.deleted), 4)

    def test_ambiguous_create_is_checked_and_cleaned(self):
        fake = FakeGitHub(ambiguous_create=True)
        with self.assertRaisesRegex(RuntimeError, "Creation.*not confirmed"):
            self.run_probe(fake)
        self.assertEqual(fake.refs, {})
        self.assertEqual(len(fake.deleted), 2)

    def test_cleanup_compare_and_swap_preserves_racing_change(self):
        fake = FakeGitHub(race_on_delete=True)
        with self.assertRaisesRegex(RuntimeError, "Inspect the listed refs"):
            self.run_probe(fake)
        self.assertTrue(fake.changed)
        self.assertEqual(list(fake.refs.values()), [OTHER])
        self.assertEqual(len(fake.deleted), 4)


if __name__ == "__main__":
    unittest.main()
