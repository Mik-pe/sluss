"""Exercise the example workflow's actual shell with a fake cargo binary."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / "examples/github-actions/sluss-ci.yml"


def workflow_script():
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index("        run: |") + 1
    body = []
    for line in lines[start:]:
        if not line.startswith("          "):
            break
        body.append(line[10:])
    if not body:
        raise AssertionError("workflow step has no shell body")
    return "\n".join(body) + "\n"


class ExampleWorkflowTests(unittest.TestCase):
    def run_selection(self, value, fail_package=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cargo = root / "cargo"
            cargo.write_text(
                '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CARGO_LOG"\n'
                'case "$*" in *"$FAIL_PACKAGE"*) '
                '[ -z "$FAIL_PACKAGE" ] || exit 42 ;; esac\n'
            )
            cargo.chmod(0o755)
            log = root / "cargo.log"
            environment = os.environ.copy()
            environment.update(
                PATH=f"{directory}:{environment.get('PATH', '')}",
                SLUSS_SUITES=value,
                CARGO_LOG=str(log),
                FAIL_PACKAGE=fail_package,
            )
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", workflow_script()],
                env=environment,
                cwd=root,
                capture_output=True,
                text=True,
                timeout=10,
            )
            calls = log.read_text().splitlines() if log.exists() else []
            return result, calls, (root / "injected").exists()

    def test_full_and_selected_suites_run_only_the_expected_commands(self):
        result, calls, _ = self.run_selection("full")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["test --workspace --locked"])
        result, calls, _ = self.run_selection("api-tests,ui-tests")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["test --locked -p example-api", "test --locked -p example-ui"])
        result, calls, _ = self.run_selection("core-tests,api-tests,ui-tests")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["test --locked -p example-core", "test --locked -p example-api", "test --locked -p example-ui"])

    def test_invalid_inputs_do_not_run_any_suite_or_shell_code(self):
        for value in ["", ",api-tests", "api-tests,", "api-tests,,ui-tests", "api-tests,api-tests", "full,ui-tests", "api-tests,unknown", "api-tests\nunknown", "api-tests\n", "$(touch injected)"]:
            with self.subTest(value=value):
                result, calls, injected = self.run_selection(value)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, [])
                self.assertFalse(injected)

    def test_failure_stops_the_remaining_suites(self):
        result, calls, _ = self.run_selection("api-tests,ui-tests", "example-api")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, ["test --locked -p example-api"])


if __name__ == "__main__":
    unittest.main()
