"""Tests for requirements_env module."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dag_converter import requirements_env
from dag_converter.requirements_env import (
    converter_constraints,
    env_dir,
    filter_requirements,
    strip_requirements_arg,
)


class TestRequirementsEnv(unittest.TestCase):
    def test_env_dir_is_keyed_by_contents(self):
        self.assertEqual(env_dir("a==1"), env_dir("a==1"))
        self.assertNotEqual(env_dir("a==1"), env_dir("a==2"))
        self.assertEqual(env_dir("a==1").parent.name, "envs")

    def test_filter_requirements(self):
        lines = [
            "# comment",
            "astronomer-cosmos==1.15.1",
            "boto3==1.30.0",
            "dag-factory>=1.0",
            "apache-airflow-providers-amazon==8.0.0  # old",
            "python-to-yaml-dag-converter-mwaa-serverless==0.1.5",
            "--extra-index-url https://example.invalid/simple",
        ]
        with patch.object(requirements_env, "print_warning") as warn, patch.object(requirements_env, "print_info"):
            kept = filter_requirements(lines)
        self.assertEqual(
            kept, ["astronomer-cosmos==1.15.1", "dag-factory>=1.0", "--extra-index-url https://example.invalid/simple"]
        )
        messages = " ".join(call.args[0] for call in warn.call_args_list)
        self.assertIn("boto3 1.43.1", messages)
        self.assertIn("apache-airflow-providers-amazon 9.38.0", messages)

    def test_strip_requirements_arg(self):
        argv = ["convert", "dag.py", "--requirements", "req.txt", "--no-validate", "--requirements=other.txt"]
        self.assertEqual(strip_requirements_arg(argv), ["convert", "dag.py", "--no-validate"])

    def test_reruns_command_in_env(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            req = Path(temp_dir) / "requirements.txt"
            req.write_text("astronomer-cosmos\n")
            with (
                patch.object(requirements_env, "build_env", return_value=Path("/env/bin/python")),
                patch.object(requirements_env.subprocess, "run", return_value=MagicMock(returncode=3)) as run,
                patch.object(requirements_env, "print_progress"),
            ):
                code = requirements_env.run_in_requirements_env(req, ["convert", "d.py", "--requirements", str(req)])
        self.assertEqual(code, 3)
        run.assert_called_once_with(["/env/bin/python", "-m", "dag_converter.cli", "convert", "d.py"])

    def test_converter_constraints(self):
        frozen = ["rich==14.0.0", "typer==0.16.0", "-e file:///x"]
        self.assertEqual(converter_constraints(frozen, ["rich>=13", "other==1"]), ["typer==0.16.0"])
        with self.assertRaises(RuntimeError) as ctx:
            converter_constraints(frozen, ["rich<13"])
        self.assertIn("rich<13", str(ctx.exception))
        self.assertIn("rich==14.0.0", str(ctx.exception))

    def test_failed_command_shows_stderr_tail(self):
        failed = MagicMock(returncode=1, stderr="line\n" * 30 + "No solution found")
        with (
            patch.object(requirements_env.subprocess, "run", return_value=failed),
            patch.object(requirements_env, "print_progress"),
            self.assertRaises(RuntimeError) as ctx,
        ):
            requirements_env._run(["uv", "pip", "install", "x"])
        self.assertIn("No solution found", str(ctx.exception))
        self.assertLess(str(ctx.exception).count("line"), 21)


class TestBuildEnv(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.req = self.root / "requirements.txt"
        self.req.write_text("-r nested/base.txt\n-c pins.txt\nfoo\n")
        (self.root / "nested").mkdir()
        (self.root / "nested" / "base.txt").write_text("bar==1\n")
        (self.root / "pins.txt").write_text("bar==1\n")

    def tearDown(self):
        self.temp_dir.cleanup()

    def build(self):
        def run(cmd):
            if "venv" in cmd:
                Path(cmd[-1]).mkdir(parents=True)
            calls.append(cmd)

        calls = []
        with (
            patch.object(requirements_env.runtime, "cache_dir", return_value=self.root / "cache"),
            patch.object(requirements_env.shutil, "which", return_value="uv"),
            patch.object(requirements_env, "_run", side_effect=run),
            patch.object(requirements_env.subprocess, "run", return_value=MagicMock(stdout="typer==0.16.0\n")),
            patch.object(requirements_env, "print_progress"),
            patch.object(requirements_env, "print_success"),
        ):
            python = requirements_env.build_env(self.req)
        return python.parent.parent

    def test_nested_files_are_installed_and_keyed(self):
        env = self.build()
        self.assertEqual((env / "requirements.txt").read_text(), f"bar==1\n-c {self.root / 'pins.txt'}\nfoo\n")
        self.assertEqual((env / "constraints.txt").read_text(), "typer==0.16.0\n")
        (self.root / "nested" / "base.txt").write_text("bar==2\n")
        self.assertNotEqual(self.build(), env)
        env = self.build()
        (self.root / "pins.txt").write_text("bar==2\n")
        self.assertNotEqual(self.build(), env)

    def test_converter_source_is_part_of_the_key(self):
        env = self.build()
        with patch.object(requirements_env, "_converter_source_digest", return_value="edited"):
            self.assertNotEqual(self.build(), env)
        self.assertNotEqual(requirements_env._converter_source_digest(), "")
