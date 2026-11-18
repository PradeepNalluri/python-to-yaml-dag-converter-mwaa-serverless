"""``--requirements``: parse Dags inside a cached virtualenv containing the converter plus the
Dag's requirements, by re-running the same convert command in that environment.

The environment is installed for the LOCAL platform (it is only used for parsing). Requirements
that conflict with packages pre-installed on MWAA Serverless are skipped with a warning, because
the service always uses its pre-installed version. The converter's own dependencies are kept at
the versions installed with it; a requirement that conflicts with them fails the build.
"""

import hashlib
import re
import shutil
import subprocess
import sys
from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement

from dag_converter import __version__, runtime
from dag_converter.console_config import print_info, print_progress, print_success, print_warning

CONVERTER_DIST = "python-to-yaml-dag-converter-mwaa-serverless"
NESTED_FILE_OPTION = re.compile(r"^(-r|--requirement|-c|--constraint)(?:\s+|\s*=\s*)(\S+)$")


def env_dir(requirements_text: str) -> Path:
    python_version = ".".join(str(v) for v in sys.version_info[:3])
    key = f"{requirements_text}\n{__version__}\n{python_version}\n{_converter_source_digest()}"
    return runtime.cache_dir() / "envs" / hashlib.sha256(key.encode()).hexdigest()[:16]


def read_requirements(path: Path, files: list[Path] | None = None) -> tuple[list[str], list[Path]]:
    """Lines of a requirements file with nested ``-r`` files inlined and ``-c`` paths made absolute (both are
    relative to the file that references them); also returns every file referenced, for the cache key."""
    files = [] if files is None else files
    path = Path(path).absolute()
    if path in files:
        return [], files
    files.append(path)
    lines = []
    for raw in path.read_text().splitlines():
        line = raw.split(" #", 1)[0].strip()
        match = NESTED_FILE_OPTION.match(line)
        if match and "://" not in match[2]:
            nested = path.parent / match[2]
            if match[1] in ("-r", "--requirement"):
                lines += read_requirements(nested, files)[0]
                continue
            files.append(nested.absolute())
            line = f"-c {nested.absolute()}"
        lines.append(line)
    return lines, files


def filter_requirements(lines: list[str]) -> list[str]:
    """Drop requirements the parse env must not install, warning about pre-installed conflicts."""
    kept = []
    for raw in lines:
        line = raw.split(" #", 1)[0].strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-"):
            kept.append(line)
            continue
        try:
            req = Requirement(line)
        except InvalidRequirement:
            kept.append(line)
            continue
        name = runtime.normalize_name(req.name)
        if name == CONVERTER_DIST:
            print_info(f"Skipping '{line}': the converter is installed into the environment separately")
            continue
        if name in runtime.PREINSTALLED_PACKAGES:
            service_version = runtime.PREINSTALLED_PACKAGES[name]
            if req.specifier and (service_version is None or not req.specifier.contains(service_version, True)):
                print_warning(
                    f"Requirement '{line}' conflicts with {name} {service_version or '(unpinned)'} pre-installed on "
                    "MWAA Serverless. The service will use the pre-installed version; skipping it for parsing."
                )
                continue
        kept.append(line)
    return kept


def strip_requirements_arg(argv: list[str]) -> list[str]:
    result, skip = [], False
    for arg in argv:
        if skip:
            skip = False
        elif arg == "--requirements":
            skip = True
        elif not arg.startswith("--requirements="):
            result.append(arg)
    return result


def _converter_source() -> str:
    """Local source checkout if running from one, otherwise the released version."""
    root = Path(__file__).resolve().parent.parent
    pyproject = root / "pyproject.toml"
    if pyproject.exists() and CONVERTER_DIST in pyproject.read_text():
        return str(root)
    return f"{CONVERTER_DIST}=={__version__}"


def _converter_source_digest() -> str:
    """Hash of a local source checkout (empty for a release), so that source edits rebuild the environment."""
    source = _converter_source()
    if source.startswith(CONVERTER_DIST):
        return ""
    digest = hashlib.sha256()
    files = [Path(source, "pyproject.toml"), *Path(source, "dag_converter").rglob("*")]
    for file in sorted(f for f in files if f.is_file() and "__pycache__" not in f.parts):
        digest.update(f"{file.relative_to(source)}\n".encode() + file.read_bytes())
    return digest.hexdigest()


def _python_in(env: Path) -> Path:
    return env / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")


def _run(cmd: list[str]) -> None:
    print_progress("$ " + " ".join(cmd))
    result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        tail = "\n".join(result.stderr.strip().splitlines()[-20:])
        raise RuntimeError(
            f"Failed to build the requirements environment (command exited {result.returncode}):\n{tail}"
        )


def converter_constraints(frozen: list[str], requirements: list[str]) -> list[str]:
    """Pins of the converter environment (``pip freeze``), minus names the requirements pin themselves.
    Raises if such a requirement excludes the version the converter was installed with."""
    pins = {runtime.normalize_name(line.split("==")[0]): line for line in frozen if "==" in line}
    for line in requirements:
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        name = runtime.normalize_name(req.name)
        if name in pins and req.specifier:
            version = pins.pop(name).split("==", 1)[1]
            if not req.specifier.contains(version, prereleases=True):
                raise RuntimeError(
                    f"Requirement '{line}' conflicts with {name}=={version}, which the converter is installed with"
                )
    return list(pins.values())


def build_env(requirements_path: Path) -> Path:
    """Create (or reuse) the cached environment; returns its Python interpreter."""
    lines, files = read_requirements(requirements_path)
    text = "\n".join(f.read_text() if f.exists() else "" for f in files)
    env = env_dir(text)
    python = _python_in(env)
    if (env / ".complete").exists() and python.exists():
        print_info(f"Reusing cached requirements environment {env}")
        return python

    print_progress(f"Creating requirements environment {env}")
    shutil.rmtree(env, ignore_errors=True)
    env.parent.mkdir(parents=True, exist_ok=True)
    uv = shutil.which("uv")
    if uv:
        _run([uv, "venv", "--quiet", "--python", sys.executable, str(env)])
        pip = [uv, "pip", "install", "--python", str(python)]
        freeze = [uv, "pip", "freeze", "--python", str(python)]
    else:
        _run([sys.executable, "-m", "venv", str(env)])
        pip = [str(python), "-m", "pip", "install"]
        freeze = [str(python), "-m", "pip", "freeze"]

    _run([*pip, _converter_source()])

    # Keep the converter's dependencies at the versions installed with it
    kept = filter_requirements(lines)
    frozen = subprocess.run(freeze, capture_output=True, text=True, check=True).stdout.splitlines()
    (env / "constraints.txt").write_text("\n".join(converter_constraints(frozen, kept)) + "\n")
    if kept:
        (env / "requirements.txt").write_text("\n".join(kept) + "\n")
        _run([*pip, "-r", str(env / "requirements.txt"), "-c", str(env / "constraints.txt")])
    (env / ".complete").write_text(text)
    print_success(f"Requirements environment ready: {env}")
    return python


def run_in_requirements_env(requirements_path: Path, argv: list[str]) -> int:
    """Re-run the convert command (without --requirements) inside the requirements environment."""
    python = build_env(requirements_path)
    cmd = [str(python), "-m", "dag_converter.cli", *strip_requirements_arg(argv)]
    print_progress("Re-running inside the requirements environment: " + " ".join(cmd))
    return subprocess.run(cmd).returncode
