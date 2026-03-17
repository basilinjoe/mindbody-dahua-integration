#!/usr/bin/env python3
"""PostToolUse hook: run ruff check + format on any .py file Claude edits."""

import json
import subprocess
import sys

data = json.loads(sys.stdin.read())
file_path = data.get("tool_input", {}).get("file_path", "")

if not file_path.endswith(".py"):
    sys.exit(0)

subprocess.run(["uv", "run", "ruff", "check", "--fix", file_path], check=False)
subprocess.run(["uv", "run", "ruff", "format", file_path], check=False)
