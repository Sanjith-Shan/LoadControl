#!/usr/bin/env python3
"""Writes NUMBERS.md: the hand-written headline list (each line names its
file) followed by every table bench/lcnumbers.py computes from results/."""
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HEADER = os.path.join(REPO, "bench", "numbers_header.md")

tables = subprocess.run([sys.executable, os.path.join(REPO, "bench", "lcnumbers.py")],
                        capture_output=True, text=True, check=True).stdout
with open(os.path.join(REPO, "NUMBERS.md"), "w", newline="\n") as f:
    f.write(open(HEADER).read().rstrip() + "\n\n---\n\n# Every table\n\n" + tables)
print("wrote NUMBERS.md")
