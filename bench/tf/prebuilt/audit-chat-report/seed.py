# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Copy a stream's sandbox-local report store into the broker store before the stack edits it.

Run in the shell sandbox as `python3 - <audit> <local store dir>`. With the broker
store on (FLEET_AUDIT_REPORT_STORE=broker, #6110), the scripts' first touch of a
stream copies the sandbox's local copy of it into the broker store, `latest.json`
included, unless the local stream directory carries the seed marker. The stack
hides `latest.json` in the broker's store (store.sh, in the agent container); a
seed after that would bring it back. So this reads each local stream once
through the broker, which seeds it, and then requires the marker in every local
stream directory before the stack goes on. A sandbox with no local store, or a
file store selected (the broker store off), has nothing to seed.
"""

import os
import sys
from pathlib import Path

SKILL_SCRIPTS = "/opt/data/skills/fleet-audit/scripts"
PLATFORM_SCRIPTS = "/opt/data/scripts"


def main() -> int:
    audit, store = sys.argv[1], Path(sys.argv[2])
    sys.path[:0] = [SKILL_SCRIPTS, PLATFORM_SCRIPTS]
    import audit_report  # noqa: PLC0415 -- the path is the sandbox's, set just above
    import report_status  # noqa: PLC0415

    if os.environ.get("FLEET_AUDIT_REPORT_STORE", "") != "broker" or not store.is_dir():
        print(f"seed: nothing to seed for {audit} (broker store off, or no local store)")
        return 0
    missing = []
    for owner in sorted(p for p in store.iterdir() if p.is_dir()):
        for stream in sorted(p for p in owner.iterdir() if p.is_dir()):
            repo = f"{owner.name}/{stream.name.replace('%2F', '/')}"
            audit_report.report_store_dir(audit, repo).exists()
            if not (stream / report_status.SEED_MARKER_NAME).exists():
                missing.append(repo)
    if missing:
        print(f"seed: no seed marker after a broker read for {', '.join(missing)}", file=sys.stderr)
        return 1
    print(f"seed: {audit}'s local streams are in the broker store")
    return 0


if __name__ == "__main__":
    sys.exit(main())
