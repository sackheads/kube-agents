#!/usr/bin/env python3
"""Mutation-verify the conformance suite: delete a control, expect red.

If deleting a control leaves the suite green, the test does not exist. Slice 2a
shipped a whole gate that could be removed with its suite byte-identical, and
only a dedicated task caught it -- so this is the property the conformance
suite is graded on, not test count.

Each mutation names the control it removes and the test it must break. The run
applies one mutation at a time to the working tree, runs the suite, restores
the file with `git checkout`, and reports:

    KILLED   the named test failed. The test is real.
    SURVIVED the named test still passed. The test is theatre -- fix it.
    NOISY    the mutation broke something other than the named test as well.
             Not a failure, but worth reading: it usually means two assertions
             overlap, and occasionally means the mutation was blunter than
             intended.

An expected failure that a mutation turns into an *unexpected success* also
counts as KILLED: the recorded gap moved, which is exactly the signal wanted.

Usage:
    python3 hack/conformance-mutations.py            # every mutation
    python3 hack/conformance-mutations.py --list
    python3 hack/conformance-mutations.py -k C1      # substring filter on the id

The tree must be clean. It edits tracked files in place and restores them, so a
dirty tree risks losing work -- it refuses rather than guessing.
"""

from __future__ import annotations

import argparse
import dataclasses
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


@dataclasses.dataclass(frozen=True)
class Mutation:
    """One removed control, and the test that has to notice."""

    id: str
    path: str
    #: (old, new) applied with str.replace, or a callable taking/returning text.
    edit: tuple[str, str]
    #: Substring matching the test name that must go red.
    kills: str
    #: What the mutation is pretending to be: a plausible bad change, not noise.
    pretext: str
    #: True for a mutation that must NOT be caught. A suite that goes red on a
    #: harmless change is a suite people learn to override, so a couple of
    #: no-op edits are run as a control on the harness itself: SURVIVED is the
    #: pass for these and KILLED is the failure.
    must_survive: bool = False


MUTATIONS: list[Mutation] = [
    # ---- A. Authority ---------------------------------------------------
    Mutation(
        "A3-impersonation-flags",
        "agents/platform/scripts/command_policy.py",
        ('{"--as", "--as-group", "--as-uid", "--as-user-extra", "--impersonate-service-account"}',
         '{"--as-group", "--as-uid", "--as-user-extra", "--impersonate-service-account"}'),
        "test_A3_rejects_caller_supplied_as",
        "drop the plain --as, keeping the others -- the shape a careless "
        "refactor of a set literal takes",
    ),
    Mutation(
        "A3-kuberc",
        "agents/platform/scripts/command_policy.py",
        ('        if name == "--kuberc":\n            return "--kuberc"\n',
         '        if name == "--kuberc-disabled":\n            return "--kuberc"\n'),
        "test_A3_rejects_kuberc",
        "neuter the dedicated kuberc check; the flag stays in "
        "_KUBECTL_IDENTITY_FLAGS, so this tests whether the second guard holds",
    ),
    Mutation(
        "A3-attached-shorthand",
        "agents/platform/scripts/command_policy.py",
        ('        if token.startswith("-s") and token != "-s":\n            return "-s"\n', ""),
        "test_A3_rejects_attached_shorthand_server",
        "remove the attached-shorthand clause, restoring the exact-token "
        "matching the slice 2a Critical defeated",
    ),
    Mutation(
        "A3-kubectl-kuberc-env",
        "agents/platform/scripts/credential_proxy.py",
        ('"KUBECTL_KUBERC": "false"', '"KUBECTL_KUBERC_UNUSED": "false"'),
        "test_A3_default_path_kuberc_is_disabled",
        "rename the env var while 'tidying', leaving the default-path kuberc "
        "feature on and the protection resting on mount geometry alone",
    ),
    Mutation(
        "A3-server-flag",
        "agents/platform/scripts/command_policy.py",
        ('        "-s", "--server",\n        "--token", "--user", "--username", "--password",',
         '        "--token", "--user", "--username", "--password",'),
        "test_A3_rejects_credential_redirection",
        "drop --server from the identity set -- it is also in "
        "_KUBECTL_FLAGS_WITH_VALUE, so it still parses and looks handled",
    ),
    Mutation(
        "A1-refusal-content",
        "agents/platform/scripts/command_policy.py",
        ('                "Identity and API server address belong to the broker. Remove "',
         '                f"Identity belongs to the broker; {argv} was refused. Remove "'),
        "test_A1_a_refusal_names_no_caller_supplied_value",
        "make the refusal more helpful by naming what was refused -- the "
        "obvious improvement that turns a denial into an oracle",
    ),
    Mutation(
        "A4-operator-escalate",
        "k8s-operator/config/rbac/role.yaml",
        ("    resourceNames:\n      - view\n", ""),
        "test_A4_the_operator_cannot_escalate",
        "remove the resourceNames bound on `bind`, so the operator can attach "
        "any existing ClusterRole to an agent",
    ),
    # ---- B. The write path ----------------------------------------------
    Mutation(
        "B1-read-only-verbs",
        "agents/platform/scripts/command_policy.py",
        ('        ("get",),\n', '        ("get",),\n        ("delete",),\n'),
        "test_B1_kubectl_write_verbs_are_refused",
        "add delete to the read allowlist -- the single-line change an "
        "operator makes to unblock a skill",
    ),
    Mutation(
        "B1-image-gate",
        "deploy/docker/Dockerfile",
        ("unexpected credential-aware CLI in sandbox image", "sandbox image note"),
        "test_B1_the_sandbox_image_ships_no_credentialed_cli",
        "reword the build gate's message, which is what the assertion anchors "
        "on -- checks that the anchor is registered and policed",
    ),
    Mutation(
        "B1-image-gate-binaries",
        "deploy/docker/Dockerfile",
        ("for binary in gcloud kubectl gh git;", "for binary in gcloud kubectl;"),
        "test_B1_the_sandbox_image_ships_no_credentialed_cli",
        "shorten the gate's binary list, the plausible edit when one of them "
        "is legitimately needed at build time",
    ),
    Mutation(
        "B1-denylist-rule",
        "k8s-operator/internal/controller/platformagent_manifests.go",
        ('{"id":"gcp.access-token-disclosure"', '{"id":"gcp.access-token-disclosure-XX"'),
        "test_B1_the_shipped_denylist_refuses_credential_disclosure",
        "renames a rule id without touching its pattern, so nothing is actually "
        "weakened. A control on the harness: the suite must not go red on a "
        "rename, or it becomes a suite people learn to override.",
        must_survive=True,
    ),
    Mutation(
        "B2-automerge",
        ".github/workflows/validate.yml",
        ("jobs:", "jobs:\n  merge:\n    runs-on: ubuntu-latest\n    steps:\n"
                  "      - run: gh pr merge --auto --squash \"$NUMBER\"\n"),
        "test_B2_no_workflow_approves_or_merges",
        "add an auto-merge job, which is the thing B2 exists to forbid",
    ),
    Mutation(
        "B4-workflow-run-gate",
        ".github/workflows/autopush-redeploy-agent.yml",
        ("github.event.workflow_run.head_branch == 'main'", "true"),
        "test_B4_every_workflow_run_deploy_gates",
        "drop the branch predicate while debugging a deploy, which is when it "
        "actually gets dropped",
    ),
    Mutation(
        "B4-pull-request-target-checkout",
        ".github/workflows/auto_request_review.yml",
        ("    steps:", "    steps:\n      - uses: actions/checkout@v4\n        with:\n"
                        "          ref: ${{ github.event.pull_request.head.sha }}"),
        "test_B4_no_pull_request_target_workflow_checks_out",
        "check out the PR head in a pull_request_target workflow -- arbitrary "
        "code execution with the base repository's token",
    ),
    Mutation(
        "B6-codeowners-bot",
        "examples/gitops-repo/CODEOWNERS.example",
        ("@your-org/security", "@kube-agents-bot[bot]"),
        "test_B6_the_gitops_template_names_no_automation_identity",
        "name an automation identity as a code owner, defeating the one rule "
        "a GitHub App cannot satisfy",
    ),
    # ---- C. Enforcement --------------------------------------------------
    Mutation(
        "C1-share-process-namespace",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("      serviceAccountName: kubeagents-platform-agent",
         "      shareProcessNamespace: true\n      serviceAccountName: kubeagents-platform-agent"),
        "test_C1_the_process_namespace_is_never_shared",
        "a golden fixture regenerated after someone set the field, which is "
        "how it would actually arrive",
    ),
    Mutation(
        "C1-uid-collapse",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("          runAsUser: 10001", "          runAsUser: 10000"),
        "test_C1_the_agent_and_the_broker_run_as_different_users",
        "collapse the broker onto the sandbox UID, restoring the procfs and "
        "socket reach the split removed",
    ),
    Mutation(
        "C1-socket-umask",
        "agents/platform/scripts/credential_proxy.py",
        ("previous_umask = os.umask(0o177)", "previous_umask = os.umask(0o022)"),
        "test_C1_the_broker_backend_socket_is_bound_private",
        "widen the umask the socket is bound under -- the slice 2b near-miss, "
        "where a umask added for the shared PVC reached the socket",
    ),
    Mutation(
        "C1-shell-true",
        "agents/platform/scripts/credential_proxy.py",
        ("            start_new_session=True,", "            start_new_session=True,\n            shell=True,"),
        "test_C1_the_executor_never_reaches_a_shell",
        "interpose a shell, which is what makes `;` and `#` live again",
    ),
    Mutation(
        "C1-executable-allowlist",
        "agents/platform/scripts/credential_proxy.py",
        ('ALLOWED_EXECUTABLES = ("gcloud", "kubectl", "gh", "git")',
         'ALLOWED_EXECUTABLES = ("gcloud", "kubectl", "gh", "git", "sh")'),
        "test_C1_the_executor_refuses_an_executable_it_does_not_ship",
        "add sh to the allowlist, giving a compound command somewhere to land",
    ),
    Mutation(
        "C1-egress-whole-internet",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent-egress-allowlist.yaml",
        ("        - ipBlock:\n            cidr: 172.16.0.0/28",
         "        - ipBlock:\n            cidr: 0.0.0.0/0\n            except:\n            - 169.254.169.254/32"),
        "test_C1_the_rendered_egress_policy",
        "the exact construction slice 2b 1.3 refused: `0.0.0.0/0 except "
        "metadata`, which adds the internet rather than subtracting an address",
    ),
    Mutation(
        "C1-cidr-guard-inert",
        "k8s-operator/internal/controller/platformagent_egress_policy.go",
        ("\tif reason := ipv4MappedRefusal(prefix, cidr); reason != \"\" {\n\t\treturn reason\n\t}\n", ""),
        "test_C1_every_operator_supplied_cidr_reaches_the_refusal_guards",
        "rename the 4-in-6 guard so every call site misses it; Go would not "
        "compile, but the point is that the conformance suite says so first",
    ),
    Mutation(
        "C2-unknown-flag-fail-open",
        "agents/platform/scripts/command_policy.py",
        ("            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:\n"
         "                return None, name\n",
         "            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:\n"
         "                index += 1\n                continue\n"),
        "test_C2_an_unparseable_argv_is_refused",
        "skip unknown flags instead of refusing -- fail open, and the reason "
        "the module enumerates arity rather than allowlisting flags",
    ),
    Mutation(
        "C2-read-only-default",
        "agents/platform/scripts/credential_proxy.py",
        ('return os.getenv("CREDENTIAL_PROXY_ENFORCE_READ_ONLY", "true").strip().lower() != "false"',
         'return os.getenv("CREDENTIAL_PROXY_ENFORCE_READ_ONLY", "true").strip().lower() == "true"'),
        "test_C2_the_read_only_gate_survives_a_typo",
        "compare for truth rather than against falsehood -- looks equivalent, "
        "and disarms the gate on every typo",
    ),
    Mutation(
        "C2-external-key-default",
        "agents/platform/scripts/credential_proxy.py",
        ('external_key = os.getenv("API_SERVER_EXTERNAL_KEY", "").strip()',
         'external_key = os.getenv("API_SERVER_EXTERNAL_KEY", "dev").strip()'),
        "test_C2_the_agent_api_proxy_refuses_to_start_without_its_key",
        "give the external key a development default, which is how the "
        "loopback sentinel got there in the first place",
    ),
    Mutation(
        "C3-policy-reads-a-file",
        "agents/platform/scripts/command_policy.py",
        ('    for token in argv[1:]:\n        name, _, _ = token.partition("=")\n        if name == "--kuberc":',
         '    for token in argv[1:]:\n        name, _, value = token.partition("=")\n'
         '        if name == "--kuberc" and value and open(value):\n'
         '            pass\n        if name == "--kuberc":'),
        "test_C3_the_policy_module_imports_nothing_that_can_read",
        "check whether the kuberc file exists before refusing, the "
        "helpful-looking change that reintroduces a rewrite-after-check race",
    ),
    Mutation(
        "C3-log-sanitiser",
        "agents/platform/scripts/credential_proxy.py",
        ("filtered = ''.join(c for c in s if unicodedata.category(c) not in ('Cc', 'Cf', 'Zl', 'Zp'))",
         "filtered = s"),
        "test_C3_untrusted_output_cannot_forge_a_log_line",
        "stop stripping control characters, so tool output can forge a record",
    ),
    Mutation(
        "C4-unpinned-action",
        ".github/workflows/prettier.yml",
        ("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1",
         "actions/checkout@v7"),
        "test_C4_every_third_party_action_is_pinned_to_a_commit",
        "float one action back to a tag, which a Dependabot conflict "
        "resolution can produce by hand",
    ),
    Mutation(
        "C4-base-image-digest",
        "tags.env",
        ("@sha256:a6ce64e2038867885c2c90f6602425e6e70293d5e6d952a0e603a99265e01c40", ""),
        "test_C4_the_agent_base_image_is_pinned_by_digest",
        "drop the digest and keep the tag, which reads as equivalent",
    ),
    Mutation(
        "C5-minted-write-verb",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("      - get\n      - list\n", "      - get\n      - list\n      - patch\n"),
        "test_C5_no_minted_role_grants_a_write_verb",
        "add patch to a minted explorer role, the change a feature request for "
        "annotating resources produces",
    ),
    Mutation(
        "C5-bind-to-edit",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("  name: view\n", "  name: edit\n"),
        "test_C5_the_agent_is_bound_to_no_write_capable_builtin_role",
        "bind the agent to `edit` while leaving every minted rule read-only, "
        "which the verb-level assertion alone cannot see",
    ),
    Mutation(
        "C5-reaper-eats-guardrail",
        "k8s-operator/internal/controller/platformagent_controller.go",
        ('&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: agent.Name + "-sandbox", Namespace: agent.Namespace}},',
         '&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: agent.Name + "-sandbox", Namespace: agent.Namespace}},\n'
         '\t\t&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: agent.Name + "-sandbox-metadata-deny", Namespace: agent.Namespace}},'),
        "test_C5_the_controller_does_not_reap_the_metadata_deny_guardrail",
        "restore the reaper's reach over the guardrail -- slice 2b 1.5, "
        "verbatim",
    ),
    Mutation(
        "C5-admission-binding-drift",
        "k8s-operator/config/admission/agent-rbac-policy.yaml",
        ("  policyName: kube-agents-agent-readonly", "  policyName: prefixed-kube-agents-agent-readonly"),
        "test_C5_the_admission_binding_names_a_policy_that_exists",
        "the kustomize namePrefix outcome slice 2b 1.2 caught: both objects "
        "exist, the binding points at nothing, and kubectl get looks right",
    ),
    Mutation(
        "C5-admission-fail-open",
        "k8s-operator/config/admission/agent-rbac-policy.yaml",
        ("  failurePolicy: Fail", "  failurePolicy: Ignore"),
        "test_C5_the_admission_policy_fails_closed",
        "the one-line edit B3 names: 'unblock apply during upgrade window'",
    ),
    # ---- D. Accountability ------------------------------------------------
    Mutation(
        "D1-principal-not-logged",
        "agents/platform/scripts/credential_proxy.py",
        ("            _sanitize_for_logging(principal.describe()),", "            \"-\","),
        "test_D1_the_exec_route_records_a_principal",
        "drop the principal from the exec record while refactoring a handler "
        "that does not yet read it",
    ),
    Mutation(
        "D1-hint-not-sanitised",
        "agents/platform/scripts/credential_proxy.py",
        ('safe_hint = _sanitize_for_logging(log_hint) if log_hint else "unknown"',
         'safe_hint = log_hint if log_hint else "unknown"'),
        "test_D1_a_log_hint_cannot_forge_a_record",
        "log the hint raw, which is the state the sanitiser was added to fix",
    ),
    Mutation(
        "D2-workflow-mode",
        "k8s-operator/api/v1alpha1/common_types.go",
        ("type SecuritySpec struct {", "type SecuritySpec struct {\n\tWorkflowMode string `json:\"workflowMode,omitempty\"`"),
        "test_D2_no_direct_apply_mode_exists",
        "add the break-glass field another design document already offers",
    ),
    Mutation(
        "D4-token-never-expires",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("              expirationSeconds: 3600", "              expirationSeconds: 86400"),
        "test_D4_every_projected_token_expires",
        "stretch the projection to a day to stop a rotation warning",
    ),
    Mutation(
        "D4-audience-dropped",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("              audience: kubeagents-credential-proxy\n", ""),
        "test_D4_the_broker_token_is_audience_bound",
        "drop the audience, making the broker's token a general-purpose "
        "cluster bearer token and TokenReview a formality",
    ),
    Mutation(
        "D4-secret-becomes-literal",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("- name: API_SERVER_EXTERNAL_KEY\n              valueFrom:\n                secretKeyRef:\n"
         "                  key: api-key\n                  name: platformagent-secrets",
         "- name: API_SERVER_EXTERNAL_KEY\n              value: hunter2"),
        "test_D4_the_customer_api_key_is_secret_backed",
        "inline the external key as a literal, the way the loopback sentinel "
        "already is",
    ),
    # ---- D15 and the harness ----------------------------------------------
    Mutation(
        "D15-guard-normalises",
        "k8s-operator/internal/controller/platformagent_egress_policy.go",
        ("Overlaps(ipv4MappedSpace)", "Contains(ipv4MappedSpace.Addr())"),
        "test_D15_the_guard_refuses_the_ambiguous_form_rather_than_normalising",
        "swap Overlaps for the Contains that produced the finding -- the same "
        "spelling, the same cross-family blind spot",
    ),
    Mutation(
        "harness-source-moved",
        "agents/platform/scripts/command_policy.py",
        ("def evaluate(", "def evaluate_command("),
        "test_every_anchor_is_still_present",
        "rename the entry point. Nothing here should pass quietly: the "
        "self-check has to be the thing that goes red first",
    ),
    Mutation(
        "harness-fixture-emptied",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("\nkind: ClusterRole\n", "\nkind: ClusterRoleXX\n"),
        "test_the_golden_fixtures_render_more_than_a_stub",
        "corrupt a fixture's object kinds, which would turn every assertion "
        "that iterates it vacuously green",
    ),
]


def _run_suite() -> tuple[set[str], set[str]]:
    """(failed test names, unexpectedly-successful test names)."""
    process = subprocess.run(
        [sys.executable, "tests/conformance/run.py"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=300,
    )
    output = process.stdout + process.stderr
    failed = set(re.findall(r"^(?:FAIL|ERROR): (\S+)", output, re.MULTILINE))
    # An expected failure that starts passing is reported as an unexpected
    # success, which is also the suite noticing the mutation.
    unexpected = set(re.findall(r"^UNEXPECTED SUCCESS: (\S+)", output, re.MULTILINE))
    if "unexpected successes" in output:
        unexpected |= set(re.findall(r"(\S+) \(.*\) \.\.\. unexpected success", output))
    return failed, unexpected


def _git_clean() -> bool:
    return not subprocess.run(
        ["git", "status", "--porcelain"], cwd=REPO, capture_output=True, text=True
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("-k", "--filter", default="")
    arguments = parser.parse_args()

    selected = [m for m in MUTATIONS if arguments.filter in m.id]
    if arguments.list:
        for mutation in selected:
            print(f"{mutation.id:38} {mutation.path}")
        return 0

    if not _git_clean():
        print(
            "the working tree is dirty. This edits tracked files in place and "
            "restores them with git checkout; refusing rather than risking it.",
            file=sys.stderr,
        )
        return 2

    baseline_failed, baseline_unexpected = _run_suite()
    if baseline_failed or baseline_unexpected:
        print(f"baseline is not green: {sorted(baseline_failed | baseline_unexpected)}")
        return 2
    print(f"baseline green. {len(selected)} mutations.\n")

    verdicts = []
    for mutation in selected:
        path = REPO / mutation.path
        original = path.read_text()
        old, new = mutation.edit
        if old not in original:
            verdicts.append((mutation, "STALE", []))
            print(f"STALE    {mutation.id}: the text it edits is not in {mutation.path}")
            continue
        try:
            path.write_text(original.replace(old, new, 1))
            failed, unexpected = _run_suite()
        finally:
            subprocess.run(["git", "checkout", "--", mutation.path], cwd=REPO, check=True)

        noticed = failed | unexpected
        killers = {name for name in noticed if mutation.kills in name}
        others = sorted(name.split(".")[-1] for name in noticed - killers)
        if mutation.must_survive:
            verdict = "OVERSHOT" if noticed else "SURVIVED (expected)"
        elif killers:
            verdict = "NOISY" if others else "KILLED"
        else:
            verdict = "SURVIVED"
        verdicts.append((mutation, verdict, others))
        detail = f"  (also: {', '.join(others[:3])}{'…' if len(others) > 3 else ''})" if others else ""
        print(f"{verdict:8} {mutation.id}{detail}")

    survived = [m.id for m, verdict, _ in verdicts if verdict in ("SURVIVED", "OVERSHOT")]
    stale = [m.id for m, verdict, _ in verdicts if verdict == "STALE"]
    print(
        f"\nkilled={sum(1 for _, v, _ in verdicts if v == 'KILLED')} "
        f"noisy={sum(1 for _, v, _ in verdicts if v == 'NOISY')} "
        f"survived={len(survived)} stale={len(stale)}"
    )
    if survived:
        print(
            f"UNRESOLVED: {survived} -- a SURVIVED mutation means the test does "
            f"not test what it claims to; an OVERSHOT one means the suite goes "
            f"red on a change that weakens nothing"
        )
    if stale:
        print(f"STALE: {stale} -- the mutation no longer applies; rewrite it")
    return 1 if survived or stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
