# Capability envelope: binding a write to the request it came from

**Status:** Open question. Input for the downscoping design session, not a decision.

## Summary

[09](../architecture/09-capability-envelope.md) specifies an attenuating capability: minted at
ingress from the requester's identity, narrowed at each hop, resolved by a verification service.
Review has closed two escalation paths against it so far -- descending from a root you were never
handed, and presenting that root directly. A third survives, and the fix we chose for it covers
only half the problem.

This is the write half. It is written up separately because fixing it properly is a change to the
mechanism rather than to the prose, and because the downscoping question it sits inside has no
consensus yet.

## The problem

The two delegate rules are one field doing two jobs. The resolver of an entry must be the delegate
that entry names. The writer of an entry must be the delegate its _parent_ names. Both are meant to
be "the party that was handed the id".

They are not proved the same way, and that is the whole difficulty:

- **The write side** is proved by the NATS subject prefix, `cap.hop.<agent-id>.*`. That is fixed per
  agent, and 02 fixes cardinality at one Cluster Admin Agent per cluster and one Developer Team
  Agent per namespace, so the prefix names a subject serving every concurrent request through that
  agent at once.
- **The read side** looked better and is not. The verifier does authenticate a live caller, so that
  half can be per-request. But it compares the caller against the entry's `delegate` field, and that
  field holds an agent id written by the parent -- so a per-request credential changes one side of a
  comparison whose other side is still per-agent. Compare at agent granularity and the escalation
  below survives untouched. Demand exact equality and no legitimate resolution matches. A
  per-request caller identity on its own buys nothing.

An earlier version of this document said the read side was carried. It is not, and it fails for the
same reason as the write side, which is worth knowing before the session rather than during it.

So the escalation moves rather than closing. Agent A is concurrently the named delegate of H1's root
(operator tier, project-wide) and H2's root (reader, one cluster). Serving H2's request, A writes a
child descending from H1's root, naming B as delegate, and dispatches it. B resolves it and every
check passes -- B really is the named delegate, the chain really does narrow, the prefix really does
say A, and A really is H1's root's delegate. The confusion was injected upstream of the hop that
checks for it. H2's reader request runs with H1's operator envelope, and the audit walk names H1.

An honest broker with a concurrency bug produces exactly this. It does not take an attacker.

**09 now says the claim is ahead of the mechanism**, and §5 states this gap along with the three
below. What it does not carry, deliberately, is the analysis: why the obvious fix does not drop in,
and what the options cost. That is this document, and it is the part the session needs.

## The obstacle is not the one it looks like

The obvious objection is ordering: the parent writes the child entry and then dispatches the
message, so a per-request credential for the downstream hop has not been issued yet and the parent
would be naming something that does not exist.

That objection is wrong, and it is worth being clear about because it was in an earlier version of
this document and in 09. **The credential does not have to exist for the parent to name it. The
name has to be derivable.** It is, if the principal is the pair of an agent and a request id,
because the parent allocates the request id it is dispatching. So the parent can write
`delegate: {agent: platform-a, request: req-77}` before anything has been issued, and whatever
issues the downstream credential binds it to that pair.

The real obstacle is narrower: **something has to issue per-request principals, and the subject
prefix has to be able to express one.** A publish permission of `cap.hop.<agent-id>.*` cannot; it
would have to become `cap.hop.<agent-id>.<request-id>.*` with the credential scoped to that segment.
That is a runtime and bus-configuration question, which is why it belongs in the session.

The failure mode to avoid is the middle one. If `delegate` holds a request-scoped name and the
prefix can only say the agent, an implementer projects the prefix back to the agent to make
legitimate writes work -- and the concurrency denial test passes while the hole stays open.

## Options

**A. The per-request credential is a NATS identity, not just a verifier-auth one.** Publish
permission becomes request-scoped, so the subject prefix proves `(agent, request)` and both delegate
rules are request-scoped by the same mechanism. The parent names `delegate: {agent, request-id}`,
which it can do because the parent allocates the id it is dispatching. Cost is a bus identity per request, and
it is worth being honest that this is the whole cost rather than an increment on something we are
already getting. Nothing issues per-request principals today. 08 section 5 holds a scope broker out
of v1, its non-goals rule out per-request credential enforcement by name, and 02 and 06 fix agent
identity as one pre-created ServiceAccount per agent. So option A is not "reuse the pool that is
coming" -- it is "build the thing that issues these, and make it issue a bus identity too". That is
the question for the session: whether the claim is worth standing that up.

**B. Accept agent-granular writes and narrow the claim.** The bound becomes the widest capability
concurrently delegated to that agent. Still a real bound, and honest, but it is not the sentence 09
exists to make, and it means one compromised or buggy hop can escalate any concurrent request it is
serving to the widest tier it holds. Cheap. Weak.

**C. Sign the child.** Kills the escalation outright and kills the design's central property with
it -- no cryptographic key anywhere is most of why this approach was chosen over macaroons. Not
recommended, listed so nobody has to rediscover why.

A is the only one that preserves the claim. Whether the claim is worth a bus identity per request is
the actual question, and it is a downscoping question rather than a capability-envelope one, which
is why it belongs in the session rather than in 09.

## Entangled, and probably the same conversation

These came out of the same review pass. They are separate defects, but each one turns on the same
unanswered question -- what "the request it is serving" means to a mechanism that cannot observe a
request.

- **Nothing expires.** No entry carries an issue time, a use count, or a request state, and TTLs are
  explicitly rejected as a benefit of the design. A root minted at 10:00 for a request that finished
  at 10:01 is still a valid parent at 03:00. A compromised hop writes a fresh child of its own old
  root and drives work attributed to a human who went home.
- **Revocation names no actor.** "Revocation is deleting an entry" is a stated goal. The verifier
  holds read only, so it cannot delete. The gateway holds `cap.root.*`. The only party that can
  delete a hop entry is the broker that wrote it, which is the party you are revoking from.
- **The envelope has no requester field.** It carries tier and scope. 09 leaves 03 §4a canonical for
  the requirement, and that requirement is a `SubjectAccessReview` against the requester's own
  identity -- which a hop holding a resolved capability cannot run, because it does not know who the
  human is. Meanwhile 09's own goal is written as "agent ceiling ∩ requester" while the mechanism
  delivers agent ceiling ∩ tier. Those need reconciling in whichever direction the session picks.
- **`tier: operator` / `tier: reader`.** 06 fixes the `tier` values, and they are the three
  personas. 09's worked example uses two that appear nowhere else in the set, and one of them reads
  as sanctioning a write-capable envelope, against invariant #1. Probably just vocabulary, but it is
  vocabulary in the one example everyone will copy.

**The auth callout's seed can mint a root, and nobody owns it yet.** This one arrived late and is
not about downscoping, but it wants a decision from the same people. Under auth callout the user
JWT carries the pub/sub permissions the server enforces, so whoever holds the signing seed can issue
itself publish on the capability root prefix and read across the bucket -- mint a root at any tier,
read everything in flight. 09 said the opposite for several drafts ("no compromise of a key forges
authority"), which is now fixed, but the posture question is open: this is the root of authority for
the bus and it needs gateway-grade custody, a rotation story and a compromise runbook, none of which
exist. Worth noting it also weakens the argument 09 uses against macaroons -- one fleet-wide minting
key in one service rather than one in every broker, which is a difference in blast radius and not in
kind.

## Not for the session

Implementation-level, real, and fixable without a design decision. Recording them so they are not
lost:

- NATS KV entries are mutable -- a `put` overwrites and a delete is itself a publish. 09's audit
  property and its caching rule both rest on "immutable once written", which the store does not
  provide. A hop can rewrite its own link after the fact.
- The permission model is written in KV key space (`cap.*`) rather than subject space. A KV bucket
  lives on `$KV.cap.>`, and a read is a JetStream API call. "No broker may read `cap.*`" as spelled
  is satisfiable while a broker holding broad `$JS.API.>` keeps a read path to the store, and the
  denial test for it passes against the wrong namespace.
- 09 said "no cryptographic key anywhere". That was corrected twice and the second correction is in
  the section above, because it turned out to be a design question rather than wording.
