# SkyServe resource-action renderer v1 dry-run evidence

Status: representative, non-persisting candidate evidence only. It is not
canary-namespace, 201/409, scheduler, runtime, shadow-parity, or authority
evidence.

The six JSON files are compact, key-sorted RFC 8259 JSON followed by one LF.
`SHA256SUMS` commits their exact raw bytes. The readbacks retain the dynamic
dry-run UID, creation timestamp, status, and Service allocation quartet exactly
as returned by `boltz-test`: the two Services at 2026-08-02T14:18:42Z and the
corrected `1G` Pod at 2026-08-02T14:50:08Z. Those dynamic raw values are archival
evidence and are not expected to recur. Hash domains are deliberately
distinct: an evidence-file hash includes its final LF, while persisted
`CanonicalJsonObject.sha256` hashes compact canonical JSON with no LF.

The role projections must reproduce both values below. “Projected file” means
the jq-produced compact JSON plus LF; “persisted canonical” means the same bytes
without that LF.

| Role | Projected evidence-file SHA-256, with LF | Persisted canonical semantic SHA-256, no LF |
|---|---|---|
| `head_ssh_service` | `97c3e83ff160245ef3d8c7a66d7cb99ef9e765768395105f626cccc8abb91e98` | `01f85e19668f5ce16850181367f80ad4bb83d2ba2b3db1e314cbf023f583f2c3` |
| `head_service` | `a3eaffa2728b2459daadd2ac64480317d974081332da3154e452ee7316fe61d9` | `b9f6e3e86df0c26dfe4da1576fe58ba9fd07af0c75c06be920bc5ac65520dd15` |
| `head_pod` | `c5aa3dfe8232a364151da16c650252b4da3b7962a7a636626b168393f93ed937` | `eb037b6c53d4900a22532126b08a20eff9144f755a2bbb9e3c24da57d51ddb38` |

For the head Service, retaining/reinserting requested
`spec.clusterIP="None"` instead yields no-LF hash
`6f56a60c19a22958840c5caffb8a613246107d085d8c5a7dad13f08034fa6ecb`.
That is the superseded provisional transform and must not compare equal to the
canonical projected hash above.

The context reported Kubernetes `v1.33.13-eks-8f14419`. The target was the
deployed representative namespace `skypilot-ha-workloads` and its existing
`skypilot-service-account`; the future `skypilot-actions-canary` namespace was
absent. All API writes below use `dryRun=All`.

## Exact reproduction

Run from the repository root on a host with the private EKS path available.
When a CONNECT proxy is required, set `BOLTZ_TEST_HTTPS_PROXY` to that proxy;
otherwise leave it empty.

```bash
export EVIDENCE_DIR=docs/designs/evidence/skyserve-resource-action-renderer-v1
export HTTPS_PROXY="${BOLTZ_TEST_HTTPS_PROXY:-}"
export NO_PROXY=

kubectl --context boltz-test version -o json

evidence_tmp_dir="$(mktemp -d)"
for object_role in head_ssh_service head_service head_pod; do
  kubectl --context boltz-test create --dry-run=server -o json \
    -f "${EVIDENCE_DIR}/${object_role}.request.json" |
    jq -Sc . > "${evidence_tmp_dir}/${object_role}.dryrun.json"
done
```

Reproduce the role-distinct request and admitted projections. The request-side
headless intent removes only its present `clusterIP`; it never requires or
fabricates the other three allocation fields. The admitted Service projection
requires the complete quartet before removing it.

```bash
jq -Sc . "${EVIDENCE_DIR}/head_ssh_service.request.json" \
  > "${evidence_tmp_dir}/head_ssh_service.requested-semantic.json"
jq -Sc 'del(.spec.clusterIP)' \
  "${EVIDENCE_DIR}/head_service.request.json" \
  > "${evidence_tmp_dir}/head_service.requested-semantic.json"
jq -Sc . "${EVIDENCE_DIR}/head_pod.request.json" \
  > "${evidence_tmp_dir}/head_pod.requested-semantic.json"

jq -Sc 'del(.status)
  | .metadata |= del(.uid,.resourceVersion,.generation,.creationTimestamp,
                     .deletionTimestamp,.managedFields)
  | del(.spec.clusterIP,.spec.clusterIPs,.spec.ipFamilies,
        .spec.ipFamilyPolicy)' \
  "${evidence_tmp_dir}/head_ssh_service.dryrun.json" \
  > "${evidence_tmp_dir}/head_ssh_service.admitted-semantic.json"
jq -Sc 'del(.status)
  | .metadata |= del(.uid,.resourceVersion,.generation,.creationTimestamp,
                     .deletionTimestamp,.managedFields)
  | del(.spec.clusterIP,.spec.clusterIPs,.spec.ipFamilies,
        .spec.ipFamilyPolicy)' \
  "${evidence_tmp_dir}/head_service.dryrun.json" \
  > "${evidence_tmp_dir}/head_service.admitted-semantic.json"
jq -Sc 'del(.status)
  | .metadata |= del(.uid,.resourceVersion,.generation,.creationTimestamp,
                     .deletionTimestamp,.managedFields)
  | del(.spec.nodeName)' \
  "${evidence_tmp_dir}/head_pod.dryrun.json" \
  > "${evidence_tmp_dir}/head_pod.admitted-semantic.json"

for object_role in head_ssh_service head_service head_pod; do
  cmp "${evidence_tmp_dir}/${object_role}.requested-semantic.json" \
      "${evidence_tmp_dir}/${object_role}.admitted-semantic.json"
  sha256sum "${evidence_tmp_dir}/${object_role}.requested-semantic.json"
done

jq -jSc . "${EVIDENCE_DIR}/head_ssh_service.request.json" | sha256sum
jq -jSc 'del(.spec.clusterIP)' \
  "${EVIDENCE_DIR}/head_service.request.json" | sha256sum
jq -jSc . "${EVIDENCE_DIR}/head_pod.request.json" | sha256sum
```

Reproduce the controlled selector probe while holding metadata and every other
spec field byte-equal. `exact` must return `IPv4`/`SingleStack`; `absent`,
`empty`, and `null` must omit the selector and returned
`IPv4,IPv6`/`RequireDualStack` on this recorded cohort.

```bash
for selector_form in absent empty null exact; do
  case "${selector_form}" in
    absent)
      selector_filter='del(.spec.selector)'
      ;;
    empty)
      selector_filter='.spec.selector={}'
      ;;
    null)
      selector_filter='.spec.selector=null'
      ;;
    exact)
      selector_filter='.'
      ;;
  esac
  jq -Sc "${selector_filter}" "${EVIDENCE_DIR}/head_service.request.json" |
    kubectl --context boltz-test create --dry-run=server -o json -f - |
    jq -Sc '{selector:(.spec.selector // "<absent>"),
             ipFamilies:.spec.ipFamilies,
             ipFamilyPolicy:.spec.ipFamilyPolicy,
             ports:(if .spec|has("ports") then .spec.ports
                    else "<absent>" end)}'
done
```

Finally verify that no probe object persisted:

```bash
kubectl --context boltz-test get service \
  ra-schema-v1-head-ssh ra-schema-v1-head \
  --namespace skypilot-ha-workloads
kubectl --context boltz-test get pod ra-schema-v1-head \
  --namespace skypilot-ha-workloads
```

Each GET must return NotFound. A future qualification reruns the same commands
in `skypilot-actions-canary`, captures the then-current admission-policy and
mutating-webhook fingerprints, and replaces representative evidence only after
the normalized semantic hashes remain byte-equal.
