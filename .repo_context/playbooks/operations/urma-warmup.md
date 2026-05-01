# URMA Connection Warmup After Kubernetes Worker Deployment

## Scope

- Use this playbook when workers run with:
  - `enable_urma=true`
  - `enable_worker_worker_batch_get=true`
  - short business `requestTimeoutMs`, for example `5ms`
- Goal:
  - consume the first worker-to-worker URMA connection cost before business traffic reaches the cluster.
- Current source-backed mechanism:
  - `KVClient::Get` can trigger worker Remote Get.
  - Remote Get fills URMA request information when URMA is enabled.
  - If the provider worker reports `K_URMA_NEED_CONNECT`, the requester worker calls
    `WorkerRemoteWorkerTransApi::ExecOnceParrallelExchange()` and then `WorkerWorkerExchangeUrmaConnectInfo()`.

## Required Deployment Order

1. Deploy all worker Pods.
2. Wait until all worker Pods are `Ready`.
3. Build a complete worker address list in `host:port` form.
4. Run `scripts/operations/urma_warmup.py` with a warmup timeout much larger than the expected URMA handshake time.
5. Only after the warmup Job succeeds, start or unpause business clients using the normal short timeout.

Do not run the business clients with `requestTimeoutMs=5ms` before step 4 succeeds. The first URMA handshake is expected
to take about `100ms`, so a `5ms` business timeout cannot safely absorb it.

## Worker Address Discovery

For the current deployment chart, worker Pods are labeled `app=kvcache-worker` and listen on
`global.port.datasystemWorker`, default `31501`.

Example after Helm deployment:

```bash
NS=default
PORT=31501
SELECTOR='app=kvcache-worker'

kubectl -n "${NS}" rollout status deployment/kvcache-worker --timeout=10m
kubectl -n "${NS}" wait --for=condition=Ready pod -l "${SELECTOR}" --timeout=10m

kubectl -n "${NS}" get pods -l "${SELECTOR}" \
  -o jsonpath="{range .items[*]}{.status.podIP}:${PORT}{'\n'}{end}" > /tmp/yr-workers.txt
```

If the production deployment uses one Deployment per node, choose a selector that matches all worker Pods, or concatenate
the addresses from all worker Deployments into the same `/tmp/yr-workers.txt`.

## Manual Post-Deploy Job

Use this when the deployment pipeline can run `kubectl` after `helm install` or `helm upgrade`.

1. Create a ConfigMap for the script and worker list:

```bash
NS=default

kubectl -n "${NS}" create configmap yr-urma-warmup-script \
  --from-file=urma_warmup.py=scripts/operations/urma_warmup.py \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NS}" create configmap yr-urma-warmup-workers \
  --from-file=workers.txt=/tmp/yr-workers.txt \
  --dry-run=client -o yaml | kubectl apply -f -
```

2. Apply the Job. Replace `openyuanrong-datasystem:0.7.0` with the same image used by the worker Pods, or another image
   that contains the `yr.datasystem` Python package and client libraries.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: yr-urma-warmup
  namespace: default
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: warmup
          image: openyuanrong-datasystem:0.7.0
          imagePullPolicy: IfNotPresent
          command: ["python3", "/warmup/urma_warmup.py"]
          args:
            - "--workers-file=/warmup/workers.txt"
            - "--timeout-ms=5000"
            - "--ttl-second=300"
            - "--parallel=32"
            - "--retries=3"
          volumeMounts:
            - name: warmup-script
              mountPath: /warmup/urma_warmup.py
              subPath: urma_warmup.py
            - name: warmup-workers
              mountPath: /warmup/workers.txt
              subPath: workers.txt
      volumes:
        - name: warmup-script
          configMap:
            name: yr-urma-warmup-script
        - name: warmup-workers
          configMap:
            name: yr-urma-warmup-workers
```

3. Wait for success:

```bash
kubectl -n default apply -f yr-urma-warmup-job.yaml
kubectl -n default wait --for=condition=Complete job/yr-urma-warmup --timeout=20m
kubectl -n default logs job/yr-urma-warmup
```

After the Job completes, business traffic can use `requestTimeoutMs=5ms`.

## Helm Or Pipeline Integration

Recommended integration point:

- keep worker Deployment unchanged;
- add a post-deploy pipeline step after `helm install` / `helm upgrade`;
- that step waits for all worker Pods, generates `/tmp/yr-workers.txt`, creates the ConfigMaps, runs the warmup Job, and
  gates business rollout on Job completion.

Avoid using an `initContainer` in worker Pods for this warmup. An init container runs before the worker in the same Pod is
ready, so it cannot warm the full cluster.

Avoid a Helm `post-install` hook unless the hook Job can discover worker Pod IPs at runtime. Helm renders templates
before the worker Pods have final Pod IPs, so a hook with a static worker list is usually easier to make correct from the
deployment pipeline than from the chart itself.

## Parameter Guidance

| Parameter | Recommended value | Reason |
| --- | --- | --- |
| `--timeout-ms` | `5000` | Must be greater than URMA handshake time, expected about `100ms`. |
| `--ttl-second` | `300` | Avoid warmup keys expiring before all `64 * 63` directed gets finish. |
| `--parallel` | `16` or `32` | Keeps total time reasonable without flooding worker-worker transport threads. |
| `--payload-bytes` | `4096` | Small payload is enough to trigger Remote Get and URMA connection setup. |
| `--retries` | `3` | Absorbs short readiness or metadata propagation races after rollout. |

For 64 workers, the directed warmup performs `64 * 63 = 4032` Remote Get calls. This is intentional: it verifies every
requester-to-provider direction instead of assuming the handshake is symmetric.

## Success Criteria

- The Job exits with status `Complete`.
- Job logs end with `URMA warmup finished`.
- Worker logs show the warmup-time URMA exchange messages.
- Business traffic starts after the Job completes and does not see first-request `K_RPC_DEADLINE_EXCEEDED` caused by
  `K_URMA_NEED_CONNECT`.

## Failure Handling

- If worker list size is smaller than expected, stop and inspect Pod readiness or the selector before starting business.
- If the Job fails with timeout, raise `--timeout-ms` first, then reduce `--parallel`.
- If failures are only for a small number of pairs, inspect the source and destination worker logs for
  `[URMA_NEED_CONNECT]`, `K_URMA_CONNECT_FAILED`, or worker-worker RPC errors.
- If warmup keys expire during execution, increase `--ttl-second` or set it to `0` and clean keys later with a separate
  operational script.

## Source References

- `src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp`
- `src/datasystem/worker/object_cache/worker_worker_transport_api.cpp`
- `src/datasystem/worker/object_cache/worker_worker_transport_service_impl.cpp`
- `src/datasystem/common/rdma/fast_transport_manager_wrapper.cpp`
- `scripts/operations/urma_warmup.py`
