"""
Horizontal autoscaler + load-balancer controller for the API tier.
(docker-compose stand-in for a Kubernetes HPA + Service/readiness gates, or an ALB target group.)

Two jobs, one loop:

1. Keep NGINX's upstream list equal to the set of HEALTHY replicas.
   The list lives in a file shared with NGINX; on any change we send NGINX a graceful reload (HUP).
   - A new replica is added only once its health check passes, so it never receives traffic while booting.
   - A replica chosen for removal is taken out of the list FIRST and given time to drain
     before its container is stopped, so in-flight requests are never cut off.

2. Scale on CPU, the same control loop as an HPA:
     desired = ceil(current_replicas * avg_cpu / target_cpu), clamped to [MIN, MAX]
   - scale UP immediately (at most doubling per step)
   - scale DOWN only to the highest desired value seen during the stabilization window
   - no scaling decisions while replicas are still starting (their idle CPU would skew the average)
"""
import math
import os
import re
import subprocess
import time

PROJECT = os.getenv("COMPOSE_PROJECT", "flash-sale-system")
SERVICE = os.getenv("SCALE_SERVICE", "backend")
COMPOSE_FILE = os.getenv("COMPOSE_FILE_PATH", "/project/docker-compose.yml")
PROJECT_DIR = os.getenv("COMPOSE_PROJECT_DIR", "/project")
NGINX_CONTAINER = os.getenv("NGINX_CONTAINER", "flash_sale_nginx")
UPSTREAM_FILE = os.getenv("UPSTREAM_FILE", "/upstream/backends.conf")
APP_PORT = int(os.getenv("APP_PORT", "8000"))

MIN_REPLICAS = int(os.getenv("MIN_REPLICAS", "2"))
MAX_REPLICAS = int(os.getenv("MAX_REPLICAS", "8"))
TARGET_CPU = float(os.getenv("TARGET_CPU_PERCENT", "50"))
INTERVAL = float(os.getenv("INTERVAL_SECONDS", "3"))
SCALE_DOWN_STABILIZATION = float(os.getenv("SCALE_DOWN_STABILIZATION_SECONDS", "60"))
DRAIN_SECONDS = float(os.getenv("DRAIN_SECONDS", "6"))


def log(msg: str):
    print(f"{time.strftime('%H:%M:%S')} [autoscaler] {msg}", flush=True)


def run(cmd, timeout=120):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def replicas():
    """Running replicas as dicts: name, index, ip, healthy. Sorted by index."""
    ids = run([
        "docker", "ps", "-q",
        "--filter", f"label=com.docker.compose.project={PROJECT}",
        "--filter", f"label=com.docker.compose.service={SERVICE}",
        "--filter", "status=running",
    ]).stdout.split()
    if not ids:
        return []
    fmt = "{{.Name}}|{{.State.Health.Status}}|{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
    out = run(["docker", "inspect", "-f", fmt, *ids]).stdout.strip().splitlines()
    result = []
    for line, cid in zip(out, ids):
        name, health, ip = (line.split("|") + ["", "", ""])[:3]
        m = re.search(r"-(\d+)$", name.strip("/"))
        result.append({
            "id": cid,
            "name": name.strip("/"),
            "index": int(m.group(1)) if m else 0,
            "ip": ip,
            "healthy": health == "healthy" and bool(ip),
        })
    return sorted(result, key=lambda r: r["index"])


def average_cpu(reps):
    """Average CPU% across the given replicas (100% = one full core, a replica's ceiling)."""
    out = run(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", *[r["id"] for r in reps]])
    values = []
    for line in out.stdout.split():
        try:
            values.append(float(line.strip().rstrip("%")))
        except ValueError:
            continue
    return sum(values) / len(values) if values else 0.0


def sync_upstream(reps):
    """Write the healthy replica list for NGINX and gracefully reload it if (and only if) it changed."""
    lines = [f"server {r['ip']}:{APP_PORT} max_fails=0;" for r in reps if r["healthy"]]
    content = "\n".join(lines) + ("\n" if lines else "")
    try:
        current = open(UPSTREAM_FILE).read()
    except FileNotFoundError:
        current = None
    if current == content:
        return False
    tmp = UPSTREAM_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(content)
    os.replace(tmp, UPSTREAM_FILE)  # atomic: NGINX never sees a half-written file
    res = run(["docker", "kill", "-s", "HUP", NGINX_CONTAINER])
    log(f"upstream -> {len(lines)} healthy replica(s); nginx reload {'ok' if res.returncode == 0 else 'FAILED: ' + res.stderr.strip()}")
    return True


def scale_to(n: int):
    res = run([
        "docker", "compose", "-p", PROJECT, "-f", COMPOSE_FILE, "--project-directory", PROJECT_DIR,
        "up", "-d", "--no-deps", "--no-build", "--no-recreate", "--scale", f"{SERVICE}={n}", SERVICE,
    ], timeout=180)
    if res.returncode != 0:
        log(f"scale to {n} failed: {res.stderr.strip()[-300:]}")
        return False
    return True


def main():
    log(f"starting: service={SERVICE} min={MIN_REPLICAS} max={MAX_REPLICAS} target_cpu={TARGET_CPU}% "
        f"down_window={SCALE_DOWN_STABILIZATION}s drain={DRAIN_SECONDS}s")
    recommendations = []  # (timestamp, desired) for the scale-down stabilization window

    while True:
        try:
            reps = replicas()
            sync_upstream(reps)  # always reconcile first: newly healthy replicas join, dead ones leave

            current = len(reps)
            healthy = [r for r in reps if r["healthy"]]
            if current == 0:
                log("no running replicas yet; waiting")
            elif current < MIN_REPLICAS:
                log(f"below minimum ({current} < {MIN_REPLICAS}); scaling up")
                scale_to(MIN_REPLICAS)
            elif len(healthy) < current:
                log(f"replicas={current} ({current - len(healthy)} still starting); holding decisions")
            else:
                cpu = average_cpu(healthy)
                raw = math.ceil(current * cpu / TARGET_CPU) if cpu > 0 else MIN_REPLICAS
                desired = max(MIN_REPLICAS, min(MAX_REPLICAS, raw, current * 2))

                now = time.monotonic()
                recommendations.append((now, desired))
                recommendations = [(t, d) for t, d in recommendations if now - t <= SCALE_DOWN_STABILIZATION]

                target = current
                if desired > current:
                    target = desired
                elif desired < current:
                    target = min(current, max(d for _, d in recommendations))

                action = "hold"
                if target > current:
                    action = f"scale up {current} -> {target}"
                    if scale_to(target):
                        recommendations = [(now, target)]
                elif target < current:
                    action = f"scale down {current} -> {target} (drain first)"
                    victims = reps[target:]  # compose removes the highest-numbered replicas
                    sync_upstream([r for r in reps if r not in victims])  # stop routing to them...
                    time.sleep(DRAIN_SECONDS)                            # ...let in-flight requests finish...
                    if scale_to(target):                                 # ...then stop them
                        recommendations = [(now, target)]
                log(f"replicas={current} avg_cpu={cpu:.0f}% desired={desired} -> {action}")
        except Exception as e:  # keep the loop alive through transient docker errors
            log(f"error: {e}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
