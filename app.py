#!/usr/bin/env python3
"""
Orchestrateur CI/CD résilient pour cluster Kubernetes à nœuds hybrides.
Version 3 — corrections (session de debug) :
  - contexte Kaniko remis en git:// (https:// fait échouer kaniko avec
    "gzip: invalid header", car l'URL est alors traitée comme une archive
    tar.gz au lieu d'être clonée comme un dépôt Git)
  - image kaniko pinnée en v1.23.1 (:latest segfault au démarrage sur
    certains nœuds du cluster — Exit Code 139 avant la première ligne
    de log, reproductible et corrigé par ce pin de version)
"""

import os
import time
import json
import hmac
import hashlib
import logging
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from kubernetes import client, config
from kubernetes.client.rest import ApiException

POD_NAME = os.environ.get("POD_NAME", f"unknown-{uuid.uuid4().hex[:6]}")
NAMESPACE = os.environ.get("NAMESPACE", "cicd-system")

LEASE_NAME = os.environ.get("LEASE_NAME", "cicd-orchestrator-leader")
LEASE_DURATION = int(os.environ.get("LEASE_DURATION_SECONDS", "15"))
RENEW_INTERVAL = int(os.environ.get("RENEW_INTERVAL_SECONDS", "5"))
RETRY_INTERVAL = int(os.environ.get("RETRY_INTERVAL_SECONDS", "3"))

GITHUB_OWNER = os.environ["GITHUB_OWNER"]
GITHUB_REPO = os.environ["GITHUB_REPO"]
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))

# --- Webhook ---
# Le webhook est le déclencheur principal (quasi instantané). Le polling
# GitHub ci-dessus reste actif comme filet de sécurité si un webhook est
# manqué (tunnel smee.io coupé, redémarrage, etc.) — d'où l'intérêt de
# garder POLL_INTERVAL raisonnable (ex. 300s) plutôt que de le désactiver.
GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET", "")
WEBHOOK_PORT = int(os.environ.get("WEBHOOK_PORT", "8000"))
LEADER_TICK_SECONDS = int(os.environ.get("LEADER_TICK_SECONDS", "3"))

REGISTRY = os.environ.get("REGISTRY", "192.168.56.105:30500")
IMAGE_NAME = os.environ.get("IMAGE_NAME", "demo-app")

TARGET_NAMESPACE = os.environ.get("TARGET_NAMESPACE", "default")
TARGET_DEPLOYMENT = os.environ.get("TARGET_DEPLOYMENT", "demo-app")
TARGET_CONTAINER = os.environ.get("TARGET_CONTAINER", "demo-app")

STATE_CONFIGMAP = os.environ.get("STATE_CONFIGMAP", "cicd-state")

RUN_TESTS = os.environ.get("RUN_TESTS", "true").lower() == "true"
TEST_COMMAND = os.environ.get("TEST_COMMAND", "")

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{POD_NAME}] %(levelname)s: %(message)s",
)
log = logging.getLogger("orchestrator")

config.load_incluster_config()
coord_api = client.CoordinationV1Api()
core_api = client.CoreV1Api()
batch_api = client.BatchV1Api()
apps_api = client.AppsV1Api()

_lock = threading.Lock()
_is_leader = False


def am_i_leader() -> bool:
    with _lock:
        return _is_leader


def set_leader(value: bool):
    global _is_leader
    with _lock:
        if _is_leader != value:
            log.info("Changement de statut : %s", "LEADER" if value else "FOLLOWER")
        _is_leader = value


def _now_iso():
    return datetime.now(timezone.utc)


def try_acquire_or_renew_lease() -> bool:
    try:
        lease = coord_api.read_namespaced_lease(LEASE_NAME, NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            body = client.V1Lease(
                metadata=client.V1ObjectMeta(name=LEASE_NAME, namespace=NAMESPACE),
                spec=client.V1LeaseSpec(
                    holder_identity=POD_NAME,
                    lease_duration_seconds=LEASE_DURATION,
                    acquire_time=_now_iso(),
                    renew_time=_now_iso(),
                    lease_transitions=0,
                ),
            )
            try:
                coord_api.create_namespaced_lease(NAMESPACE, body)
                log.info("Bail créé, je deviens leader.")
                return True
            except ApiException as e2:
                if e2.status == 409:
                    return False
                raise
        else:
            raise

    holder = lease.spec.holder_identity
    renew_time = lease.spec.renew_time or lease.spec.acquire_time
    expired = (_now_iso() - renew_time).total_seconds() > (lease.spec.lease_duration_seconds or LEASE_DURATION)

    if holder == POD_NAME:
        lease.spec.renew_time = _now_iso()
        try:
            coord_api.replace_namespaced_lease(LEASE_NAME, NAMESPACE, lease)
            return True
        except ApiException as e:
            if e.status == 409:
                log.warning("Conflit en renouvelant le bail — je perds le leadership.")
                return False
            raise

    if not expired:
        return False

    log.info("Bail expiré (dernier détenteur : %s) — tentative de reprise.", holder)
    lease.spec.holder_identity = POD_NAME
    lease.spec.acquire_time = _now_iso()
    lease.spec.renew_time = _now_iso()
    lease.spec.lease_transitions = (lease.spec.lease_transitions or 0) + 1
    try:
        coord_api.replace_namespaced_lease(LEASE_NAME, NAMESPACE, lease)
        log.info("Bail repris avec succès — je deviens leader.")
        return True
    except ApiException as e:
        if e.status == 409:
            return False
        raise


def leader_election_loop():
    while True:
        try:
            leader_now = try_acquire_or_renew_lease()
            set_leader(leader_now)
        except Exception:
            log.exception("Erreur pendant l'élection — je me considère FOLLOWER par prudence.")
            set_leader(False)
        time.sleep(RENEW_INTERVAL if am_i_leader() else RETRY_INTERVAL)


def read_state() -> dict:
    """Lit la ConfigMap d'état (lastDeployedSha, webhookSha...), la crée si absente."""
    try:
        cm = core_api.read_namespaced_config_map(STATE_CONFIGMAP, NAMESPACE)
        return cm.data or {}
    except ApiException as e:
        if e.status == 404:
            body = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=STATE_CONFIGMAP, namespace=NAMESPACE),
                data={"lastDeployedSha": ""},
            )
            core_api.create_namespaced_config_map(NAMESPACE, body)
            return {"lastDeployedSha": ""}
        raise


def patch_state(fields: dict):
    """Met à jour un ou plusieurs champs de la ConfigMap d'état.
    Utilisable par n'importe quel pod (leader ou non), notamment le
    handler webhook — RBAC 'patch configmaps' déjà accordé dans 02-rbac.yaml.
    """
    read_state()  # garantit que la ConfigMap existe avant de la patcher
    body = client.V1ConfigMap(data=fields)
    core_api.patch_namespaced_config_map(STATE_CONFIGMAP, NAMESPACE, body)


def get_last_deployed_sha() -> str:
    return read_state().get("lastDeployedSha", "")


def set_last_deployed_sha(sha: str):
    patch_state({"lastDeployedSha": sha})


def get_latest_commit_sha() -> str:
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/commits/{GITHUB_BRANCH}"
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()["sha"]


def run_job_and_wait(job_name: str, container_image: str, args=None, command=None,
                      env=None, timeout_seconds: int = 900) -> bool:
    container = client.V1Container(
        name="worker",
        image=container_image,
        args=args,
        command=command,
        env=[client.V1EnvVar(name=k, value=v) for k, v in (env or {}).items()],
    )
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, namespace=NAMESPACE,
                                      labels={"app": "cicd-pipeline"}),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=300,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(restart_policy="Never", containers=[container]),
            ),
        ),
    )
    batch_api.create_namespaced_job(NAMESPACE, job)
    log.info("Job '%s' lancé (image=%s).", job_name, container_image)

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status = batch_api.read_namespaced_job_status(job_name, NAMESPACE).status
        if status.succeeded:
            log.info("Job '%s' terminé avec succès.", job_name)
            return True
        if status.failed:
            log.error("Job '%s' a échoué.", job_name)
            return False
        time.sleep(5)

    log.error("Job '%s' : timeout dépassé.", job_name)
    return False


def build_image(sha: str) -> str:
    short_sha = sha[:8]
    image_tag = f"{REGISTRY}/{IMAGE_NAME}:{short_sha}"
    # git:// est le contexte fonctionnel pour kaniko sur ce dépôt public.
    # https:// échoue avec "gzip: invalid header" (kaniko tente de le
    # traiter comme une archive tar.gz plutôt que de cloner le dépôt).
    # On garde https:// uniquement dans le cas avec token ci-dessous, car
    # un token d'authentification ne peut pas être passé via git://.
    git_context = f"git://github.com/{GITHUB_OWNER}/{GITHUB_REPO}.git#refs/heads/{GITHUB_BRANCH}"
    if GITHUB_TOKEN:
        git_context = (
            f"https://{GITHUB_TOKEN}@github.com/{GITHUB_OWNER}/{GITHUB_REPO}"
            f".git#refs/heads/{GITHUB_BRANCH}"
        )

    ok = run_job_and_wait(
        job_name=f"kaniko-build-{short_sha}-{int(time.time())}",
        container_image="gcr.io/kaniko-project/executor:v1.23.1",
        args=[
            f"--context={git_context}",
            "--dockerfile=Dockerfile",
            f"--destination={image_tag}",
            f"--destination={REGISTRY}/{IMAGE_NAME}:latest",
            "--insecure",
            "--skip-tls-verify",
            "--insecure-pull",
        ],
        timeout_seconds=900,
    )
    return image_tag if ok else ""


def run_tests(image_tag: str, sha: str) -> bool:
    if not RUN_TESTS or not TEST_COMMAND:
        log.info("Étape de tests sautée (RUN_TESTS=false ou TEST_COMMAND vide).")
        return True
    short_sha = sha[:8]
    return run_job_and_wait(
        job_name=f"tests-{short_sha}-{int(time.time())}",
        container_image=image_tag,
        command=["/bin/sh", "-c"],
        args=[TEST_COMMAND],
        timeout_seconds=600,
    )


def deploy(image_tag: str):
    log.info("Déploiement de %s sur %s/%s", image_tag, TARGET_NAMESPACE, TARGET_DEPLOYMENT)
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": TARGET_CONTAINER, "image": image_tag}]
                }
            }
        }
    }
    apps_api.patch_namespaced_deployment(TARGET_DEPLOYMENT, TARGET_NAMESPACE, patch)


def process_commit(current_sha: str, last_sha: str):
    log.info("Nouveau commit détecté : %s (précédent : %s)",
              current_sha[:8], last_sha[:8] if last_sha else "aucun")

    image_tag = build_image(current_sha)
    if not image_tag:
        log.error("Échec du build — pipeline interrompu pour ce commit.")
        return

    if not run_tests(image_tag, current_sha):
        log.error("Échec des tests — déploiement annulé pour ce commit.")
        return

    deploy(image_tag)
    set_last_deployed_sha(current_sha)
    log.info("Pipeline terminé avec succès pour le commit %s.", current_sha[:8])


def worker_loop():
    """Boucle du leader : réagit en priorité au SHA reçu par webhook (quasi
    instantané), et retombe sur un polling GitHub classique en filet de
    sécurité si aucun webhook n'a été reçu depuis POLL_INTERVAL secondes.
    """
    last_api_poll_time = 0.0
    while True:
        if am_i_leader():
            try:
                state = read_state()
                last_sha = state.get("lastDeployedSha", "")
                webhook_sha = state.get("webhookSha", "")

                current_sha = ""
                if webhook_sha and webhook_sha != last_sha:
                    current_sha = webhook_sha
                    log.info("Déclenchement via webhook (sha=%s).", current_sha[:8])
                elif time.time() - last_api_poll_time >= POLL_INTERVAL:
                    last_api_poll_time = time.time()
                    polled_sha = get_latest_commit_sha()
                    if polled_sha != last_sha:
                        current_sha = polled_sha
                        log.info("Déclenchement via polling de secours (sha=%s).", current_sha[:8])
                    else:
                        log.debug("Polling de secours : aucun nouveau commit (sha=%s).", polled_sha[:8])

                if current_sha and current_sha != last_sha:
                    process_commit(current_sha, last_sha)
            except Exception:
                log.exception("Erreur pendant le cycle de pipeline.")
            time.sleep(LEADER_TICK_SECONDS)
        else:
            time.sleep(RETRY_INTERVAL)


def verify_webhook_signature(raw_body: bytes, signature_header: str) -> bool:
    if not GITHUB_WEBHOOK_SECRET:
        # Aucun secret configuré : on accepte quand même (utile pour un test
        # rapide en réseau privé), mais ce n'est pas recommandé même sur un
        # tunnel smee.io — configurez toujours GITHUB_WEBHOOK_SECRET.
        log.warning("GITHUB_WEBHOOK_SECRET non défini — signature non vérifiée !")
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        GITHUB_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length) if length else b""
        signature = self.headers.get("X-Hub-Signature-256", "")

        if not verify_webhook_signature(raw_body, signature):
            log.warning("Webhook reçu avec signature invalide — ignoré.")
            self.send_response(401)
            self.end_headers()
            return

        event = self.headers.get("X-GitHub-Event", "")
        if event == "ping":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"pong")
            return
        if event != "push":
            self.send_response(200)
            self.end_headers()
            return

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning("Webhook : payload JSON invalide.")
            self.send_response(400)
            self.end_headers()
            return

        ref = payload.get("ref", "")
        repo_name = (payload.get("repository") or {}).get("name", "")
        expected_ref = f"refs/heads/{GITHUB_BRANCH}"

        if repo_name != GITHUB_REPO or ref != expected_ref:
            log.info("Webhook ignoré (repo=%s ref=%s ; attendu repo=%s ref=%s).",
                      repo_name, ref, GITHUB_REPO, expected_ref)
            self.send_response(200)
            self.end_headers()
            return

        sha = payload.get("after", "")
        if sha:
            try:
                patch_state({"webhookSha": sha})
                log.info("Webhook accepté : nouveau commit %s sur %s.", sha[:8], repo_name)
            except Exception:
                log.exception("Impossible d'enregistrer le sha reçu par webhook.")

        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        # Coupe les logs HTTP par défaut (bruyants), on logge nous-mêmes ci-dessus.
        pass


def start_webhook_server():
    server = ThreadingHTTPServer(("0.0.0.0", WEBHOOK_PORT), WebhookHandler)
    log.info("Serveur webhook démarré sur le port %s (endpoint /webhook).", WEBHOOK_PORT)
    server.serve_forever()


if __name__ == "__main__":
    log.info("Démarrage de l'orchestrateur CI/CD (pod=%s, ns=%s)", POD_NAME, NAMESPACE)
    log.info("Dépôt surveillé : %s/%s@%s (webhook prioritaire, polling de secours toutes les %ss)",
              GITHUB_OWNER, GITHUB_REPO, GITHUB_BRANCH, POLL_INTERVAL)

    election_thread = threading.Thread(target=leader_election_loop, daemon=True)
    election_thread.start()

    webhook_thread = threading.Thread(target=start_webhook_server, daemon=True)
    webhook_thread.start()

    worker_loop()
