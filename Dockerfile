FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Utilisateur non-root par bonne pratique (l'orchestrateur n'a besoin d'aucun
# privilège particulier au niveau OS, seulement des permissions RBAC Kubernetes)
RUN useradd -u 1000 -m orchestrator
USER 1000

CMD ["python", "app.py"]