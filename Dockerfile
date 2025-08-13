FROM quay.io/astronomer/astro-runtime:13.1.0

# Instalar gcloud
USER root
RUN apt-get update -y && \
apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    gnupg \
    curl && \
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list && \
apt-get update -y && \
apt-get install -y --no-install-recommends google-cloud-cli && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

#Desactivar los prompts interactivos
ENV CLOUDSDK_CORE_DISABLE_PROMPTS=1

# Volver al usuario astro
USER astro