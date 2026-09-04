# syntax=docker/dockerfile:1

# Stage 1: Install Google Cloud SDK using APT
FROM python:3.14.5-slim AS gcloud-apt-install

# Keep in sync with _GCLOUD_VERSION in sky/clouds/gcp.py. Pinned so the apt
# install layer doesn't bake in a stale version via buildx registry caching
# (the RUN command's hash is the cache key, so without a version specifier
# the layer is reused indefinitely from whatever apt resolved at first build).
# 567.0.0 ships gsutil 5.37, which replaced OpenSSL.crypto.sign with the
# cryptography library — required after pyopenssl 24.3 dropped that API (#8070).
ARG GCLOUD_VERSION=567.0.0-0

RUN apt-get update && \
    apt-get install -y curl gnupg lsb-release && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && \
    apt-get install --no-install-recommends -y \
        google-cloud-cli=${GCLOUD_VERSION} \
        google-cloud-cli-gke-gcloud-auth-plugin=${GCLOUD_VERSION} && \
    apt-get clean && rm -rf /usr/lib/google-cloud-sdk/platform/bundledpythonunix \
    /var/lib/apt/lists/*


# Stage 2: Process the source code for INSTALL_FROM_SOURCE
FROM python:3.14.5-slim AS process-source

# Control installation method - default to install from source
ARG INSTALL_FROM_SOURCE=true
ARG NEXT_BASE_PATH=/dashboard
ARG INSTALL_BOLTZ_RECLAIM_POLICY=false
ARG SKYPILOT_VERSION
ARG SKYPILOT_COMMIT_SHA
ARG SKYPILOT_COMMIT_TIMESTAMP
ARG SKYPILOT_COMMIT_COUNT
WORKDIR /skypilot

# New Python slim images do not guarantee setuptools is preinstalled. setup.py
# imports it while replacing the source commit hash, so pin it explicitly.
RUN python -m pip install --no-cache-dir setuptools==78.1.1

# Run NPM and node install in a separate step for caching.
RUN if [ "$INSTALL_FROM_SOURCE" = "true" ]; then \
        echo "Installing NPM and Node.js for dashboard build" && \
        apt-get update -y && \
        apt-get install --no-install-recommends -y git curl ca-certificates gnupg && \
        curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
        apt-get install -y nodejs && \
        apt-get clean && rm -rf /var/lib/apt/lists/*; \
fi

COPY sky/dashboard/package.json sky/dashboard/package-lock.json \
    /skypilot/sky/dashboard/

RUN if [ "$INSTALL_FROM_SOURCE" = "true" ]; then \
        echo "Installing dashboard dependencies in Stage 2" && \
        npm --prefix sky/dashboard ci --no-audit --fund=false; \
    fi

COPY sky/dashboard /skypilot/sky/dashboard

RUN --mount=type=cache,id=dashboard-next-cache,target=/skypilot/sky/dashboard/.next/cache \
    if [ "$INSTALL_FROM_SOURCE" = "true" ]; then \
        echo "Building dashboard in Stage 2" && \
        NEXT_BASE_PATH=${NEXT_BASE_PATH} npm --prefix sky/dashboard run build && \
        echo "Cleaning up dashboard build-time dependencies" && \
        rm -rf sky/dashboard/node_modules ~/.npm /root/.npm; \
    fi

COPY . /skypilot

# The source image keeps the repository metadata, but .dockerignore omits
# tracked documentation and test trees. Restore only those omitted paths while
# recording the commit so they do not make every clean image look dirty, then
# remove them again before the layer is committed.
RUN cd /skypilot && \
    if [ "$INSTALL_FROM_SOURCE" != "true" ]; then \
        echo "Removing source code (wheel installation)" && \
        # Retain an /skypilot/dist dir to keep compatibility in stage 3.
        mv /skypilot/dist /dist.backup && cd .. && rm -rf /skypilot && mkdir /skypilot && mv /dist.backup /skypilot/dist; \
    else \
        echo "Keeping source code and record commit sha (editable installation)" && \
        if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
            git ls-files --deleted -z -- .github docs examples llm tests \
                > /tmp/skypilot-docker-omitted-files && \
            git checkout-index --force --stdin -z \
                < /tmp/skypilot-docker-omitted-files && \
            python -c "import setup; setup.replace_commit_hash()" && \
            xargs -0 -r rm -f < /tmp/skypilot-docker-omitted-files && \
            rm -f /tmp/skypilot-docker-omitted-files; \
        else \
            python -c "import setup; setup.replace_commit_hash()"; \
        fi && \
        # Remove .git dir to reduce the final image size
        rm -rf .git; \
    fi

# setup.py's generic source materialization above must run first: it derives
# identity from Git and would otherwise overwrite an exact deployment stamp.
# A deployment release may now project one immutable identity across Python and
# OCI metadata. Generic builds leave every argument empty, making this a no-op.
RUN if [ "$INSTALL_FROM_SOURCE" = "true" ]; then \
      python boltz/stamp_image_release.py \
        --root /skypilot \
        --version "$SKYPILOT_VERSION" \
        --commit "$SKYPILOT_COMMIT_SHA" \
        --commit-timestamp "$SKYPILOT_COMMIT_TIMESTAMP" \
        --commit-count "$SKYPILOT_COMMIT_COUNT" \
        --install-policy "$INSTALL_BOLTZ_RECLAIM_POLICY"; \
    elif [ "$INSTALL_BOLTZ_RECLAIM_POLICY" != "false" ] || \
         [ -n "$SKYPILOT_VERSION$SKYPILOT_COMMIT_SHA$SKYPILOT_COMMIT_TIMESTAMP$SKYPILOT_COMMIT_COUNT" ]; then \
      echo "Error: exact release stamping requires INSTALL_FROM_SOURCE=true" >&2; \
      exit 1; \
    fi


# Stage 3: Main image
FROM python:3.14.5-slim

ARG INSTALL_FROM_SOURCE=true
ARG INSTALL_BOLTZ_RECLAIM_POLICY=false
ARG SKYPILOT_VERSION
ARG SKYPILOT_COMMIT_SHA
ARG SKYPILOT_COMMIT_TIMESTAMP
ARG SKYPILOT_COMMIT_COUNT
ARG SKYPILOT_EXTRAS=all

LABEL org.opencontainers.image.version="${SKYPILOT_VERSION}" \
      org.opencontainers.image.revision="${SKYPILOT_COMMIT_SHA}" \
      bio.boltz.skypilot.commit-timestamp="${SKYPILOT_COMMIT_TIMESTAMP}" \
      bio.boltz.skypilot.commit-count="${SKYPILOT_COMMIT_COUNT}"

# Copy Google Cloud SDK from Stage 1
COPY --from=gcloud-apt-install /usr/lib/google-cloud-sdk /opt/google-cloud-sdk

# Set environment variable
ENV PATH="/opt/google-cloud-sdk/bin:$PATH"

# Detect architecture
ARG TARGETARCH

# Control Next.js basePath for staging deployments
ARG NEXT_BASE_PATH=/dashboard

# Install system packages
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install --no-install-recommends -y \
        git gcc rsync sudo patch openssh-server \
        pciutils nano fuse socat netcat-openbsd curl tini autossh jq logrotate && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install the session manager plugin for AWS CLI.
RUN ARCH=$(case "${TARGETARCH:-$(uname -m)}" in \
        "amd64"|"x86_64") echo "64bit" ;; \
        "aarch64") echo "arm64" ;; \
        *) echo "${TARGETARCH:-$(uname -m)}" ;; \
    esac) && \
    echo "Installing session manager plugin for AWS CLI for ${ARCH}" && \
    curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_${ARCH}/session-manager-plugin.deb" -o "session-manager-plugin.deb" && \
    sudo dpkg -i session-manager-plugin.deb && \
    rm session-manager-plugin.deb

# Install kubectl based on architecture
RUN ARCH=${TARGETARCH:-$(case "$(uname -m)" in \
        "x86_64") echo "amd64" ;; \
        "aarch64") echo "arm64" ;; \
        *) echo "$(uname -m)" ;; \
    esac)} && \
    curl -LO "https://dl.k8s.io/release/v1.33.7/bin/linux/$ARCH/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl

# Install Nebius CLI
RUN curl -sSL https://storage.eu-north1.nebius.cloud/cli/install.sh | NEBIUS_INSTALL_FOLDER=/usr/local/bin bash
# Install uv. Azure CLI's pinned dependency graph requires prereleases, so
# preseed it only for images which actually request the Azure or all extra.
# The Boltz control-plane image requests only AWS, GCP, and Kubernetes.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    case ",${SKYPILOT_EXTRAS}," in \
      *,all,*|*,azure,*) \
        ~/.local/bin/uv pip install --prerelease allow \
          "azure-cli<2.87.0" --system ;; \
    esac && \
    # Upgrade setuptools in base image to mitigate CVE-2024-6345
    ~/.local/bin/uv pip install --system --upgrade setuptools==78.1.1 && \
    ~/.local/bin/uv cache clean && \
    rm -rf ~/.cache/pip ~/.cache/uv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add source code
COPY --from=process-source /skypilot /skypilot

# Install SkyPilot and set up dashboard based on installation method
RUN cd /skypilot && \
    if [ "$INSTALL_FROM_SOURCE" = "true" ]; then \
        echo "Installing from source in editable mode" && \
        ~/.local/bin/uv pip install -e ".[${SKYPILOT_EXTRAS}]" --system; \
    else \
        echo "Installing from wheel file" && \
        WHEEL_FILE=$(ls dist/*skypilot*.whl 2>/dev/null | head -1) && \
        if [ -z "$WHEEL_FILE" ]; then \
            echo "Error: No wheel file found in /skypilot/dist/" && \
            ls -la /skypilot/dist/ && \
            exit 1; \
        fi && \
        ~/.local/bin/uv pip install \
          "${WHEEL_FILE}[${SKYPILOT_EXTRAS}]" --system && \
        echo "Skipping dashboard build for wheel installation"; \
    fi && \
    if [ "$INSTALL_BOLTZ_RECLAIM_POLICY" = "true" ]; then \
        if [ "$INSTALL_FROM_SOURCE" != "true" ]; then \
            echo "Error: the Boltz reclaim policy requires the source image" >&2; \
            exit 1; \
        fi; \
        echo "Installing the Boltz reserved-fill reclaim policy" && \
        ~/.local/bin/uv pip install --no-deps --no-build-isolation \
          /skypilot/boltz/reserved_fill_reclaim_policy --system; \
    elif [ "$INSTALL_BOLTZ_RECLAIM_POLICY" != "false" ]; then \
        echo "Error: INSTALL_BOLTZ_RECLAIM_POLICY must be true or false" >&2; \
        exit 1; \
    fi && \
    # Cleanup all caches to reduce the image size
    ~/.local/bin/uv cache clean && \
    rm -rf ~/.cache/pip ~/.cache/uv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    # Remove the empty /skypilot dir for backward compatibility
    if [ "$INSTALL_FROM_SOURCE" != "true" ]; then \
        rm -rf /skypilot; \
    fi
