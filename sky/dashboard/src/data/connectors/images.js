import { apiClient } from '@/data/connectors/client';

export class ImageApiError extends Error {
  constructor(code, status) {
    super(code || `IMAGE_API_HTTP_${status}`);
    this.name = 'ImageApiError';
    this.code = code || `IMAGE_API_HTTP_${status}`;
    this.status = status;
  }
}

async function decode(response) {
  if (response.ok) {
    return await response.json();
  }
  let code = null;
  try {
    const payload = await response.json();
    code = payload?.detail?.code || payload?.code || null;
  } catch {
    // The direct API deliberately does not require parsing provider text.
  }
  throw new ImageApiError(code, response.status);
}

function withQuery(path, query = {}) {
  const params = new URLSearchParams();
  Object.entries(query).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      params.set(key, String(value));
    }
  });
  const encoded = params.toString();
  return encoded ? `${path}?${encoded}` : path;
}

async function get(path, query, signal) {
  return decode(await apiClient.get(withQuery(path, query), { signal }));
}

async function mutate(path, body, idempotencyKey, signal) {
  return decode(
    await apiClient.post(path, body, {
      signal,
      headers: { 'Idempotency-Key': idempotencyKey },
    })
  );
}

export function newIdempotencyKey() {
  return (
    globalThis.crypto?.randomUUID?.() ||
    `dashboard-${Date.now()}-${Math.random().toString(16).slice(2)}`
  );
}

export const getImageCapabilities = (workspace, signal) =>
  get('/images/capabilities', { workspace }, signal);

export const getImageCatalog = (options = {}, signal) =>
  get('/images/catalog', options, signal);

export const getImagePublications = (options = {}, signal) =>
  get('/images/publications', options, signal);

export const getImageReadiness = (workspace, signal) =>
  get('/images/readiness', { workspace }, signal);

export const getImageOperation = (operationId, workspace, signal) =>
  get(
    `/images/operations/${encodeURIComponent(operationId)}`,
    { workspace },
    signal
  );

export async function getImageArtifactDetail(imageId, workspace, signal) {
  const encoded = encodeURIComponent(imageId);
  const query = { workspace, limit: 100 };
  const [artifact, releases, sources, publications, locations, demands] =
    await Promise.all([
      get(`/images/artifacts/${encoded}`, { workspace }, signal),
      get(`/images/artifacts/${encoded}/releases`, query, signal),
      get(`/images/artifacts/${encoded}/sources`, query, signal),
      get(`/images/artifacts/${encoded}/publications`, query, signal),
      get(`/images/artifacts/${encoded}/locations`, query, signal),
      get(`/images/artifacts/${encoded}/demands`, query, signal),
    ]);
  return {
    artifact: artifact.artifact,
    releases: releases.items,
    sources: sources.items,
    publications: publications.items,
    locations: locations.items,
    demands: demands.items,
    truncated: Boolean(
      releases.next_cursor ||
        sources.next_cursor ||
        publications.next_cursor ||
        locations.next_cursor ||
        demands.next_cursor
    ),
  };
}

export const publishImage = (body, idempotencyKey, signal) =>
  mutate('/images/publications', body, idempotencyKey, signal);

export const prepareImage = (imageId, body, idempotencyKey, signal) =>
  mutate(
    `/images/artifacts/${encodeURIComponent(imageId)}/prepare`,
    body,
    idempotencyKey,
    signal
  );

export const retryImagePublication = (
  publicationId,
  workspace,
  idempotencyKey,
  signal
) =>
  mutate(
    `/images/publications/${encodeURIComponent(publicationId)}/retry`,
    { workspace },
    idempotencyKey,
    signal
  );

export const retryImageLocation = (
  locationId,
  workspace,
  idempotencyKey,
  signal
) =>
  mutate(
    `/images/locations/${encodeURIComponent(locationId)}/retry`,
    { workspace },
    idempotencyKey,
    signal
  );

export const qualifyImageProfile = (
  profile,
  manifest,
  idempotencyKey,
  signal
) =>
  mutate(
    `/images/profiles/${encodeURIComponent(profile)}/qualification`,
    { manifest },
    idempotencyKey,
    signal
  );

export const canaryImageProfile = (profile, body, idempotencyKey, signal) =>
  mutate(
    `/images/profiles/${encodeURIComponent(profile)}/canaries`,
    body,
    idempotencyKey,
    signal
  );
