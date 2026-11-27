# Security configuration

## API authentication

The data, anomaly, metric, and prediction routes require an API key in the `X-API-Key` header.
`/health` remains unauthenticated so container and platform health checks can operate without
receiving application credentials. Comparisons use a constant-time function, and startup fails when
authentication is enabled with a missing key or a key shorter than 32 characters.

Generate the local key once:

```bash
make secret
```

This writes `.secrets/api_key` without printing the value. Git ignores the file. Docker Compose
mounts it read-only into the API and dashboard through `/run/secrets/api_key`; it is not placed in
the image or passed as a command-line argument.

The dashboard sends the key only as an HTTP header. Swagger UI exposes an **Authorize** control for
the same header. For a direct API request, read the key into a shell variable and avoid placing it
in source files or shell history.

## Configuration

| Variable | Purpose |
|---|---|
| `API_AUTH_ENABLED` | Enables protected routes; Compose defaults to `true` |
| `API_KEY_FILE` | Host secret path used by Compose |
| `API_KEY` | Direct-value fallback for non-Compose development only |

`API_KEY` and `API_KEY_FILE` are mutually exclusive inside the API process. Authentication can be
disabled explicitly for isolated development, but it should remain enabled in shared environments.

## Rotation

To rotate the local key:

1. Stop the API and dashboard.
2. Move the existing `.secrets/api_key` to a secure recovery location.
3. Run `make secret` to create a replacement.
4. Recreate the API and dashboard containers.
5. Remove the old key after confirming access with the replacement.

In a deployed environment, use the platform's managed secret store and its native rotation
workflow instead of a repository-adjacent file.

## Security boundary

The API key authenticates one internal client identity; it does not provide individual users,
roles, audit identities, or browser login. Internet-facing deployment additionally requires TLS at
an ingress or reverse proxy, network restrictions, rate limiting, and preferably OIDC/OAuth2 with
role-based authorization. PostgreSQL and Redpanda ports should remain on private networks.
