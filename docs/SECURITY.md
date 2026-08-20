# Where credentials live

Every place a secret exists in this system, what form it takes there, and what
an attacker gets if they reach it.

## User passwords

**Stored as bcrypt hashes in the Postgres `users` table. Never in plaintext,
anywhere.**

```
username | roles        | password_hash
---------+--------------+----------------------------------
admin    | ["admin"]    | $2b$12$FgTKg15cFKA2MeQfpHbHMut1k…
```

`$2b$12$` is bcrypt at cost factor 12. Passwords are SHA-256 pre-hashed and
base64-encoded before hashing, because bcrypt silently truncates at 72 bytes —
without the pre-hash, two different long passwords can produce the same hash.

A database dump yields hashes, not passwords. Login does not reveal whether a
username exists, and hashes on the missing-user path so response timing does
not reveal it either.

## The browser

**localStorage holds a signed JWT, never a password.** Its full contents:

```json
{"sub":"local:admin","name":"Bootstrap administrator","email":"",
 "roles":["admin"],"iss":"kafka-guardian","exp":1787268079,"iat":1787224879}
```

Identity and roles only. It is signed with `AUTH_JWT_SECRET`, so it cannot be
forged or have its roles edited, and it expires after 12 hours.

Storing a bearer token in localStorage is a deliberate trade-off: it is
readable by any script that achieves XSS on this origin. The alternative is an
httpOnly cookie, which requires the API to set it and brings CSRF handling
with it. For a dashboard on your own network this is the right cost; for a
public deployment, move to cookies.

## Plugin credentials (Kafka API keys, Anthropic keys, webhook URLs)

**Fernet-encrypted in the `plugin_config` table**, keyed from
`GUARDIAN_SECRET_KEY`:

```json
{"api_key": "enc:v1:gAAAAABqhlsieGTY5-UAxPAglW2GYL8ny…"}
```

The browser only ever receives a masked form (`••••1234`), and a masked value
submitted back is discarded rather than overwriting the real one.

**The honest limit:** if `GUARDIAN_SECRET_KEY` lives in the same `.env` as the
database password, an attacker with the host has both. This protects against a
database dump, not against host compromise. `SECRETS_BACKEND=kms` is the seam
for a KMS where the key never enters the process; it is documented and
deliberately unimplemented rather than faked.

## Infrastructure secrets

`AUTH_JWT_SECRET`, `GUARDIAN_SECRET_KEY`, `BOOTSTRAP_ADMIN_PASSWORD` and
`KAFKA_SASL_PASSWORD` live in `.env`, in plaintext.

- `.env` is gitignored and has never been committed.
- It should be mode `600`. Check with `ls -l .env`.

**They are visible to `docker inspect`.** Compose passes environment variables
to containers, and anyone in the `docker` group can read every one in
plaintext from outside the container:

```bash
docker inspect kga-api --format '{{range .Config.Env}}{{println .}}{{end}}'
```

This is inherent to env-var configuration, not something the application can
fix. The way out is the `*_FILE` convention below.

### Using real secret stores

Any of those variables can be supplied as a file instead, which is how Docker
secrets, Kubernetes secret volumes and Vault Agent all deliver them:

```yaml
services:
  api:
    environment:
      AUTH_JWT_SECRET_FILE: /run/secrets/auth_jwt_secret
      GUARDIAN_SECRET_KEY_FILE: /run/secrets/guardian_key
    secrets: [auth_jwt_secret, guardian_key]

secrets:
  auth_jwt_secret:
    file: ./secrets/auth_jwt_secret
  guardian_key:
    file: ./secrets/guardian_key
```

A readable `*_FILE` always takes precedence over the plain variable, so a
migration can set both and the safer source wins. The value never appears in
`docker inspect` or a process listing.

## The bootstrap password

`BOOTSTRAP_ADMIN_PASSWORD` is read **only when the users table is empty**.
Once the first admin exists it does nothing, and leaving it set is a standing
plaintext copy of a live admin password. The API logs a warning on every start
while it remains set. Remove it from `.env` after first login.

There is no default. A shipped default admin password is a shipped
vulnerability — every deployment that forgets to change it is compromised by
anyone who has read this repository.

## Rotating

| Secret | How | Effect |
|---|---|---|
| A user's password | No self-service endpoint yet — recreate the user | — |
| `AUTH_JWT_SECRET` | Change it and restart the API | All sessions invalidated; everyone signs in again |
| `GUARDIAN_SECRET_KEY` | Change it and restart | **Stored plugin credentials become unreadable** and must be re-entered. The API logs `secret_decrypt_failed` rather than failing silently. |
| A plugin credential | Re-enter it on the Connections page | Overwrites the encrypted value |

## Known gaps

Honest list of what is not done:

- **No password self-service.** No change-password or reset flow; users are
  created by an admin via `POST /api/auth/users`.
- **No account lockout or login rate limiting.** Brute-forcing a weak password
  over the API is currently unthrottled. bcrypt cost 12 makes it slow, not
  impossible.
- **No MFA** in local mode. Use `AUTH_MODE=oidc` and let your IdP enforce it.
- **No session revocation.** A JWT stays valid until it expires; deleting a
  user does not invalidate their existing token.
- **The audit log is tamper-evident, not tamper-proof.** See
  [ARCHITECTURE.md](ARCHITECTURE.md).
