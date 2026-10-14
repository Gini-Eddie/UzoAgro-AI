# UzoAgro Trace

See [LOGISTICS_README.md](LOGISTICS_README.md) for the integrated farmer/driver workflow, dashboard charts and batch-to-transport linking.

An extension of the existing FastAPI logistics app: crop image → assessment → batch → fingerprint → testnet anchor → checkpoints → public QR verification.

## Local preview

```powershell
uv venv .trace-venv
uv pip install --python .trace-venv/Scripts/python.exe -r requirements.txt
.trace-venv/Scripts/python.exe -m uvicorn trace_server:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000. The workspace is `/trace.html`. Existing logistics remain in `/logistics.html` and `/dashboard.html` (their existing API configuration is retained).

## Shared Neon accounts

Set the existing `DATABASE_URL` in `.env` before trying to sign in locally. No SQLite account fallback is enabled. Both applications use the existing Neon `users` table and four-digit PINs. Trace's earlier six-digit demo accounts and tokens do not grant access to the unified application.

The landing page's **Explore your workspace** button opens `login.html`. Successful login opens the existing personalized dashboard. Farmers can access Trace assessments and batches there; drivers keep their driver workspace and can use public verification. Direct visits to private Trace pages redirect to the same login. A validated farmer can return to a requested Trace page after login.

The backend adds nullable `email`, `pin`, `pin_hash` and `pin_salt` columns if missing, plus `user_sessions`, `login_attempts` and `trace_records` tables. Existing users, roles and PINs are retained. Existing plaintext PINs gain a PBKDF2 hash on successful login without changing the user's PIN; new accounts store only hashes. Sessions expire after 24 hours, and `/api/me` reads the authoritative role from Neon on every validation. Sign-out invalidates that session for both workspaces.

The local preview now serves the existing logistics API and Trace together using `trace_server.py`. It no longer starts with the Neon connection disabled. If `DATABASE_URL` is absent, pages still load, but account APIs show an explicit configuration error. SQLite record storage is limited to tests using an explicit `TRACE_SQLITE_PATH`.

For the hosted Vercel frontend, shared `auth.js` targets the existing Hugging Face backend. Deploy the updated backend **before** the frontend: old backend versions do not issue session tokens or implement `/api/me`. Custom hosting can set `window.UZOAGRO_API_BASE` before `auth.js` loads. Both clients use this same base. Existing Paystack signup UI is retained; this change does not introduce a payment or KYC implementation.

## Vision

Set `OPENAI_API_KEY` and optionally `VISION_MODEL` in `.env`. Actual image assessment uses the Responses API, validates image bytes and structured output, and stores a hashed report. Uploads are sent to the vision provider, not stored publicly. Provider confidence is an uncalibrated estimate, not a diagnostic guarantee. Without credentials, the real assessment action is unavailable; an explicit sample action provides a labeled fixture. A sample stays labeled in the batch passport.

## Blockchain

```powershell
npm install
npm run contracts:test
# Configure DEPLOYER_PRIVATE_KEY with a funded Base Sepolia test wallet.
npm run contracts:deploy
```

Set the printed `TRACE_CONTRACT_ADDRESS` in `.env` and restart the backend. Base Sepolia chain ID is 84532. Connect MetaMask and confirm a batch transaction in its detail page. Only crop labels and SHA-256 fingerprints are placed on-chain. Contract checkpoints contain a hub/community label; never enter private addresses there.

The backend verifies successful receipts, contract address, batch events, fingerprints, and on-chain state before saving an anchor. A submitted transaction hash is retained in the browser tab so confirmation can be retried without paying for another transaction. The public verifier recomputes the database fingerprint and independently reads the configured contract. Batch and checkpoint verification are separate; unanchored checkpoints are clearly marked. Blockchain shows tamper-evidence after submission, not the truth of farmer-entered claims.

One immutable batch record and append-only checkpoints are supported by `UzoAgroTrace.sol`. A creator can authorize wallet operators via `setOperator`. In this MVP, application writes are managed by the farmer account; role handover for independent aggregator accounts is a future enhancement.

## Data & authentication

Accounts and roles come exclusively from the existing Neon user table. The cached browser profile is for display only; it cannot grant access. Farmer-only API dependencies enforce assessments and batch writes on the server. Public verification remains available without login. The farmer's public name and community appear on the public passport; phone numbers and PINs do not.

Trace's simple record storage is suitable for a single-worker hackathon MVP. Before production, replace its JSON record table with normalized tables, database transactions/row locking, secure account recovery, distributed rate limiting and audited identity/role management. SQLite requires a persistent filesystem; use PostgreSQL on ephemeral deployments. Frontend fonts and wallet tooling need internet access; records and QR generation are server-backed.

## Checks

```powershell
uv pip install --python .trace-venv/Scripts/python.exe httpx
.trace-venv/Scripts/python.exe -m unittest discover -s tests -v
node --check frontend/trace.js
npm run contracts:test
```

An optional complete blockchain integration check uses two terminals:

```powershell
npx hardhat node
# In a second terminal:
.trace-venv/Scripts/python.exe scripts/verify_local_chain.py
```

This deploys a disposable contract and exercises all receipt, event, batch and checkpoint verification against a local chain configured with Base Sepolia's chain ID. It does not use a testnet wallet or change application records.

Implementation references: [Base chain IDs](https://docs.base.org/base-chain/api-reference/ethereum-json-rpc-api/eth_chainId), [ethers v6](https://docs.ethers.org/v6/single-page/), [OpenAI image inputs](https://developers.openai.com/api/docs/guides/images-vision).

API tests use isolated account fixtures that simulate the existing PostgreSQL queries, plus temporary Trace storage. They do not touch Neon. No testnet deployment or vision calls are made by tests.
