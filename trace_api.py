"""Trace MVP: persistent records, PIN sessions, vision assessments and chain verification."""
import base64
import hashlib
import hmac
import io
import json
import os
import secrets
import sqlite3
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import Response
from pydantic import BaseModel, Field

load_dotenv(Path(__file__).resolve().with_name('.env'))
router = APIRouter(prefix="/api/trace", tags=["UzoAgro Trace"])
lock = threading.RLock()
security = HTTPBearer(auto_error=False)


def now():
    return datetime.now(timezone.utc).isoformat()


def fingerprint(record):
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def db():
    if os.getenv("DATABASE_URL"):
        import psycopg2
        try:
            conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=10)
        except psycopg2.Error:
            raise HTTPException(503, 'The Neon database is temporarily unavailable. Please try again.')
    elif os.getenv("TRACE_SQLITE_PATH"):
        conn = sqlite3.connect(os.getenv("TRACE_SQLITE_PATH", str(Path(__file__).parent / "trace.db")), timeout=20)
    else:
        raise HTTPException(503, "Neon database is not configured. Set DATABASE_URL on the backend.")
    with conn.cursor() if os.getenv("DATABASE_URL") else conn as cur:
        if os.getenv("DATABASE_URL"):
            cur.execute("CREATE TABLE IF NOT EXISTS trace_records (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        else:
            conn.execute("CREATE TABLE IF NOT EXISTS trace_records (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
    conn.commit()
    return conn


def put(key, value):
    conn = db()
    try:
        cur = conn.cursor()
        mark = "%s" if os.getenv("DATABASE_URL") else "?"
        cur.execute(f"INSERT INTO trace_records(id,payload) VALUES ({mark},{mark}) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (key, json.dumps(value)))
        conn.commit()
    finally:
        conn.close()


def get(key):
    conn = db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT payload FROM trace_records WHERE id=" + ("%s" if os.getenv("DATABASE_URL") else "?"), (key,))
        row = cur.fetchone()
        return json.loads(row[0]) if row else None
    finally:
        conn.close()


def all_records(prefix):
    conn = db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT payload FROM trace_records WHERE id LIKE " + ("%s" if os.getenv("DATABASE_URL") else "?"), (prefix + "%",))
        return [json.loads(row[0]) for row in cur.fetchall()]
    finally:
        conn.close()


# Share the existing authenticated farmer identity.
from auth_api import farmer as user


@router.get("/config")
def config():
    return {"contract_address": os.getenv("TRACE_CONTRACT_ADDRESS", ""), "chain_id": 84532, "rpc_url": os.getenv("BASE_SEPOLIA_RPC_URL", "https://sepolia.base.org"), "vision_ready": bool(os.getenv("OPENAI_API_KEY")), "storage": "postgresql" if os.getenv("DATABASE_URL") else "test sqlite" if os.getenv("TRACE_SQLITE_PATH") else "not configured"}


@router.get("/qr")
def qr(url: str, download: bool = False):
    import qrcode
    import qrcode.image.svg
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or len(url) > 1000:
        raise HTTPException(400, "Invalid verification URL.")
    image = qrcode.make(url, image_factory=qrcode.image.svg.SvgPathImage, border=3)
    stream = io.BytesIO()
    image.save(stream)
    headers = {"Content-Disposition": 'attachment; filename="uzoagro-batch.svg"'} if download else {}
    return Response(stream.getvalue(), media_type="image/svg+xml", headers=headers)


class Assessment(BaseModel):
    crop: str = Field(max_length=80)
    condition: str = Field(max_length=200)
    confidence: float = Field(ge=0, le=1)
    recommendation: str = Field(max_length=2000)


@router.post("/diagnose")
async def diagnose(file: UploadFile = File(...), owner=Depends(user)):
    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(503, "Vision model is not configured. Use the labeled sample assessment to explore the workflow.")
    content = await file.read(8 * 1024 * 1024 + 1)
    if len(content) > 8 * 1024 * 1024:
        raise HTTPException(413, "Choose an image smaller than 8 MB.")
    try:
        from PIL import Image
        image = Image.open(io.BytesIO(content))
        if image.format not in ("JPEG", "PNG", "WEBP"):
            raise ValueError()
        image.verify()
        mime = {"JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp"}[image.format]
    except Exception:
        raise HTTPException(400, "Upload a valid JPG, PNG or WebP crop image.")
    try:
        response = requests.post("https://api.openai.com/v1/responses", headers={"Authorization": "Bearer " + os.environ["OPENAI_API_KEY"]}, json={"model": os.getenv("VISION_MODEL", "gpt-4.1-mini"), "store": False, "input": [{"role": "user", "content": [{"type": "input_text", "text": 'Assess this crop image. Return ONLY JSON with crop, condition, confidence (0 to 1, an uncalibrated model estimate), recommendation. Be cautious about uncertainty. If not a crop, state that. Recommend consulting an extension professional, never prescribe pesticide concentrations or destructive actions.'}, {"type": "input_image", "image_url": "data:" + mime + ";base64," + base64.b64encode(content).decode()}]}]}, timeout=60)
        response.raise_for_status()
        output = "".join(c.get("text", "") for item in response.json().get("output", []) for c in item.get("content", []) if c.get("type") == "output_text")
        assessment = Assessment.model_validate_json(output.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()).model_dump()
    except Exception:
        raise HTTPException(502, "The vision service could not return an assessment. Please try again.")
    record = assessment | {"id": "DIA-" + secrets.token_hex(6).upper(), "created_at": now(), "sample": False, "image_hash": hashlib.sha256(content).hexdigest()}
    record["hash"] = fingerprint(record)
    put("diagnosis:" + record["id"], {"owner": owner["phone"], "report": record})
    return record


@router.post("/diagnose/sample")
def sample_diagnosis(owner=Depends(user)):
    record = {"id": "DIA-" + secrets.token_hex(6).upper(), "crop": "Cassava", "condition": "Possible mosaic symptoms â€” sample", "confidence": 0.86, "recommendation": "This is a sample report, not an analysis of your image. For a real assessment, consult an agricultural extension professional.", "created_at": now(), "sample": True}
    record["hash"] = fingerprint(record)
    put("diagnosis:" + record["id"], {"owner": owner["phone"], "report": record})
    return record


class NewBatch(BaseModel):
    crop: str = Field(min_length=2, max_length=64)
    quantity: float = Field(gt=0, le=10000000)
    community: str = Field(min_length=2, max_length=120)
    harvest_date: date
    diagnosis_id: str | None = None


def batch_or_404(batch_id):
    batch = get("batch:" + batch_id)
    if not batch:
        raise HTTPException(404, "Batch not found. Check the ID and try again.")
    return batch


def owned(batch_id, owner):
    batch = batch_or_404(batch_id)
    if batch["owner"] != owner["phone"]:
        raise HTTPException(403, "Only the batch owner can update this record.")
    return batch


def public(batch):
    return {k: v for k, v in batch.items() if k != "owner"}


@router.post("/batches")
def create_batch(data: NewBatch, owner=Depends(user)):
    if data.harvest_date > date.today():
        raise HTTPException(422, "Harvest date cannot be in the future.")
    diagnosis = None
    if data.diagnosis_id:
        stored = get("diagnosis:" + data.diagnosis_id)
        if not stored or stored["owner"] != owner["phone"]:
            raise HTTPException(404, "Assessment not found.")
        diagnosis = stored["report"]
    batch_id = "UZA-" + secrets.token_hex(4).upper()
    metadata = data.model_dump(mode="json", exclude={"diagnosis_id"}) | {"id": batch_id, "farmer": owner["name"], "created_at": now(), "diagnosis": diagnosis}
    batch = {"id": batch_id, "owner": owner["phone"], "metadata": metadata, "hash": fingerprint(metadata), "chain_key": hashlib.sha256(batch_id.encode()).hexdigest(), "transaction": None, "checkpoints": []}
    put("batch:" + batch_id, batch)
    return public(batch)


@router.get("/batches")
def list_batches(owner=Depends(user)):
    return sorted([public(b) for b in all_records("batch:") if b["owner"] == owner["phone"]], key=lambda b: b["metadata"]["created_at"], reverse=True)


@router.get("/batches/{batch_id}")
def read_batch(batch_id: str):
    return public(batch_or_404(batch_id))


@router.get("/batches/{batch_id}/history")
def history(batch_id: str):
    batch = batch_or_404(batch_id)
    return {"batch_id": batch_id, "created_at": batch["metadata"]["created_at"], "checkpoints": batch["checkpoints"]}


class NewCheckpoint(BaseModel):
    stage: str = Field(pattern="^(Picked up|Received at hub|Delivered to buyer)$")
    location: str = Field(min_length=2, max_length=120)


@router.post("/batches/{batch_id}/checkpoints")
def add_checkpoint(batch_id: str, data: NewCheckpoint, owner=Depends(user)):
    with lock:
        batch = owned(batch_id, owner)
        expected = ["Picked up", "Received at hub", "Delivered to buyer"]
        if len(batch["checkpoints"]) >= 3 or data.stage != expected[len(batch["checkpoints"])]:
            raise HTTPException(409, "Record checkpoints in order: pickup, aggregation, delivery.")
        if any(not cp["transaction"] for cp in batch["checkpoints"]) and batch["transaction"]:
            raise HTTPException(409, "Anchor the previous checkpoint first.")
        record = data.model_dump() | {"id": "CP-" + secrets.token_hex(6).upper(), "batch_id": batch_id, "recorded_by": owner["name"], "created_at": now()}
        cp = {"record": record, "hash": fingerprint(record), "transaction": None}
        batch["checkpoints"].append(cp)
        put("batch:" + batch_id, batch)
        return public(batch)


ABI = [
    {"type": "function", "name": "getBatch", "stateMutability": "view", "inputs": [{"name": "id", "type": "bytes32"}], "outputs": [{"type": "tuple", "components": [{"name": "creator", "type": "address"}, {"name": "crop", "type": "string"}, {"name": "dataHash", "type": "bytes32"}, {"name": "createdAt", "type": "uint256"}, {"name": "exists", "type": "bool"}]}]},
    {"type": "function", "name": "getBatchCheckpoints", "stateMutability": "view", "inputs": [{"name": "id", "type": "bytes32"}], "outputs": [{"type": "tuple[]", "components": [{"name": "stage", "type": "string"}, {"name": "location", "type": "string"}, {"name": "dataHash", "type": "bytes32"}, {"name": "timestamp", "type": "uint256"}, {"name": "recordedBy", "type": "address"}]}]},
]


def chain():
    from web3 import Web3
    address = os.getenv("TRACE_CONTRACT_ADDRESS")
    if not address:
        raise HTTPException(503, "Deploy the contract and configure TRACE_CONTRACT_ADDRESS first.")
    w3 = Web3(Web3.HTTPProvider(os.getenv("BASE_SEPOLIA_RPC_URL", "https://sepolia.base.org"), request_kwargs={"timeout": 15}))
    if w3.eth.chain_id != 84532:
        raise HTTPException(503, "RPC must use Base Sepolia.")
    return w3, w3.eth.contract(address=Web3.to_checksum_address(address), abi=ABI)


class Anchor(BaseModel):
    transaction: str = Field(pattern=r"^0x[0-9a-fA-F]{64}$")
    checkpoint_id: str | None = None


@router.post("/batches/{batch_id}/anchor")
def anchor(batch_id: str, data: Anchor, owner=Depends(user)):
    with lock:
        batch = owned(batch_id, owner)
        target = batch
        index = None
        if data.checkpoint_id:
            matches = [(i, cp) for i, cp in enumerate(batch["checkpoints"]) if cp["record"]["id"] == data.checkpoint_id]
            if not matches or not batch["transaction"]:
                raise HTTPException(409, "Anchor the batch before its checkpoints.")
            index, target = matches[0]
        try:
            w3, contract = chain()
            receipt = w3.eth.get_transaction_receipt(data.transaction)
            tx = w3.eth.get_transaction(data.transaction)
            if receipt["status"] != 1 or not tx["to"] or tx["to"].lower() != contract.address.lower():
                raise HTTPException(422, "Transaction was not a successful call to this contract.")
            expected_event = w3.keccak(text="CheckpointAdded(bytes32,uint256,bytes32)" if index is not None else "BatchCreated(bytes32,address,bytes32)")
            logs = [entry for entry in receipt["logs"] if entry["address"].lower() == contract.address.lower() and entry["topics"][0] == expected_event and entry["topics"][1].hex().removeprefix("0x") == batch["chain_key"]]
            if not logs:
                raise HTTPException(422, "Transaction does not contain this batch event.")
            event_data = bytes(logs[0]["data"])
            if event_data[-32:].hex() != target["hash"] or (index is not None and int.from_bytes(event_data[:32]) != index):
                raise HTTPException(422, "Transaction fingerprint does not match this record.")
            state = contract.functions.getBatch(bytes.fromhex(batch["chain_key"])).call()
            if not state[4] or bytes(state[2]).hex() != batch["hash"]:
                raise HTTPException(422, "On-chain batch fingerprint does not match.")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(502, "Cannot confirm the transaction yet. Retry after confirmation.")
        target["transaction"] = data.transaction
        put("batch:" + batch_id, batch)
        return public(batch)


@router.get("/batches/{batch_id}/verify")
def verify(batch_id: str):
    batch = batch_or_404(batch_id)
    intact = hmac.compare_digest(fingerprint(batch["metadata"]), batch["hash"]) and all(fingerprint(cp["record"]) == cp["hash"] for cp in batch["checkpoints"])
    result = {"integrity": intact, "blockchain_verified": False, "checkpoints_verified": False, "status": "Unanchored", "batch": public(batch)}
    if not intact:
        result["status"] = "Fingerprint mismatch"
    elif batch["transaction"]:
        try:
            _, contract = chain()
            state = contract.functions.getBatch(bytes.fromhex(batch["chain_key"])).call()
            result["blockchain_verified"] = bool(state[4]) and bytes(state[2]).hex() == batch["hash"]
            checkpoints = contract.functions.getBatchCheckpoints(bytes.fromhex(batch["chain_key"])).call()
            result["checkpoints_verified"] = len(checkpoints) == len(batch["checkpoints"]) and all(bytes(chain_cp[2]).hex() == cp["hash"] for chain_cp, cp in zip(checkpoints, batch["checkpoints"]))
            result["status"] = "Blockchain verified" if result["blockchain_verified"] else "Fingerprint mismatch"
        except Exception:
            result["status"] = "Network unavailable â€” verification pending"
    return result
