#!/usr/bin/env python3
"""
create_encrypted_pd.py — Produce AES-GCM encrypted ``.enc`` files for the
document-ingestion pipeline's bronze layer.

The bronze layer (``bronze_ingestion.py``) reads ``*.enc`` files as raw bytes
and decrypts them with:

    AESGCM(key).decrypt(data[:12], data[12:], None)

so every ``.enc`` file this script writes has the byte layout:

    nonce (12 bytes)  ||  AESGCM(key).encrypt(nonce, pdf_bytes, None)

(the AES-GCM ciphertext already carries its 16-byte auth tag appended).

The key MUST match ``key_b64`` in ``config.py`` / ``bronze_ingestion.py`` —
otherwise the pipeline can't decrypt. For this demo it is a hardcoded shared
key; see SECURITY NOTE below.

⚠️  SECURITY NOTE — the AES key is hardcoded here and across the pipeline source
    purely so the self-contained demo runs without external setup. In any real
    AMEX deployment the key MUST come from a Databricks secret scope / cloud KMS,
    never from source. The synthetic PDFs contain only fabricated PII, so the
    encrypted samples are safe to share externally as a demo dataset.

----------------------------------------------------------------------------
Usage (local) — encrypt every PDF in a directory into .enc files:

    python create_encrypted_pd.py \
        --input-dir  ../../pdfs \
        --output-dir ./encrypted_pdfs

Encrypt a single file:

    python create_encrypted_pd.py --input-file foo.pdf --output-dir ./encrypted_pdfs

Verify a round-trip (encrypt → decrypt → byte-compare) without writing:

    python create_encrypted_pd.py --input-file foo.pdf --self-test

Then upload the .enc files to the source volume, e.g.:

    databricks -p e2-demo-field-eng fs cp -r ./encrypted_pdfs \
        dbfs:/Volumes/mason_demo_catalog/amex_enc_demo/vol1/encrypted_pdfs
----------------------------------------------------------------------------
"""

import argparse
import base64
import os
import sys

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# Must match key_b64 in config.py / bronze_ingestion.py / silver / gold.
KEY_B64 = "0EiSK5fLsVLlQZNl1+8obChaZYjADvyJutadxb2yb40="
NONCE_LEN = 12  # bronze decrypt_udf slices data[:12] as the nonce


def encrypt_bytes(plaintext: bytes, key: bytes) -> bytes:
    """Return nonce || AESGCM ciphertext(+tag), matching the bronze UDF."""
    nonce = os.urandom(NONCE_LEN)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)
    return nonce + ciphertext


def decrypt_bytes(data: bytes, key: bytes) -> bytes:
    """Mirror of bronze_ingestion.decrypt_udf — for the self-test only."""
    return AESGCM(key).decrypt(data[:NONCE_LEN], data[NONCE_LEN:], None)


def encrypt_file(in_path: str, out_dir: str, key: bytes) -> str:
    with open(in_path, "rb") as f:
        plaintext = f.read()
    enc = encrypt_bytes(plaintext, key)
    # round-trip guard so we never ship a file the pipeline can't read
    assert decrypt_bytes(enc, key) == plaintext, f"round-trip failed for {in_path}"
    base = os.path.splitext(os.path.basename(in_path))[0]
    out_path = os.path.join(out_dir, f"{base}.enc")
    with open(out_path, "wb") as f:
        f.write(enc)
    print(f"  {os.path.basename(in_path)} ({len(plaintext):,} B) -> {base}.enc ({len(enc):,} B)")
    return out_path


def main() -> int:
    p = argparse.ArgumentParser(description="Encrypt PDFs into .enc for the ingestion pipeline.")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-dir", help="Directory of .pdf files to encrypt")
    src.add_argument("--input-file", help="Single .pdf file to encrypt")
    p.add_argument("--output-dir", default="./encrypted_pdfs", help="Where to write .enc files")
    p.add_argument("--self-test", action="store_true",
                   help="Encrypt+decrypt+compare in memory; write nothing")
    args = p.parse_args()

    key = base64.b64decode(KEY_B64)
    assert len(key) == 32, "expected a 256-bit (32-byte) AES key"

    if args.input_dir:
        files = sorted(
            os.path.join(args.input_dir, f)
            for f in os.listdir(args.input_dir)
            if f.lower().endswith(".pdf")
        )
    else:
        files = [args.input_file]

    if not files:
        print("No PDF files found.", file=sys.stderr)
        return 1

    if args.self_test:
        for f in files:
            with open(f, "rb") as fh:
                pt = fh.read()
            assert decrypt_bytes(encrypt_bytes(pt, key), key) == pt, f"FAIL {f}"
            print(f"  self-test OK: {os.path.basename(f)}")
        print(f"\nSelf-test passed for {len(files)} file(s).")
        return 0

    os.makedirs(args.output_dir, exist_ok=True)
    print(f"Encrypting {len(files)} PDF(s) -> {args.output_dir}")
    for f in files:
        encrypt_file(f, args.output_dir, key)
    print(f"\nDone. {len(files)} .enc file(s) written to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
