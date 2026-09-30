import urllib.request
import hashlib
import json
import os
from pathlib import Path

TOKEN = "nfc_DSmCh7JTojTcjqYzrskVKJSjeUbASCykadc5"
SITE_ID = "394aee28-0ab5-4cbc-b061-35ded197dcf1"
HTML_PATH = Path(__file__).resolve().parent / "index.html"
if not HTML_PATH.exists():
    HTML_PATH = Path("c:/AT/Github/anhmoe/index.html")

def deploy():
    if not HTML_PATH.exists():
        print(f"Error: {HTML_PATH} does not exist!")
        return

    content = HTML_PATH.read_bytes()
    sha1 = hashlib.sha1(content).hexdigest()
    print(f"Deploying {HTML_PATH.name} ({len(content):,} bytes, SHA1: {sha1[:8]}...) to Netlify...")

    # 1. Create deploy manifest
    create_url = f"https://api.netlify.com/api/v1/sites/{SITE_ID}/deploys"
    manifest = json.dumps({"files": {"/index.html": sha1}}).encode("utf-8")
    req = urllib.request.Request(
        create_url,
        data=manifest,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(req) as resp:
        deploy_info = json.loads(resp.read().decode())
        deploy_id = deploy_info["id"]
        required = deploy_info.get("required", [])

    # 2. Upload file if needed
    if sha1 in required:
        upload_url = f"https://api.netlify.com/api/v1/deploys/{deploy_id}/files/index.html"
        req_upload = urllib.request.Request(
            upload_url,
            data=content,
            headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "text/html; charset=UTF-8"},
            method="PUT"
        )
        with urllib.request.urlopen(req_upload) as resp:
            print("Uploaded index.html successfully.")

    # 3. Verify status
    status_url = f"https://api.netlify.com/api/v1/deploys/{deploy_id}"
    req_status = urllib.request.Request(status_url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req_status) as resp:
        status_info = json.loads(resp.read().decode())
        print(f"Deploy status: {status_info.get('state')} -> https://anhmoe.netlify.app")

if __name__ == "__main__":
    deploy()
