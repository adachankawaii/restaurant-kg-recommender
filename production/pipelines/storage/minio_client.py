from __future__ import annotations

import subprocess
from urllib.parse import urlparse
from pathlib import Path


class MinioStorageAdapter:
    def __init__(
        self,
        endpoint: str = "http://minio:9000",
        bucket: str = "restaurant-prod",
        access_key: str = "minio",
        secret_key: str = "minio123",
        network: str = "production_default",
    ):
        self.endpoint = endpoint
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.network = network

    def _python_client(self):
        from minio import Minio

        parsed = urlparse(self.endpoint)
        if parsed.scheme:
            endpoint = parsed.netloc
            secure = parsed.scheme == "https"
        else:
            endpoint = self.endpoint
            secure = False
        return Minio(endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=secure)

    def put_file(self, source: Path, key: str) -> dict[str, str]:
        source = Path(source).resolve()
        if not source.exists():
            return {"status": "missing", "source": str(source), "key": key}
        try:
            client = self._python_client()
            if not client.bucket_exists(self.bucket):
                client.make_bucket(self.bucket)
            client.fput_object(self.bucket, key, str(source))
            return {"status": "ok", "source": str(source), "key": key, "stdout": "", "stderr": ""}
        except Exception:
            pass

        source_dir = source.parent
        command = [
            "docker",
            "run",
            "--rm",
            "--network",
            self.network,
            "--entrypoint",
            "/bin/sh",
            "-v",
            f"{source_dir}:/data:ro",
            "minio/mc",
            "-c",
            (
                f"mc alias set local {self.endpoint} {self.access_key} {self.secret_key} && "
                f"mc mb -p local/{self.bucket} >/dev/null 2>&1 || true && "
                f"mc cp /data/{source.name} local/{self.bucket}/{key}"
            ),
        ]
        result = subprocess.run(
            command,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=120,
        )
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        return {
            "status": "ok" if result.returncode == 0 else "failed",
            "source": str(source),
            "key": key,
            "stdout": stdout[-500:],
            "stderr": stderr[-500:],
        }

    def download_prefix(self, prefix: str, destination: Path) -> dict[str, object]:
        destination = Path(destination).resolve()
        destination.mkdir(parents=True, exist_ok=True)
        try:
            client = self._python_client()
            objects = list(client.list_objects(self.bucket, prefix=prefix, recursive=True))
            downloaded = []
            for obj in objects:
                if obj.is_dir:
                    continue
                relative = obj.object_name[len(prefix) :].lstrip("/")
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                client.fget_object(self.bucket, obj.object_name, str(target))
                downloaded.append(str(target))
            return {"status": "ok", "prefix": prefix, "destination": str(destination), "downloaded": downloaded}
        except Exception as exc:
            python_error = str(exc)

        command = [
            "docker",
            "run",
            "--rm",
            "--network",
            self.network,
            "--entrypoint",
            "/bin/sh",
            "-v",
            f"{destination}:/data",
            "minio/mc",
            "-c",
            (
                f"mc alias set local {self.endpoint} {self.access_key} {self.secret_key} && "
                f"mc cp --recursive local/{self.bucket}/{prefix.rstrip('/')}/ /data/"
            ),
        ]
        result = subprocess.run(
            command,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=180,
        )
        downloaded = [str(path) for path in destination.rglob("*") if path.is_file()]
        return {
            "status": "ok" if result.returncode == 0 else "failed",
            "prefix": prefix,
            "destination": str(destination),
            "downloaded": downloaded,
            "stdout": (result.stdout or "")[-500:],
            "stderr": (result.stderr or "")[-500:],
            "python_error": python_error,
        }
