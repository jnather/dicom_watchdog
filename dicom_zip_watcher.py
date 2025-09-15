#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dicom_zip_watcher.py
Monitora uma pasta por novos ZIPs, extrai, detecta DICOMs e envia via C-STORE.

Dependências:
  pip install pydicom pynetdicom pyyaml
Recomendado para melhor notificação de FS:
  pip install watchdog

Config:
  Forneça um YAML (exemplo no final, se desejar).

Autor: (you)
"""

import os
import sys
import time
import zipfile
import hashlib
import logging
import shutil
import sqlite3
import signal
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import yaml
import pydicom
from pydicom.dataset import Dataset
from pydicom.errors import InvalidDicomError

from pynetdicom import AE, debug_logger
from pydicom.uid import UID
from pynetdicom import StoragePresentationContexts

# -------- Optional watchdog support --------
try:
    from watchdog.observers import Observer
    from watchdog.observers.polling import PollingObserver
    from watchdog.events import FileSystemEventHandler
    WATCHDOG_AVAILABLE = True
except Exception:
    WATCHDOG_AVAILABLE = False


# =========================
# Utilities & helpers
# =========================

def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=handlers
    )


def sha256_file(path: Path, block_size: int = 65536) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


@contextmanager
def sqlite_conn(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()


def init_db(db_path: Path) -> None:
    with sqlite_conn(db_path) as conn:
        cur = conn.cursor()
        # ZIP-level ledger
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_zip (
                zip_sha256 TEXT PRIMARY KEY,
                zip_name TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Image-level ledger
        cur.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_objects (
                sop_instance_uid TEXT,
                file_sha256 TEXT,
                zip_sha256 TEXT,
                file_path TEXT,
                status TEXT, -- 'OK' or 'FAIL'
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (sop_instance_uid, file_sha256)
            )
        """)
        # Helpful indexes for fast lookups
        cur.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_uid ON uploaded_objects(sop_instance_uid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_sha ON uploaded_objects(file_sha256)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_uploaded_zip ON uploaded_objects(zip_sha256)")
        conn.commit()


def is_zip_processed(db_path: Path, zip_sha256: str) -> bool:
    with sqlite_conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM processed_zip WHERE zip_sha256=?", (zip_sha256,))
        return cur.fetchone() is not None


def mark_zip_processed(db_path: Path, zip_sha256: str, zip_name: str) -> None:
    with sqlite_conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO processed_zip (zip_sha256, zip_name) VALUES (?, ?)",
            (zip_sha256, zip_name)
        )


def is_object_uploaded(db_path: Path, sop_uid: Optional[str], file_sha256: Optional[str]) -> bool:
    query = []
    args = []
    if sop_uid:
        query.append("sop_instance_uid = ?")
        args.append(sop_uid)
    if file_sha256:
        query.append("file_sha256 = ?")
        args.append(file_sha256)
    if not query:
        return False
    where = " OR ".join(query)
    with sqlite_conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM uploaded_objects WHERE {where} LIMIT 1", tuple(args))
        return cur.fetchone() is not None


def mark_object_uploaded(db_path: Path, sop_uid: Optional[str], file_sha256: Optional[str],
                         zip_sha256: Optional[str], file_path: str, ok: bool) -> None:
    with sqlite_conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO uploaded_objects
            (sop_instance_uid, file_sha256, zip_sha256, file_path, status, uploaded_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (sop_uid, file_sha256, zip_sha256, file_path, "OK" if ok else "FAIL"))


def sha256_of_path(path: Path) -> str:
    """Helper to compute SHA256 of a file path."""
    return sha256_file(path)


def fast_dicom_magic_check(path: Path) -> bool:
    """Quick probe: check 128-byte preamble + 'DICM' magic. Not all DICOMs have it."""
    try:
        with path.open("rb") as f:
            head = f.read(132)
        return len(head) >= 132 and head[128:132] == b"DICM"
    except Exception:
        return False


def is_dicom(path: Path) -> bool:
    """
    Robust DICOM detection:
    1) Fast preamble check (if true, it's very likely a DICOM)
    2) Try pydicom.dcmread with force=True, stopping before pixel data to be fast
    """
    if fast_dicom_magic_check(path):
        return True
    try:
        _ = pydicom.dcmread(str(path), stop_before_pixels=True, force=True, specific_tags=[])
        return True
    except (InvalidDicomError, Exception):
        return False


def _win_long(p: Path) -> str:
    """Return a Windows long-path string if on Windows, else regular path."""
    s = os.path.abspath(str(p))
    if os.name == "nt":
        if not s.startswith("\\\\?\\"):
            s = "\\\\?\\" + s
    return s


def _is_encrypted(info: zipfile.ZipInfo) -> bool:
    # Bit 0 of general purpose flag indicates encryption for classic ZipCrypto/AES.
    return bool(info.flag_bits & 0x1)


def extract_zip(zip_path: Path, dest_dir: Path) -> Path:
    target_dir = dest_dir / f"{zip_path.stem}__extract"
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
    safe_mkdir(target_dir)

    files_ok = 0
    files_fail = 0
    dirs_made = 0

    base_resolved = target_dir.resolve()
    with zipfile.ZipFile(zip_path, 'r') as z:
        infos = z.infolist()
        if not infos:
            logging.warning(f"ZIP appears empty: {zip_path}")
        for info in infos:
            rel = Path(info.filename.replace("\\", "/"))

            # Directory entries
            if str(rel).endswith("/"):
                out_dir = (base_resolved / rel).resolve()
                if not str(out_dir).startswith(str(base_resolved)):
                    logging.error(f"Blocked zip path traversal (dir): {rel}")
                    files_fail += 1
                    continue
                try:
                    safe_mkdir(out_dir)
                    dirs_made += 1
                except Exception as e:
                    logging.exception(f"Failed to create dir {out_dir}: {e}")
                continue

            out_path = (base_resolved / rel).resolve()
            if not str(out_path).startswith(str(base_resolved)):
                logging.error(f"Blocked zip path traversal (file): {rel}")
                files_fail += 1
                continue

            # Ensure parents exist
            try:
                safe_mkdir(out_path.parent)
            except Exception as e:
                logging.exception(f"Failed to create parents for {out_path}: {e}")
                files_fail += 1
                continue

            # Encrypted?
            if _is_encrypted(info):
                logging.error(f"Encrypted entry detected (cannot extract without password): {info.filename}")
                files_fail += 1
                continue

            # Extract with long path support on Windows
            try:
                with z.open(info, 'r') as src, open(_win_long(out_path), 'wb') as dst:
                    shutil.copyfileobj(src, dst)
                files_ok += 1
                logging.debug(f"Extracted: {out_path}")
            except Exception as e:
                files_fail += 1
                logging.exception(f"Failed extracting {info.filename} -> {out_path}: {e}")

    logging.info(f"Extraction summary: dirs={dirs_made}, files_ok={files_ok}, files_fail={files_fail} from {zip_path.name}")
    return target_dir


def walk_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


# =========================
# DICOM Sender
# =========================

class DicomSender:
    # Transfer Syntax UIDs
    IMPL_VR_LE = UID("1.2.840.10008.1.2")
    EXPL_VR_LE = UID("1.2.840.10008.1.2.1")
    EXPL_VR_BE = UID("1.2.840.10008.1.2.2")
    RLE_LOSSLESS = UID("1.2.840.10008.1.2.5")
    JPEG_BASELINE = UID("1.2.840.10008.1.2.4.50")
    JPEG_EXTENDED = UID("1.2.840.10008.1.2.4.51")
    JPEG_LOSSLESS_14 = UID("1.2.840.10008.1.2.4.57")
    JPEG_LOSSLESS_14_S1 = UID("1.2.840.10008.1.2.4.70")
    JPEG2000_LOSSLESS = UID("1.2.840.10008.1.2.4.90")
    JPEG2000 = UID("1.2.840.10008.1.2.4.91")

    COMPRESSED_TS = {
        RLE_LOSSLESS, JPEG_BASELINE, JPEG_EXTENDED, JPEG_LOSSLESS_14,
        JPEG_LOSSLESS_14_S1, JPEG2000_LOSSLESS, JPEG2000
    }

    def __init__(self, config: dict):
        self.config = config
        self.ae: Optional[AE] = None
        self.assoc = None

        # Client (local AE) settings
        self.local_ae_title = self.config["dicom"]["local_ae_title"]
        self.local_port = int(self.config["dicom"].get("local_port", 0))  # 0 = ephemeral
        self.max_pdu = int(self.config["dicom"].get("max_pdu", 0)) or None

        # Remote (server/peer) settings
        self.peer_host = self.config["dicom"]["peer_host"]
        self.peer_port = int(self.config["dicom"]["peer_port"])
        self.peer_ae_title = self.config["dicom"].get("peer_ae_title", "ANY-SCP")

        # Behavior
        behavior = self.config.get("behavior", {})
        self.transcode_to_accepted = bool(behavior.get("transcode_to_accepted", True))
        self.prefer_uncompressed = behavior.get("prefer_uncompressed_ts", "explicit").lower()  # 'explicit'|'implicit'

        # TLS (opcional; não configurado aqui)
        self.use_tls = bool(self.config["dicom"].get("use_tls", False))

        # Debug logging para pynetdicom (opcional)
        if bool(self.config.get("logging", {}).get("enable_pynetdicom_debug", False)):
            debug_logger()

        self._contexts_logged = False  # evita log duplicado

    def _build_ae(self) -> AE:
        ae = AE(ae_title=self.local_ae_title)
        # Port/PDU
        if self.local_port:
            # Observação: 'local_socket' é experimental; se tiver problema, remova.
            ae.local_socket = (None, self.local_port)
        if self.max_pdu:
            ae.maximum_pdu = self.max_pdu

        # C-ECHO
        ae.add_requested_context(UID("1.2.840.10008.1.1"))

        # SOP classes comuns
        common_sop_classes = [
            "1.2.840.10008.5.1.4.1.1.1",     # CR
            "1.2.840.10008.5.1.4.1.1.2",     # CT
            "1.2.840.10008.5.1.4.1.1.4",     # MR
            "1.2.840.10008.5.1.4.1.1.4.1",   # Enhanced MR
            "1.2.840.10008.5.1.4.1.1.4.3",   # MR Spectroscopy
            "1.2.840.10008.5.1.4.1.1.6.1",   # US
            "1.2.840.10008.5.1.4.1.1.7",     # Secondary Capture
            "1.2.840.10008.5.1.4.1.1.12.1",  # XA
            "1.2.840.10008.5.1.4.1.1.12.2",  # RF
            "1.2.840.10008.5.1.4.1.1.128",   # PET
            "1.2.840.10008.5.1.4.1.1.20",    # NM
        ]

        # Transfer syntaxes apresentadas (propomos várias; o SCP aceitará subset)
        transfer_syntaxes = [
            self.EXPL_VR_LE,
            self.IMPL_VR_LE,
            self.EXPL_VR_BE,
            self.RLE_LOSSLESS,
            self.JPEG_BASELINE,
            self.JPEG_EXTENDED,
            self.JPEG_LOSSLESS_14,
            self.JPEG_LOSSLESS_14_S1,
            self.JPEG2000_LOSSLESS,
            self.JPEG2000,
        ]
        # Adiciona contexts
        for sop in common_sop_classes:
            try:
                ae.add_requested_context(UID(sop), transfer_syntaxes)
            except Exception as e:
                logging.warning(f"Failed to add SOP class {sop}: {e}")

        return ae

    def _log_accepted_contexts_once(self) -> None:
        if self._contexts_logged or not self.assoc:
            return
        self._contexts_logged = True
        lines = ["Accepted Presentation Contexts:"]
        for cx in self.assoc.accepted_contexts:
            try:
                ts_list = ", ".join(str(ts) for ts in cx.transfer_syntax)
            except Exception:
                ts_list = str(cx.transfer_syntax)
            lines.append(f"  - {cx.abstract_syntax.name} [{cx.abstract_syntax}]: {ts_list}")
        logging.info("\n".join(lines))

    def _accepted_ts_for_sop(self, sop_class_uid: UID) -> List[UID]:
        if not self.assoc:
            return []
        out = []
        for cx in self.assoc.accepted_contexts:
            if str(cx.abstract_syntax) == str(sop_class_uid):
                # cx.transfer_syntax pode ser único ou lista
                ts_vals = cx.transfer_syntax if isinstance(cx.transfer_syntax, (list, tuple)) else [cx.transfer_syntax]
                out.extend([UID(str(ts)) for ts in ts_vals])
        return out

    def _choose_uncompressed_ts(self, accepted_ts: List[UID]) -> Optional[UID]:
        # Preferência configurável
        if self.EXPL_VR_LE in accepted_ts and self.prefer_uncompressed == "explicit":
            return self.EXPL_VR_LE
        if self.IMPL_VR_LE in accepted_ts:
            return self.IMPL_VR_LE
        if self.EXPL_VR_LE in accepted_ts:
            return self.EXPL_VR_LE
        return None

    def _is_compressed_ts(self, ts: UID) -> bool:
        return ts in self.COMPRESSED_TS

    def _try_decompress(self, ds: Dataset) -> bool:
        try:
            # Tenta descompressão (RLE/JPEG/etc). Requer handlers instalados.
            ds.decompress()
            return True
        except Exception as e:
            logging.error(
                "Falha ao descomprimir Pixel Data. "
                "Instale handlers: 'pip install numpy pylibjpeg pylibjpeg-libjpeg pylibjpeg-openjpeg pylibjpeg-rle gdcm'. "
                f"Erro: {e}"
            )
            return False

    def _reencode_to_accepted_if_needed(self, ds: Dataset) -> Tuple[Optional[Dataset], Optional[str]]:
        """Garante que o dataset use uma transfer syntax aceita pelo peer.
        Retorna (ds_possivelmente_modificado, motivo_info_ou_None). Se não for possível, retorna (None, motivo)."""
        sop = getattr(ds, "SOPClassUID", None) or getattr(getattr(ds, "file_meta", None), "MediaStorageSOPClassUID", None)
        if not sop:
            return None, "Dataset sem SOPClassUID"

        accepted_ts = self._accepted_ts_for_sop(UID(str(sop)))
        if not accepted_ts:
            return None, f"Nenhum presentation context aceito para SOP {sop}"

        current_ts = getattr(getattr(ds, "file_meta", None), "TransferSyntaxUID", None)
        if not current_ts:
            # Assume implicit LE se ausente (caso raro)
            current_ts = self.IMPL_VR_LE

        current_ts = UID(str(current_ts))
        if current_ts in accepted_ts:
            return ds, None  # já aceito

        # Tenta escolher um TS descompactado aceito
        fallback_ts = self._choose_uncompressed_ts(accepted_ts)
        if not fallback_ts:
            return None, (
                f"O peer não aceitou nenhuma Transfer Syntax descompactada para SOP {sop}. "
                f"Aceitas: {[str(ts) for ts in accepted_ts]}; atual: {current_ts}"
            )

        # Se atual for comprimido, precisamos descomprimir
        if self._is_compressed_ts(current_ts):
            ok = self._try_decompress(ds)
            if not ok:
                return None, f"Não foi possível descomprimir {current_ts} -> {fallback_ts}"

        # Ajusta metadados de TS e flags de endianness/VR
        if not hasattr(ds, "file_meta"):
            ds.file_meta = pydicom.dataset.FileMetaDataset()
        ds.file_meta.TransferSyntaxUID = fallback_ts

        if fallback_ts == self.EXPL_VR_LE:
            ds.is_little_endian = True
            ds.is_implicit_VR = False
        elif fallback_ts == self.IMPL_VR_LE:
            ds.is_little_endian = True
            ds.is_implicit_VR = True
        elif fallback_ts == self.EXPL_VR_BE:
            ds.is_little_endian = False
            ds.is_implicit_VR = False
        else:
            # Outros TS não compactados não são esperados aqui
            pass

        return ds, f"Re-encodado de {current_ts} para {fallback_ts}"

    def c_echo(self) -> bool:
        self._ensure_association()
        if not self.assoc or not self.assoc.is_established:
            return False
        status = self.assoc.send_c_echo()
        ok = (status and status.Status == 0x0000)
        logging.info(f"C-ECHO {'success' if ok else 'failed'} (Status={getattr(status, 'Status', None)})")
        return ok

    def _ensure_association(self) -> None:
        if self.assoc and self.assoc.is_established:
            return
        if self.ae is None:
            self.ae = self._build_ae()

        logging.info(f"Associating to {self.peer_host}:{self.peer_port} as '{self.peer_ae_title}' from AE '{self.local_ae_title}'")
        self.assoc = self.ae.associate(self.peer_host, self.peer_port, ae_title=self.peer_ae_title)
        if not self.assoc.is_established:
            raise RuntimeError(f"Association to {self.peer_host}:{self.peer_port} failed")
        self._log_accepted_contexts_once()

    def _teardown(self) -> None:
        try:
            if self.assoc and self.assoc.is_established:
                self.assoc.release()
        finally:
            self.assoc = None
            if self.ae:
                try:
                    self.ae.shutdown()
                except Exception:
                    pass
            self.ae = None
        self._contexts_logged = False

    def _send_single(self, ds: Dataset, src_path: Path) -> bool:
        """Envio de um único dataset com possível re-encode para TS aceita."""
        # Garante associação
        self._ensure_association()

        # Tenta re-encode se necessário
        if self.transcode_to_accepted:
            ds2, reason = self._reencode_to_accepted_if_needed(ds)
            if ds2 is None:
                logging.error(f"Não é possível enviar {src_path.name}: {reason}")
                return False
            if reason:
                logging.info(f"{src_path.name}: {reason}")
            ds = ds2

        status = self.assoc.send_c_store(ds)
        if status and status.Status in (0x0000, 0xB000):
            logging.info(f"C-STORE OK: {src_path.name} (Status=0x{status.Status:04X})")
            return True
        else:
            st = getattr(status, "Status", None)
            logging.error(f"C-STORE FAILED: {src_path.name} (Status={st})")
            return False

    def send_files(self, files: List[Path], batch_size: int = 128) -> Tuple[int, int]:
        """Envio simples (sem dedup). Retorna (ok, fail)."""
        sent_ok = 0
        sent_failed = 0

        def send_batch(batch: List[Path]) -> Tuple[int, int]:
            ok, fail = 0, 0
            self._ensure_association()
            for f in batch:
                try:
                    ds = pydicom.dcmread(str(f), force=True)
                    # Sanidade: precisa SOPClass/Instance
                    if not hasattr(ds, "SOPClassUID") or not hasattr(ds, "SOPInstanceUID"):
                        if hasattr(ds, "file_meta"):
                            ds.SOPClassUID = getattr(ds.file_meta, "MediaStorageSOPClassUID", None)
                            ds.SOPInstanceUID = getattr(ds.file_meta, "MediaStorageSOPInstanceUID", None)
                    if not getattr(ds, "SOPClassUID", None) or not getattr(ds, "SOPInstanceUID", None):
                        logging.warning(f"Skipping (missing SOP UIDs): {f}")
                        fail += 1
                        continue

                    if self._send_single(ds, f):
                        ok += 1
                    else:
                        fail += 1
                except Exception as e:
                    fail += 1
                    logging.exception(f"Exception sending {f}: {e}")
            return ok, fail

        try:
            for i in range(0, len(files), batch_size):
                b = files[i:i + batch_size]
                ok, fail = send_batch(b)
                sent_ok += ok
                sent_failed += fail
        finally:
            self._teardown()

        return sent_ok, sent_failed

    def send_files_with_dedup(self, files: List[Path], db_path: Path, zip_sha256: str,
                              batch_size: int = 128, dedup_policy: str = "uid_then_sha256") -> Tuple[int, int]:
        """
        Envio com deduplicação por imagem. Retorna (ok, fail).
        """
        sent_ok, sent_failed = 0, 0

        def should_skip(sop_uid: Optional[str], fsha: Optional[str]) -> bool:
            if dedup_policy == "uid_only":
                return is_object_uploaded(db_path, sop_uid, None)
            if dedup_policy == "sha256_only":
                return is_object_uploaded(db_path, None, fsha)
            # default: uid_then_sha256
            if is_object_uploaded(db_path, sop_uid, None):
                return True
            return is_object_uploaded(db_path, None, fsha)

        def send_batch(batch: List[Tuple[Path, str]]):
            nonlocal sent_ok, sent_failed
            self._ensure_association()
            for fpath, fsha in batch:
                sop_uid = None
                try:
                    ds = pydicom.dcmread(str(fpath), force=True)
                    sop_uid = getattr(ds, "SOPInstanceUID", None)
                    if not sop_uid and hasattr(ds, "file_meta"):
                        sop_uid = getattr(ds.file_meta, "MediaStorageSOPInstanceUID", None)

                    if should_skip(sop_uid, fsha):
                        logging.info(f"SKIP (already uploaded): {fpath.name} (UID={sop_uid} SHA={fsha[:12]})")
                        continue

                    # Sanidade
                    if not hasattr(ds, "SOPClassUID") and hasattr(ds, "file_meta"):
                        ds.SOPClassUID = getattr(ds.file_meta, "MediaStorageSOPClassUID", None)

                    if not getattr(ds, "SOPClassUID", None) or not getattr(ds, "SOPInstanceUID", None):
                        logging.warning(f"Skipping (missing SOP UIDs): {fpath}")
                        mark_object_uploaded(db_path, sop_uid, fsha, zip_sha256, str(fpath), ok=False)
                        sent_failed += 1
                        continue

                    ok = self._send_single(ds, fpath)
                    mark_object_uploaded(db_path, sop_uid or getattr(ds, "SOPInstanceUID", None),
                                         fsha, zip_sha256, str(fpath), ok=ok)
                    if ok:
                        sent_ok += 1
                    else:
                        sent_failed += 1

                except Exception as e:
                    logging.exception(f"Exception sending {fpath}: {e}")
                    mark_object_uploaded(db_path, sop_uid, fsha, zip_sha256, str(fpath), ok=False)
                    sent_failed += 1

        # Precompute SHA256
        pairs = [(p, sha256_of_path(p)) for p in files]

        try:
            for i in range(0, len(pairs), batch_size):
                send_batch(pairs[i:i + batch_size])
        finally:
            self._teardown()

        return sent_ok, sent_failed


# =========================
# Watcher logic
# =========================

class ZipProcessor:
    def __init__(self, config: dict):
        self.config = config
        self.watch_dir = Path(config["paths"]["watch_dir"]).resolve()
        self.work_dir = Path(config["paths"]["work_dir"]).resolve()
        self.archive_dir = Path(config["paths"]["archive_dir"]).resolve()
        self.db_path = Path(config["paths"]["state_db"]).resolve()

        safe_mkdir(self.watch_dir)
        safe_mkdir(self.work_dir)
        safe_mkdir(self.archive_dir)
        safe_mkdir(self.db_path.parent)
        init_db(self.db_path)

        self.sender = DicomSender(config)
        self.post_extract_delete = bool(config["behavior"].get("delete_extracted_after_upload", True))
        self.move_zip_to_archive = bool(config["behavior"].get("move_zip_to_archive", True))
        self.poll_interval = int(config["behavior"].get("poll_interval_seconds", 10))
        self.send_batch_size = int(config["behavior"].get("send_batch_size", 128))
        self.wait_zip_complete_seconds = int(config["behavior"].get("wait_zip_complete_seconds", 2))

    def process_zip(self, zip_path: Path) -> None:
        logging.info(f"Processing ZIP: {zip_path}")

        # Aguarda estabilizar tamanho (arquivo totalmente gravado)
        prev_size = -1
        for _ in range(self.wait_zip_complete_seconds):
            size = zip_path.stat().st_size
            if size == prev_size:
                break
            prev_size = size
            time.sleep(1)

        # Dedup por SHA256 do ZIP
        zhash = sha256_file(zip_path)
        if is_zip_processed(self.db_path, zhash):
            logging.info(f"Zip already processed (hash ledger): {zip_path.name}")
            if self.move_zip_to_archive:
                self._archive_zip(zip_path)
            return

        # Extrai
        try:
            extract_root = extract_zip(zip_path, self.work_dir)
        except zipfile.BadZipFile:
            logging.error(f"Bad ZIP file, moving to archive (bad): {zip_path}")
            bad_dir = self.archive_dir / "bad"
            safe_mkdir(bad_dir)
            try:
                shutil.move(str(zip_path), str(bad_dir / zip_path.name))
            except Exception:
                pass
            return
        except Exception as e:
            logging.exception(f"Extraction error, moving to archive (failed): {zip_path}")
            failed_dir = self.archive_dir / "failed"
            safe_mkdir(failed_dir)
            try:
                shutil.move(str(zip_path), str(failed_dir / zip_path.name))
            except Exception:
                pass
            return

        # Detecta DICOMs
        dicom_files: List[Path] = []
        total_files = 0
        for f in walk_files(extract_root):
            total_files += 1
            try:
                if is_dicom(f):
                    dicom_files.append(f)
            except Exception:
                pass

        logging.info(f"Extracted {total_files} files; detected {len(dicom_files)} DICOM files")

        if dicom_files:
            # C-ECHO opcional
            try:
                self.sender.c_echo()
            except Exception as e:
                logging.warning(f"C-ECHO failed (will attempt C-STORE anyway): {e}")

            # Envia com dedup
            dedup_policy = self.config["behavior"].get("dedup_policy", "uid_then_sha256")
            ok, fail = self.sender.send_files_with_dedup(
                dicom_files, self.db_path, zhash, self.send_batch_size, dedup_policy
            )
            logging.info(f"C-STORE summary: OK={ok}, FAILED={fail}")
        else:
            logging.info("No DICOM files found in this ZIP")

        # Housekeeping
        mark_zip_processed(self.db_path, zhash, zip_path.name)

        if self.post_extract_delete:
            try:
                shutil.rmtree(extract_root, ignore_errors=True)
            except Exception as e:
                logging.warning(f"Failed to remove temp extract dir {extract_root}: {e}")

        if self.move_zip_to_archive:
            self._archive_zip(zip_path)

    def _archive_zip(self, zip_path: Path) -> None:
        dest = self.archive_dir / zip_path.name
        try:
            if dest.exists():
                dest = self.archive_dir / f"{zip_path.stem}__{int(time.time())}.zip"
            shutil.move(str(zip_path), str(dest))
            logging.info(f"Moved ZIP to archive: {dest}")
        except Exception as e:
            logging.error(f"Failed to move ZIP to archive: {e}")

    # --------- Two watch modes: watchdog or polling ---------

    def _run_polling(self) -> None:
        logging.info(f"Polling {self.watch_dir} every {self.poll_interval}s for new ZIPs")
        while True:
            try:
                for p in self.watch_dir.glob("*.zip"):
                    self.process_zip(p)
                time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logging.info("Interrupted by user. Exiting.")
                break
            except Exception:
                logging.exception("Unexpected error in polling loop")

    def _run_watchdog(self) -> None:
        class ZipHandler(FileSystemEventHandler):
            def __init__(self, outer: "ZipProcessor"):
                super().__init__()
                self.outer = outer

            def on_created(self, event):
                if not event.is_directory and event.src_path.lower().endswith(".zip"):
                    self.outer.process_zip(Path(event.src_path))

            def on_moved(self, event):
                if (not event.is_directory) and event.dest_path.lower().endswith(".zip"):
                    self.outer.process_zip(Path(event.dest_path))

            def on_modified(self, event):
                if (not event.is_directory) and event.src_path.lower().endswith(".zip"):
                    self.outer.process_zip(Path(event.src_path))

        # Processa ZIPs já existentes
        logging.info(f"Processing existing ZIP files in {self.watch_dir}")
        existing_zips = list(self.watch_dir.glob("*.zip"))
        if existing_zips:
            logging.info(f"Found {len(existing_zips)} existing ZIP files to process")
            for zip_path in existing_zips:
                self.process_zip(zip_path)
        else:
            logging.info("No existing ZIP files found")

        event_handler = ZipHandler(self)

        force_poll = bool(self.config["behavior"].get("watchdog_force_polling", False))
        observer = (PollingObserver if force_poll else Observer)()
        observer.schedule(event_handler, str(self.watch_dir), recursive=False)
        observer.start()

        mode = "polling watchdog" if force_poll else "watchdog"
        logging.info(f"Watching ({mode}) {self.watch_dir} for new ZIPs")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Interrupted by user. Exiting.")
        finally:
            observer.stop()
            observer.join()

    def run(self) -> None:
        if WATCHDOG_AVAILABLE:
            self._run_watchdog()
        else:
            self._run_polling()


# =========================
# Main
# =========================

def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Basic validation
    required = [
        ("paths", "watch_dir"),
        ("paths", "work_dir"),
        ("paths", "archive_dir"),
        ("paths", "state_db"),
        ("dicom", "local_ae_title"),
        ("dicom", "peer_host"),
        ("dicom", "peer_port"),
        ("logging", "level"),
    ]
    for section, key in required:
        if section not in cfg or key not in cfg[section]:
            raise ValueError(f"Missing config: {section}.{key}")
    return cfg


def main():
    if len(sys.argv) < 2:
        print("Usage: python dicom_zip_watcher.py /path/to/config.yaml")
        sys.exit(2)

    config_path = Path(sys.argv[1]).resolve()
    cfg = load_config(config_path)

    setup_logging(level=cfg["logging"]["level"], log_file=cfg["logging"].get("file"))

    logging.info("Starting DICOM ZIP watcher")
    logging.info(f"Config file: {config_path}")

    processor = ZipProcessor(cfg)

    # Graceful exit on SIGTERM (systemd, docker)
    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM, exiting gracefully.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    processor.run()


if __name__ == "__main__":
    main()
