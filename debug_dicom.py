#!/usr/bin/env python3
import pydicom
from pathlib import Path
from pydicom.errors import InvalidDicomError

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
        # If it parsed a dataset and has some standard attributes or file_meta, we consider it DICOM
        return True
    except (InvalidDicomError, Exception):
        return False

def walk_files(root: Path):
    for p in root.rglob("*"):
        if p.is_file():
            yield p

# Test the extracted files
extract_dir = Path("work/1.2.826.0.1.3680043.6.15372.14625.20250909075007173.734__extract")
print(f"Extract directory exists: {extract_dir.exists()}")
print(f"Extract directory: {extract_dir}")

if extract_dir.exists():
    total_files = 0
    dicom_files = 0
    
    for f in walk_files(extract_dir):
        total_files += 1
        if is_dicom(f):
            dicom_files += 1
            print(f"DICOM: {f.name}")
        else:
            print(f"Not DICOM: {f.name}")
            
    print(f"\nTotal files: {total_files}")
    print(f"DICOM files: {dicom_files}")
else:
    print("Extract directory not found!")

