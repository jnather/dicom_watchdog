#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
db_manager.py
Database management utility for the DICOM ZIP watcher.
Provides commands to query, clean up, and manage the uploaded objects database.

Usage:
    python db_manager.py <command> [args]

Commands:
    stats                    - Show database statistics
    list [limit]            - List recent uploads (default: 20)
    search <uid_or_sha>     - Search for specific UID or SHA
    cleanup [days]          - Remove failed uploads older than N days (default: 7)
    export <filename>       - Export upload log to CSV
    reset                   - WARNING: Reset all data (ZIP and object tracking)
"""

import sys
import sqlite3
import csv
from pathlib import Path
from typing import Optional, List, Tuple
import argparse


def get_db_connection(db_path: Path):
    """Get database connection."""
    return sqlite3.connect(str(db_path))


def show_stats(db_path: Path):
    """Show database statistics."""
    with get_db_connection(db_path) as conn:
        cur = conn.cursor()
        
        # ZIP stats
        cur.execute("SELECT COUNT(*) FROM processed_zip")
        zip_count = cur.fetchone()[0]
        
        # Object stats
        cur.execute("SELECT COUNT(*) FROM uploaded_objects WHERE status='OK'")
        uploaded_ok = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM uploaded_objects WHERE status='FAIL'")
        uploaded_fail = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT sop_instance_uid) FROM uploaded_objects WHERE status='OK'")
        unique_uids = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT file_sha256) FROM uploaded_objects WHERE status='OK'")
        unique_shas = cur.fetchone()[0]
        
        # Recent activity
        cur.execute("""
            SELECT COUNT(*) FROM uploaded_objects 
            WHERE uploaded_at > datetime('now', '-24 hours')
        """)
        recent_24h = cur.fetchone()[0]
        
        print("=== Database Statistics ===")
        print(f"Processed ZIP files: {zip_count}")
        print(f"Uploaded objects (OK): {uploaded_ok}")
        print(f"Failed uploads: {uploaded_fail}")
        print(f"Unique SOP Instance UIDs: {unique_uids}")
        print(f"Unique file SHA256s: {unique_shas}")
        print(f"Uploads in last 24h: {recent_24h}")


def list_uploads(db_path: Path, limit: int = 20):
    """List recent uploads."""
    with get_db_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT uploaded_at, status, sop_instance_uid, 
                   substr(file_sha256,1,12) AS sha12, file_path
            FROM uploaded_objects
            ORDER BY uploaded_at DESC
            LIMIT ?
        """, (limit,))
        
        rows = cur.fetchall()
        if not rows:
            print("No uploads found.")
            return
            
        print(f"=== Last {len(rows)} Uploads ===")
        print(f"{'Time':<20} {'Status':<6} {'UID':<40} {'SHA12':<12} {'File'}")
        print("-" * 100)
        
        for row in rows:
            time_str, status, uid, sha12, file_path = row
            uid_display = uid[:37] + "..." if uid and len(uid) > 40 else uid or "N/A"
            file_name = Path(file_path).name if file_path else "N/A"
            print(f"{time_str:<20} {status:<6} {uid_display:<40} {sha12:<12} {file_name}")


def search_uploads(db_path: Path, search_term: str):
    """Search for specific UID or SHA."""
    with get_db_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT uploaded_at, status, sop_instance_uid, 
                   substr(file_sha256,1,12) AS sha12, file_path, zip_sha256
            FROM uploaded_objects
            WHERE sop_instance_uid LIKE ? OR file_sha256 LIKE ?
            ORDER BY uploaded_at DESC
        """, (f"%{search_term}%", f"%{search_term}%"))
        
        rows = cur.fetchall()
        if not rows:
            print(f"No uploads found matching '{search_term}'")
            return
            
        print(f"=== Search Results for '{search_term}' ({len(rows)} found) ===")
        print(f"{'Time':<20} {'Status':<6} {'UID':<40} {'SHA12':<12} {'File'}")
        print("-" * 100)
        
        for row in rows:
            time_str, status, uid, sha12, file_path, zip_sha = row
            uid_display = uid[:37] + "..." if uid and len(uid) > 40 else uid or "N/A"
            file_name = Path(file_path).name if file_path else "N/A"
            print(f"{time_str:<20} {status:<6} {uid_display:<40} {sha12:<12} {file_name}")


def cleanup_failed(db_path: Path, days: int = 7):
    """Remove failed uploads older than specified days."""
    with get_db_connection(db_path) as conn:
        cur = conn.cursor()
        
        # Count records to be deleted
        cur.execute("""
            SELECT COUNT(*) FROM uploaded_objects
            WHERE status='FAIL' AND uploaded_at < datetime('now', '-{} days')
        """.format(days))
        count = cur.fetchone()[0]
        
        if count == 0:
            print(f"No failed uploads older than {days} days found.")
            return
            
        print(f"Found {count} failed uploads older than {days} days.")
        confirm = input("Delete them? (y/N): ")
        
        if confirm.lower() == 'y':
            cur.execute("""
                DELETE FROM uploaded_objects
                WHERE status='FAIL' AND uploaded_at < datetime('now', '-{} days')
            """.format(days))
            conn.commit()
            print(f"Deleted {count} failed upload records.")
        else:
            print("Cleanup cancelled.")


def export_csv(db_path: Path, filename: str):
    """Export upload log to CSV."""
    with get_db_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT uploaded_at, status, sop_instance_uid, file_sha256, 
                   zip_sha256, file_path
            FROM uploaded_objects
            ORDER BY uploaded_at DESC
        """)
        
        rows = cur.fetchall()
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['uploaded_at', 'status', 'sop_instance_uid', 
                           'file_sha256', 'zip_sha256', 'file_path'])
            writer.writerows(rows)
        
        print(f"Exported {len(rows)} records to {filename}")


def reset_database(db_path: Path):
    """WARNING: Reset all data."""
    print("WARNING: This will delete ALL data from the database!")
    print("This includes:")
    print("- All processed ZIP records")
    print("- All uploaded object records")
    print("- All tracking information")
    print()
    
    confirm1 = input("Type 'RESET' to confirm: ")
    if confirm1 != 'RESET':
        print("Reset cancelled.")
        return
        
    confirm2 = input("Are you absolutely sure? Type 'YES' to proceed: ")
    if confirm2 != 'YES':
        print("Reset cancelled.")
        return
    
    with get_db_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM uploaded_objects")
        cur.execute("DELETE FROM processed_zip")
        conn.commit()
    
    print("Database reset complete.")


def main():
    parser = argparse.ArgumentParser(description='DICOM ZIP Watcher Database Manager')
    parser.add_argument('command', choices=['stats', 'list', 'search', 'cleanup', 'export', 'reset'],
                       help='Command to execute')
    parser.add_argument('args', nargs='*', help='Command arguments')
    
    # Default database path (same as in config.yaml)
    default_db = Path("E:/programacao_novo/python/Laudos_Matao/watchdog_dicom_upload/state/processed.sqlite3")
    
    if len(sys.argv) < 2:
        parser.print_help()
        return
    
    args = parser.parse_args()
    command = args.command
    cmd_args = args.args
    
    if not default_db.exists():
        print(f"Database not found at {default_db}")
        print("Make sure the DICOM ZIP watcher has been run at least once.")
        return
    
    try:
        if command == 'stats':
            show_stats(default_db)
            
        elif command == 'list':
            limit = int(cmd_args[0]) if cmd_args else 20
            list_uploads(default_db, limit)
            
        elif command == 'search':
            if not cmd_args:
                print("Usage: python db_manager.py search <uid_or_sha>")
                return
            search_uploads(default_db, cmd_args[0])
            
        elif command == 'cleanup':
            days = int(cmd_args[0]) if cmd_args else 7
            cleanup_failed(default_db, days)
            
        elif command == 'export':
            filename = cmd_args[0] if cmd_args else 'upload_log.csv'
            export_csv(default_db, filename)
            
        elif command == 'reset':
            reset_database(default_db)
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
