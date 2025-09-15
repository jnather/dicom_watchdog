#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para remover registros de processamento de ZIPs do banco de dados
Permite reprocessar arquivos que já foram processados anteriormente
"""

import sqlite3
import hashlib
from pathlib import Path
import sys

def sha256_file(path: Path, block_size: int = 65536) -> str:
    """Calcula SHA256 de um arquivo"""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def reset_zip_processing(db_path: Path, zip_path: Path):
    """Remove registros de processamento de um ZIP específico"""
    zip_hash = sha256_file(zip_path)
    
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.cursor()
        
        # Verificar se o ZIP existe no banco
        cur.execute("SELECT zip_name FROM processed_zip WHERE zip_sha256=?", (zip_hash,))
        result = cur.fetchone()
        
        if not result:
            print(f"ZIP não encontrado no banco de dados: {zip_path.name}")
            return False
        
        print(f"Encontrado no banco: {result[0]}")
        
        # Remover da tabela processed_zip
        cur.execute("DELETE FROM processed_zip WHERE zip_sha256=?", (zip_hash,))
        zip_deleted = cur.rowcount
        
        # Remover objetos relacionados da tabela uploaded_objects
        cur.execute("DELETE FROM uploaded_objects WHERE zip_sha256=?", (zip_hash,))
        objects_deleted = cur.rowcount
        
        conn.commit()
        
        print(f"Removido do banco:")
        print(f"  - Registro do ZIP: {zip_deleted}")
        print(f"  - Objetos DICOM relacionados: {objects_deleted}")
        
        return True

def list_processed_zips(db_path: Path):
    """Lista todos os ZIPs processados"""
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.cursor()
        cur.execute("SELECT zip_name, processed_at FROM processed_zip ORDER BY processed_at DESC")
        results = cur.fetchall()
        
        if not results:
            print("Nenhum ZIP processado encontrado no banco de dados")
            return
        
        print("ZIPs processados:")
        for zip_name, processed_at in results:
            print(f"  - {zip_name} (processado em: {processed_at})")

def main():
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python reset_zip_processing.py <db_path> list                    # Listar ZIPs processados")
        print("  python reset_zip_processing.py <db_path> reset <zip_path>       # Resetar ZIP específico")
        sys.exit(1)
    
    db_path = Path(sys.argv[1])
    
    if not db_path.exists():
        print(f"Banco de dados não encontrado: {db_path}")
        sys.exit(1)
    
    if sys.argv[2] == "list":
        list_processed_zips(db_path)
    elif sys.argv[2] == "reset":
        if len(sys.argv) < 4:
            print("Erro: Forneça o caminho do arquivo ZIP para resetar")
            sys.exit(1)
        
        zip_path = Path(sys.argv[3])
        if not zip_path.exists():
            print(f"Arquivo ZIP não encontrado: {zip_path}")
            sys.exit(1)
        
        reset_zip_processing(db_path, zip_path)
    else:
        print("Comando inválido. Use 'list' ou 'reset'")
        sys.exit(1)

if __name__ == "__main__":
    main()
