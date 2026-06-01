import socket
import threading
import os
import json
import struct
import sys
import time

#  KONFIGURASI
HOST          = "0.0.0.0"
PORT_SINGLE   = 9090
PORT_MULTI    = 9091
BUFFER_SIZE   = 4096
ENCODING      = "utf-8"
SAVE_DIR      = "received_files"

#  STATE GLOBAL
clients_lock  = threading.Lock()
clients       = {}   # { username: conn }

#  UTILITAS SOCKET
def ensure_save_dir():
    os.makedirs(SAVE_DIR, exist_ok=True)

def recv_exact(conn, n):
    """Menerima tepat n byte dari socket."""
    data = b""
    while len(data) < n:
        chunk = conn.recv(min(n - len(data), BUFFER_SIZE))
        if not chunk:
            raise ConnectionError("Koneksi terputus sebelum data selesai diterima.")
        data += chunk
    return data

def send_framed(conn, payload_bytes):
    """Kirim payload dengan prefiks panjang 4-byte big-endian."""
    length = struct.pack(">I", len(payload_bytes))
    conn.sendall(length + payload_bytes)

def recv_framed(conn):
    """Terima payload dengan prefiks panjang 4-byte big-endian."""
    raw_len = recv_exact(conn, 4)
    msg_len = struct.unpack(">I", raw_len)[0]
    return recv_exact(conn, msg_len)

def send_header(conn, header_dict):
    """Kirim header JSON sebagai pesan berframe."""
    payload = json.dumps(header_dict, ensure_ascii=False).encode(ENCODING)
    send_framed(conn, payload)

def recv_header(conn):
    """Terima header JSON sebagai pesan berframe."""
    payload = recv_framed(conn)
    return json.loads(payload.decode(ENCODING))

def send_ack(conn, message="ACK"):
    conn.sendall(message.encode(ENCODING))

def recv_ack(conn):
    return conn.recv(16).decode(ENCODING, errors="replace")

#  FORWARD KE KLIEN TUJUAN
def forward_message(header, raw_data, targets, sender_username):
    """
    Forward header + raw_data ke setiap username dalam targets.
    raw_data adalah bytes mentah isi file/teks (bisa b"" untuk teks).
    Menambahkan field 'sender' ke header sebelum dikirim ke tujuan.
    """
    header_out = dict(header)
    header_out["sender"] = sender_username
    # Hapus field targets agar tidak membingungkan penerima
    header_out.pop("targets", None)
    header_out.pop("mode", None)

    failed = []
    with clients_lock:
        target_conns = {}
        for t in targets:
            if t in clients:
                target_conns[t] = clients[t]
            else:
                failed.append(t)

    for uname, conn in target_conns.items():
        try:
            send_header(conn, header_out)
            if raw_data:
                conn.sendall(raw_data)
        except Exception as e:
            print(f"[!] Gagal forward ke '{uname}': {e}")
            failed.append(uname)

    return failed
