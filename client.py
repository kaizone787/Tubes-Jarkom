import socket
import threading
import os
import json
import struct
import sys
import time

#  KONFIGURASI
PORT_SINGLE  = 9090
PORT_MULTI   = 9091
BUFFER_SIZE  = 4096
ENCODING     = "utf-8"
SAVE_DIR     = "received_files"

FILE_TYPE_MAP = {
    "4":  ([".txt"],        "Dokumen .txt"),
    "5":  ([".docx"],       "Dokumen .docx"),
    "6":  ([".pdf"],        "Dokumen .pdf"),
    "7":  ([".jpg",".jpeg"],"Gambar .jpg"),
    "8":  ([".png"],        "Gambar .png"),
    "9":  ([".mp3"],        "Audio .mp3"),
    "10": ([".mp4"],        "Video .mp4"),
}

TEXT_SUBTYPE_HINTS = {
    "short":     "1-5 kata  (contoh: Halo dunia!)",
    "sentence":  "1 kalimat panjang",
    "paragraph": "1 paragraf (ketik '.' di baris baru untuk selesai)",
}

#  STATE GLOBAL
known_clients    = []          # daftar username online (dari server)
clients_lock     = threading.Lock()
print_lock       = threading.Lock()
sock_global      = None        # socket yang sedang aktif

#  UTILITAS SOCKET
def ensure_save_dir():
    os.makedirs(SAVE_DIR, exist_ok=True)

def recv_exact(conn, n):
    data = b""
    while len(data) < n:
        chunk = conn.recv(min(n - len(data), BUFFER_SIZE))
        if not chunk:
            raise ConnectionError("Koneksi terputus.")
        data += chunk
    return data

def send_framed(conn, payload_bytes):
    length = struct.pack(">I", len(payload_bytes))
    conn.sendall(length + payload_bytes)

def recv_framed(conn):
    raw_len = recv_exact(conn, 4)
    msg_len = struct.unpack(">I", raw_len)[0]
    return recv_exact(conn, msg_len)

def send_header(conn, header_dict):
    payload = json.dumps(header_dict, ensure_ascii=False).encode(ENCODING)
    send_framed(conn, payload)

def recv_header(conn):
    payload = recv_framed(conn)
    return json.loads(payload.decode(ENCODING))

def safe_print(msg):
    with print_lock:
        print(msg)

#  THREAD PENERIMA (LISTENER)
def listener_thread(conn, username):
    """
    Thread terpisah yang terus-menerus mendengarkan pesan masuk dari server.
    Berjalan selamanya selama koneksi aktif.
    """
    ensure_save_dir()
    while True:
        try:
            header = recv_header(conn)
        except (ConnectionError, struct.error, json.JSONDecodeError, OSError):
            safe_print("\n[!] Koneksi ke server terputus.")
            os._exit(1)
            break

        msg_type = header.get("type")

        if msg_type == "client_list":
            # Update daftar klien online
            with clients_lock:
                known_clients.clear()
                known_clients.extend(header.get("clients", []))
            # Tampilkan notifikasi singkat
            with clients_lock:
                cl = list(known_clients)
            safe_print(f"\n  [INFO] Klien online sekarang: {cl}")

        elif msg_type == "text":
            sender  = header.get("sender", "?")
            subtype = header.get("subtype", "")
            content = header.get("content", "")
            safe_print(f"\n{'─'*55}")
            safe_print(f"  [PESAN MASUK] Dari: {sender}  |  Tipe: teks/{subtype}")
            safe_print(f"  Isi: {content}")
            safe_print(f"{'─'*55}")

        elif msg_type == "file":
            sender   = header.get("sender", "?")
            filename = header.get("filename", "unknown")
            filesize = header.get("size", 0)
            safe_print(f"\n{'─'*55}")
            safe_print(f"  [FILE MASUK] Dari: {sender}  |  File: {filename} ({filesize} bytes)")

            # Terima payload file
            received = b""
            try:
                while len(received) < filesize:
                    to_read = min(BUFFER_SIZE, filesize - len(received))
                    chunk = conn.recv(to_read)
                    if not chunk:
                        raise ConnectionError("Koneksi terputus saat terima file.")
                    received += chunk
            except ConnectionError as e:
                safe_print(f"  [!] Error terima file: {e}")
                break

            save_path = os.path.join(SAVE_DIR, f"from_{sender}_{filename}")
            with open(save_path, "wb") as f:
                f.write(received)
            safe_print(f"  File disimpan: {save_path}")
            safe_print(f"{'─'*55}")

        elif msg_type == "send_ack":
            fwd  = header.get("forwarded_to", [])
            fail = header.get("failed", [])
            safe_print(f"\n  [ACK] Terkirim ke: {fwd}" + (f"  | Gagal: {fail}" if fail else ""))

        elif msg_type == "register_ack":
            status = header.get("status")
            msg    = header.get("message", "")
            safe_print(f"\n  [REGISTER] {status.upper()}: {msg}")

        else:
            safe_print(f"\n  [INFO] Pesan dari server: {header}")