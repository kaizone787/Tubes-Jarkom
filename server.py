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

#  HANDLER PESAN MASUK
def read_file_payload(conn, filesize):
    """Baca filesize byte dari conn dan kembalikan sebagai bytes."""
    received = b""
    while len(received) < filesize:
        to_read = min(BUFFER_SIZE, filesize - len(received))
        chunk = conn.recv(to_read)
        if not chunk:
            raise ConnectionError("Koneksi terputus saat membaca payload file.")
        received += chunk
    return received

def handle_client(conn, addr, mode_label):
    """Loop utama untuk satu klien."""
    print(f"\n[+] Koneksi baru dari {addr} ({mode_label})")
    username = None

    try:
        # ── Handshake: terima username ──
        try:
            header = recv_header(conn)
        except Exception as e:
            print(f"[!] Gagal baca header registrasi dari {addr}: {e}")
            conn.close()
            return

        if header.get("type") != "register":
            send_header(conn, {"type": "register_ack", "status": "error",
                                "message": "Harap kirim paket register terlebih dahulu."})
            conn.close()
            return

        requested_name = header.get("username", "").strip()
        if not requested_name:
            send_header(conn, {"type": "register_ack", "status": "error",
                                "message": "Username tidak boleh kosong."})
            conn.close()
            return

        with clients_lock:
            if requested_name in clients:
                send_header(conn, {"type": "register_ack", "status": "error",
                                    "message": f"Username '{requested_name}' sudah digunakan."})
                conn.close()
                return
            clients[requested_name] = conn
            username = requested_name

        send_header(conn, {"type": "register_ack", "status": "ok",
                            "message": f"Selamat datang, {username}!"})
        print(f"[+] '{username}' terdaftar dari {addr}. Total klien: {len(clients)}")
        _broadcast_client_list()

        # ── Loop terima pesan ──
        while True:
            try:
                header = recv_header(conn)
            except (ConnectionError, struct.error, json.JSONDecodeError, OSError):
                break

            msg_type = header.get("type")
            mode     = header.get("mode", "unicast")   # unicast | multicast | broadcast
            targets  = _resolve_targets(header, username, mode)

            # ── Ambil payload file jika ada ──
            raw_data = b""
            if msg_type == "file":
                filesize = header.get("size", 0)
                try:
                    raw_data = read_file_payload(conn, filesize)
                except ConnectionError as e:
                    print(f"[!] Error baca file dari '{username}': {e}")
                    break
                # Simpan salinan di server
                _save_file_on_server(header, raw_data, username)

            elif msg_type == "text":
                pass  # konten ada di header["content"]

            elif msg_type == "get_clients":
                _send_client_list(conn)
                continue

            else:
                print(f"[!] Tipe pesan tidak dikenal dari '{username}': {msg_type}")
                send_ack(conn, "ERR_UNKNOWN_TYPE")
                continue

            # ── Log di server ──
            _log_incoming(header, username, mode, targets)

            # ── Forward ke tujuan ──
            failed = forward_message(header, raw_data, targets, username)

            # ── ACK ke pengirim ──
            ack_payload = {"type": "send_ack", "status": "ok",
                           "forwarded_to": [t for t in targets if t not in failed],
                           "failed": failed}
            send_header(conn, ack_payload)

    except Exception as e:
        print(f"[!] Error tak terduga untuk '{username or addr}': {e}")
    finally:
        if username:
            with clients_lock:
                clients.pop(username, None)
            print(f"[-] '{username}' ({addr}) terputus. Sisa klien: {len(clients)}")
            _broadcast_client_list()
        conn.close()

def _resolve_targets(header, sender_username, mode):
    """Hitung daftar username tujuan berdasarkan mode."""
    if mode == "broadcast":
        with clients_lock:
            return [u for u in clients if u != sender_username]
    elif mode == "multicast":
        raw = header.get("targets", [])
        if isinstance(raw, str):
            raw = [r.strip() for r in raw.split(",") if r.strip()]
        return [t for t in raw if t != sender_username]
    else:  # unicast
        target = header.get("target", "")
        return [target] if target else []

def _save_file_on_server(header, raw_data, sender_username):
    ensure_save_dir()
    filename = header.get("filename", "unknown_file")
    save_path = os.path.join(SAVE_DIR, f"{sender_username}_{filename}")
    with open(save_path, "wb") as f:
        f.write(raw_data)
    print(f"  [SERVER] File dari '{sender_username}' disimpan: {save_path}")

def _log_incoming(header, username, mode, targets):
    msg_type = header.get("type")
    print(f"\n{'='*55}")
    print(f"  [IN] Dari='{username}' | Mode={mode.upper()} | Type={msg_type}")
    if msg_type == "text":
        snippet = header.get("content", "")[:60]
        print(f"  Subtype : {header.get('subtype')} | Isi: {snippet}")
    elif msg_type == "file":
        print(f"  File    : {header.get('filename')} ({header.get('size')} bytes)")
    print(f"  Targets : {targets}")
    print(f"{'='*55}")

def _send_client_list(conn):
    with clients_lock:
        user_list = list(clients.keys())
    send_header(conn, {"type": "client_list", "clients": user_list})

def _broadcast_client_list():
    """Kirim daftar klien terbaru ke semua klien yang terhubung."""
    with clients_lock:
        user_list = list(clients.keys())
        snapshot  = dict(clients)
    payload = {"type": "client_list", "clients": user_list}
    for uname, conn in snapshot.items():
        try:
            send_header(conn, payload)
        except Exception:
            pass

#  SERVER SINGLE-THREAD

class UnicastSingleServer:
    def __init__(self, host=HOST, port=PORT_SINGLE):
        self.host = host
        self.port = port

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(5)
            srv.settimeout(1.0)
            print(f"[SERVER - Unicast Single-thread] Berjalan di {self.host}:{self.port}")
            print("Menunggu koneksi... (tekan Ctrl+C untuk berhenti)\n")
            try:
                while True:
                    try:
                        conn, addr = srv.accept()
                    except socket.timeout:
                        continue
                    conn.settimeout(None)
                    handle_client(conn, addr, "Single-thread")
            except KeyboardInterrupt:
                print("\n[!] Server dihentikan oleh pengguna.")

#  SERVER MULTI-THREAD (RELAY)

class UnicastMultiServer:
    def __init__(self, host=HOST, port=PORT_MULTI):
        self.host = host
        self.port = port

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(20)
            srv.settimeout(1.0)
            print(f"[SERVER - Unicast Multi-thread Relay] Berjalan di {self.host}:{self.port}")
            print("Menunggu koneksi... (tekan Ctrl+C untuk berhenti)\n")
            try:
                while True:
                    try:
                        conn, addr = srv.accept()
                    except socket.timeout:
                        continue
                    conn.settimeout(None)
                    t = threading.Thread(
                        target=handle_client,
                        args=(conn, addr, "Multi-thread"),
                        daemon=True,
                        name=f"Thread-{addr}"
                    )
                    t.start()
                    active = threading.active_count() - 1
                    print(f"[*] Thread baru untuk {addr} | Thread aktif: {active}")
            except KeyboardInterrupt:
                print("\n[!] Server dihentikan oleh pengguna.")

#  ENTRY POINT

def print_usage():
    print("Penggunaan: python server.py <mode>")
    print("  mode:")
    print("    single  -> Unicast Single-thread (port 9090)")
    print("    multi   -> Unicast Multi-thread Relay (port 9091)")

def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    mode = sys.argv[1].strip().lower()
    if mode == "single":
        UnicastSingleServer().start()
    elif mode == "multi":
        UnicastMultiServer().start()
    else:
        print(f"[!] Mode tidak dikenal: '{mode}'")
        print_usage()
        sys.exit(1)

if __name__ == "__main__":
    main()