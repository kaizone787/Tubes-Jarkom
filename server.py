import socket
import threading
import os
import json
import struct
import sys
import time
import selectors

#  KONFIGURASI
HOST        = "0.0.0.0"
PORT_SINGLE = 9090
PORT_MULTI  = 9091
BUFFER_SIZE = 4096
ENCODING    = "utf-8"
SAVE_DIR    = "received_files"

#  STATE GLOBAL
clients_lock = threading.Lock()
clients      = {}   # { username: conn }

#  UTILITAS SOCKET
def ensure_save_dir():
    os.makedirs(SAVE_DIR, exist_ok=True)

def recv_exact(conn, n):
    """Menerima tepat n byte dari socket (blocking)."""
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
    """Terima payload dengan prefiks panjang 4-byte big-endian (blocking)."""
    raw_len = recv_exact(conn, 4)
    msg_len = struct.unpack(">I", raw_len)[0]
    return recv_exact(conn, msg_len)

def send_header(conn, header_dict):
    """Kirim header JSON sebagai pesan berframe."""
    payload = json.dumps(header_dict, ensure_ascii=False).encode(ENCODING)
    send_framed(conn, payload)

def recv_header(conn):
    """Terima header JSON sebagai pesan berframe (blocking)."""
    payload = recv_framed(conn)
    return json.loads(payload.decode(ENCODING))

#  UTILITAS BERSAMA
def resolve_targets(header, sender_username, mode):
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

def save_file_on_server(header, raw_data, sender_username):
    """Simpan salinan file yang diterima di server."""
    ensure_save_dir()
    filename  = header.get("filename", "unknown_file")
    save_path = os.path.join(SAVE_DIR, f"{sender_username}_{filename}")
    with open(save_path, "wb") as f:
        f.write(raw_data)
    print(f"  [SERVER] File dari '{sender_username}' disimpan: {save_path}")

def log_incoming(header, username, mode, targets):
    """Cetak log pesan masuk ke konsol server."""
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

def send_client_list(conn):
    """Kirim daftar klien online ke satu koneksi."""
    with clients_lock:
        user_list = list(clients.keys())
    send_header(conn, {"type": "client_list", "clients": user_list})

def broadcast_client_list():
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

def forward_message(header, raw_data, targets, sender_username):
    """
    Forward header + raw_data ke setiap username dalam targets.
    Menambahkan field 'sender' ke header sebelum dikirim ke tujuan.
    """
    header_out = dict(header)
    header_out["sender"] = sender_username
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


#  SERVER SINGLE-THREAD (I/O Multiplexing dengan selectors)

class ClientSession:
    """
    State-machine per klien untuk parsing pesan secara inkremental.
    Digunakan oleh server single-thread agar dapat menerima data dari
    banyak klien tanpa memblokir satu sama lain.

    Alur state:
      WAIT_LEN    --(4 byte diterima)-->  WAIT_HEADER
      WAIT_HEADER --(N byte diterima)-->  [jika file: WAIT_FILE, lainnya: selesai]
      WAIT_FILE   --(M byte diterima)-->  selesai  -->  kembali ke WAIT_LEN
    """
    ST_WAIT_LEN    = 0   # Menunggu 4 byte prefiks panjang
    ST_WAIT_HEADER = 1   # Menunggu N byte body header JSON
    ST_WAIT_FILE   = 2   # Menunggu M byte payload file

    def __init__(self, conn, addr):
        self.conn       = conn
        self.addr       = addr
        self.username   = None
        self.registered = False
        self.buffer     = b""
        self.state      = self.ST_WAIT_LEN
        self.needed     = 4
        self.header     = None
        self.file_data  = b""

    def feed(self, data):
        """
        Masukkan data mentah dari recv().
        Mengembalikan list of (header_dict, raw_file_bytes) untuk setiap
        pesan lengkap yang berhasil di-assemble.
        """
        self.buffer += data
        messages = []

        while True:
            if self.state == self.ST_WAIT_LEN:
                if len(self.buffer) >= 4:
                    self.needed = struct.unpack(">I", self.buffer[:4])[0]
                    self.buffer = self.buffer[4:]
                    self.state  = self.ST_WAIT_HEADER
                else:
                    break

            elif self.state == self.ST_WAIT_HEADER:
                if len(self.buffer) >= self.needed:
                    raw_hdr     = self.buffer[:self.needed]
                    self.buffer = self.buffer[self.needed:]
                    try:
                        self.header = json.loads(raw_hdr.decode(ENCODING))
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        print(f"[!] Header parse error dari {self.addr}: {e}")
                        self._reset()
                        break

                    # Apakah pesan ini membawa payload file?
                    if (self.header.get("type") == "file"
                            and self.header.get("size", 0) > 0):
                        self.needed    = self.header["size"]
                        self.file_data = b""
                        self.state     = self.ST_WAIT_FILE
                    else:
                        messages.append((self.header, b""))
                        self._reset()
                else:
                    break

            elif self.state == self.ST_WAIT_FILE:
                remaining = self.needed - len(self.file_data)
                take      = min(len(self.buffer), remaining)
                self.file_data += self.buffer[:take]
                self.buffer     = self.buffer[take:]

                if len(self.file_data) >= self.needed:
                    messages.append((self.header, self.file_data))
                    self._reset()
                else:
                    break

        return messages

    def _reset(self):
        """Reset state machine ke awal untuk pesan berikutnya."""
        self.state     = self.ST_WAIT_LEN
        self.needed    = 4
        self.header    = None
        self.file_data = b""


class UnicastSingleServer:
    """
    Server single-thread menggunakan I/O Multiplexing (modul selectors).

    Tidak membuat thread baru untuk setiap klien. Sebagai gantinya,
    menggunakan select() untuk memantau semua socket secara bersamaan
    dan memproses data dari klien manapun yang siap dibaca.

    Ini memungkinkan banyak klien terhubung dan berkomunikasi secara
    bersamaan meskipun hanya ada satu thread.
    """

    def __init__(self, host=HOST, port=PORT_SINGLE):
        self.host     = host
        self.port     = port
        self.sel      = selectors.DefaultSelector()
        self.sessions = {}   # conn -> ClientSession
        self.srv_sock = None

    def start(self):
        self.srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv_sock.bind((self.host, self.port))
        self.srv_sock.listen(10)
        self.srv_sock.setblocking(False)
        self.sel.register(self.srv_sock, selectors.EVENT_READ)

        print(f"[SERVER - Single-thread (I/O Multiplexing)] Berjalan di {self.host}:{self.port}")
        print("Menunggu koneksi... (tekan Ctrl+C untuk berhenti)\n")

        try:
            while True:
                events = self.sel.select(timeout=1.0)
                for key, mask in events:
                    if key.fileobj is self.srv_sock:
                        self._accept()
                    else:
                        self._on_data(key.fileobj)
        except KeyboardInterrupt:
            print("\n[!] Server dihentikan oleh pengguna.")
        finally:
            for conn in list(self.sessions.keys()):
                self._disconnect(conn)
            try:
                self.sel.unregister(self.srv_sock)
            except Exception:
                pass
            self.srv_sock.close()
            self.sel.close()

    # ---------- Internal ----------

    def _accept(self):
        """Terima koneksi baru dan daftarkan ke selector."""
        try:
            conn, addr = self.srv_sock.accept()
        except OSError:
            return
        # Socket klien di-set blocking agar sendall() bekerja normal.
        # select() tetap bisa memantaunya untuk readability.
        conn.setblocking(True)
        session = ClientSession(conn, addr)
        self.sessions[conn] = session
        self.sel.register(conn, selectors.EVENT_READ)
        print(f"\n[+] Koneksi baru dari {addr} (Single-thread)")

    def _on_data(self, conn):
        """Dipanggil ketika selector mendeteksi data siap dibaca."""
        session = self.sessions.get(conn)
        if not session:
            return

        try:
            data = conn.recv(BUFFER_SIZE)
            if not data:
                raise ConnectionError("Koneksi ditutup oleh klien.")
        except (ConnectionError, OSError):
            self._disconnect(conn)
            return

        try:
            messages = session.feed(data)
        except Exception as e:
            print(f"[!] Error parsing data dari {session.addr}: {e}")
            self._disconnect(conn)
            return

        for header, raw_data in messages:
            try:
                self._process(conn, session, header, raw_data)
            except Exception as e:
                print(f"[!] Error proses pesan dari "
                      f"'{session.username or session.addr}': {e}")

    def _process(self, conn, session, header, raw_data):
        """Proses satu pesan lengkap dari klien."""
        msg_type = header.get("type")

        # -- Registrasi --
        if msg_type == "register":
            self._do_register(conn, session, header)
            return

        # Harus sudah terdaftar untuk pesan lainnya
        if not session.registered:
            try:
                send_header(conn, {"type": "error",
                                    "message": "Anda belum terdaftar."})
            except Exception:
                pass
            return

        username = session.username
        mode     = header.get("mode", "unicast")
        targets  = resolve_targets(header, username, mode)

        if msg_type == "file":
            save_file_on_server(header, raw_data, username)
        elif msg_type == "text":
            pass  # konten ada di dalam header["content"]
        elif msg_type == "get_clients":
            try:
                send_client_list(conn)
            except Exception:
                pass
            return
        else:
            print(f"[!] Tipe pesan tidak dikenal dari '{username}': {msg_type}")
            return

        log_incoming(header, username, mode, targets)
        failed = forward_message(header, raw_data, targets, username)

        ack = {
            "type":         "send_ack",
            "status":       "ok",
            "forwarded_to": [t for t in targets if t not in failed],
            "failed":       failed,
        }
        try:
            send_header(conn, ack)
        except Exception as e:
            print(f"[!] Gagal kirim ACK ke '{username}': {e}")

    def _do_register(self, conn, session, header):
        """Proses paket registrasi dari klien baru."""
        name = header.get("username", "").strip()

        if not name:
            try:
                send_header(conn, {"type": "register_ack", "status": "error",
                                    "message": "Username tidak boleh kosong."})
            except Exception:
                pass
            self._disconnect(conn)
            return

        with clients_lock:
            if name in clients:
                try:
                    send_header(conn, {"type": "register_ack", "status": "error",
                                        "message": f"Username '{name}' sudah digunakan."})
                except Exception:
                    pass
                self._disconnect(conn)
                return
            clients[name] = conn

        session.username   = name
        session.registered = True

        try:
            send_header(conn, {"type": "register_ack", "status": "ok",
                                "message": f"Selamat datang, {name}!"})
        except Exception as e:
            print(f"[!] Gagal kirim register_ack ke '{name}': {e}")
            self._disconnect(conn)
            return

        print(f"[+] '{name}' terdaftar dari {session.addr}. "
              f"Total klien: {len(clients)}")
        broadcast_client_list()

    def _disconnect(self, conn):
        """Bersihkan sesi klien dan hapus dari selector."""
        session = self.sessions.pop(conn, None)
        try:
            self.sel.unregister(conn)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

        if session and session.username:
            with clients_lock:
                clients.pop(session.username, None)
            print(f"[-] '{session.username}' ({session.addr}) terputus. "
                  f"Sisa klien: {len(clients)}")
            broadcast_client_list()


#  SERVER MULTI-THREAD (RELAY)

def read_file_payload(conn, filesize):
    """Baca filesize byte dari conn (blocking)."""
    received = b""
    while len(received) < filesize:
        to_read = min(BUFFER_SIZE, filesize - len(received))
        chunk = conn.recv(to_read)
        if not chunk:
            raise ConnectionError("Koneksi terputus saat membaca payload file.")
        received += chunk
    return received


def handle_client(conn, addr, mode_label):
    """Loop utama untuk satu klien (dipakai oleh server multi-thread)."""
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
        broadcast_client_list()

        # ── Loop terima pesan ──
        while True:
            try:
                header = recv_header(conn)
            except (ConnectionError, struct.error, json.JSONDecodeError, OSError):
                break

            msg_type = header.get("type")
            mode     = header.get("mode", "unicast")
            targets  = resolve_targets(header, username, mode)

            # ── Ambil payload file jika ada ──
            raw_data = b""
            if msg_type == "file":
                filesize = header.get("size", 0)
                try:
                    raw_data = read_file_payload(conn, filesize)
                except ConnectionError as e:
                    print(f"[!] Error baca file dari '{username}': {e}")
                    break
                save_file_on_server(header, raw_data, username)

            elif msg_type == "text":
                pass  # konten ada di header["content"]

            elif msg_type == "get_clients":
                send_client_list(conn)
                continue

            else:
                print(f"[!] Tipe pesan tidak dikenal dari '{username}': {msg_type}")
                continue

            log_incoming(header, username, mode, targets)
            failed = forward_message(header, raw_data, targets, username)

            ack_payload = {
                "type":         "send_ack",
                "status":       "ok",
                "forwarded_to": [t for t in targets if t not in failed],
                "failed":       failed,
            }
            send_header(conn, ack_payload)

    except Exception as e:
        print(f"[!] Error tak terduga untuk '{username or addr}': {e}")
    finally:
        if username:
            with clients_lock:
                clients.pop(username, None)
            print(f"[-] '{username}' ({addr}) terputus. Sisa klien: {len(clients)}")
            broadcast_client_list()
        conn.close()


class UnicastMultiServer:
    """Server multi-thread: membuat thread baru untuk setiap klien."""

    def __init__(self, host=HOST, port=PORT_MULTI):
        self.host = host
        self.port = port

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(20)
            srv.settimeout(1.0)
            print(f"[SERVER - Multi-thread Relay] Berjalan di {self.host}:{self.port}")
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
                        name=f"Thread-{addr}",
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
    print("    single  -> Single-thread I/O Multiplexing (port 9090)")
    print("    multi   -> Multi-thread Relay             (port 9091)")

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