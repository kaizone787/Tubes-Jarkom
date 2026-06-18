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
BUFFER_SIZE  = 65536
ENCODING     = "utf-8"
SAVE_DIR     = "received_files"

FILE_TYPE_MAP = {
    "4":  ([".txt"],         "Dokumen .txt"),
    "5":  ([".docx"],        "Dokumen .docx"),
    "6":  ([".pdf"],         "Dokumen .pdf"),
    "7":  ([".jpg", ".jpeg"],"Gambar .jpg"),
    "8":  ([".png"],         "Gambar .png"),
    "9":  ([".mp3"],         "Audio .mp3"),
    "10": ([".mp4"],         "Video .mp4"),
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
    """Menerima tepat n byte dari socket."""
    data = b""
    while len(data) < n:
        chunk = conn.recv(min(n - len(data), BUFFER_SIZE))
        if not chunk:
            raise ConnectionError("Koneksi terputus.")
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

def safe_print(msg):
    """Thread-safe print."""
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

        # ── Update daftar klien online ──
        if msg_type == "client_list":
            with clients_lock:
                known_clients.clear()
                known_clients.extend(header.get("clients", []))
            with clients_lock:
                cl = list(known_clients)
            safe_print(f"\n  [INFO] Klien online sekarang: {cl}")

        # ── Pesan teks masuk ──
        elif msg_type == "text":
            sender  = header.get("sender", "?")
            subtype = header.get("subtype", "")
            content = header.get("content", "")
            safe_print(f"\n{'─'*55}")
            safe_print(f"  [PESAN MASUK] Dari: {sender}  |  Tipe: teks/{subtype}")
            safe_print(f"  Isi: {content}")
            safe_print(f"{'─'*55}")

        # ── File masuk (streaming langsung ke disk) ──
        elif msg_type == "file":
            sender   = header.get("sender", "?")
            filename = header.get("filename", "unknown")
            filesize = header.get("size", 0)
            safe_print(f"\n{'─'*55}")
            safe_print(f"  [FILE MASUK] Dari: {sender}  |  File: {filename} ({filesize} bytes)")

            save_path     = os.path.join(SAVE_DIR, f"from_{sender}_{filename}")
            received_size = 0
            last_print    = 0
            try:
                with open(save_path, "wb") as f:
                    while received_size < filesize:
                        to_read = min(BUFFER_SIZE, filesize - received_size)
                        chunk = conn.recv(to_read)
                        if not chunk:
                            raise ConnectionError("Koneksi terputus saat terima file.")
                        f.write(chunk)
                        received_size += len(chunk)
                        
                        if filesize > 0:
                            now = time.time()
                            if now - last_print > 0.1:  # Update terminal maksimal 10x per detik
                                pct = (received_size / filesize) * 100
                                print(f"\r  Progress terima: {received_size}/{filesize} "
                                      f"bytes ({pct:.1f}%)", end="", flush=True)
                                last_print = now
                                
                print(f"\r  Progress terima: {filesize}/{filesize} bytes (100.0%)", flush=True)
                safe_print(f"  File disimpan: {save_path}")
            except ConnectionError as e:
                safe_print(f"\n  [!] Error terima file: {e}")
                # Bersihkan file parsial
                try:
                    os.remove(save_path)
                except OSError:
                    pass
                break
            safe_print(f"{'─'*55}")

        # ── ACK dari server ──
        elif msg_type == "send_ack":
            fwd  = header.get("forwarded_to", [])
            fail = header.get("failed", [])
            safe_print(f"\n  [ACK] Terkirim ke: {fwd}"
                       + (f"  | Gagal: {fail}" if fail else ""))

        # ── ACK registrasi ──
        elif msg_type == "register_ack":
            status = header.get("status")
            msg    = header.get("message", "")
            safe_print(f"\n  [REGISTER] {status.upper()}: {msg}")

        # ── Pesan lainnya ──
        else:
            safe_print(f"\n  [INFO] Pesan dari server: {header}")

#  INPUT & KIRIM TEKS
def prompt_text_input(subtype):
    """Minta input teks dari pengguna sesuai subtipe. Return None jika dibatalkan."""
    hint = TEXT_SUBTYPE_HINTS[subtype]
    safe_print(f"\n  Ketik teks ({hint})  [ketik 0 untuk batal]")
    if subtype == "paragraph":
        lines = []
        while True:
            line = input("  > ")
            if line.strip() == "0" and not lines:
                return None
            if line.strip() == ".":
                break
            lines.append(line)
        content = " ".join(lines).strip()
    else:
        content = input("  > ").strip()
        if content == "0":
            return None
    while not content:
        safe_print("  [!] Teks tidak boleh kosong.")
        content = input("  > ").strip()
        if content == "0":
            return None
    return content

def prompt_file_path(allowed_exts, label):
    """Minta path file dari pengguna dengan validasi ekstensi. Return None jika dibatalkan."""
    while True:
        path = input(f"  Path file {label} [0 = batal]: ").strip().strip('"').strip("'")
        if path == "0":
            return None
        if not path:
            safe_print("  [!] Input tidak boleh kosong.")
            continue
        if not os.path.isfile(path):
            safe_print(f"  [!] File tidak ditemukan: {path}")
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext not in allowed_exts:
            safe_print(f"  [!] Ekstensi harus salah satu dari: {allowed_exts}")
            continue
        return path

def prompt_targets(mode):
    """Minta input target tergantung mode. Return None jika dibatalkan."""
    if mode == "broadcast":
        return []
    elif mode == "multicast":
        with clients_lock:
            online = list(known_clients)
        safe_print(f"  Klien online: {online}")
        raw = input("  Masukkan username tujuan (pisah koma) [0 = batal]: ").strip()
        if raw == "0":
            return None
        targets = [t.strip() for t in raw.split(",") if t.strip()]
        return targets
    else:  # unicast
        with clients_lock:
            online = list(known_clients)
        safe_print(f"  Klien online: {online}")
        target = input("  Masukkan username tujuan [0 = batal]: ").strip()
        if target == "0":
            return None
        return [target] if target else []

#  MENU PENGIRIMAN
def menu_message(sock, username):
    """Menu interaktif untuk memilih jenis pesan dan mengirimnya."""
    while True:
        print("\n" + "─"*50)
        print("  PILIH JENIS PESAN:")
        print("  1.  Teks singkat (1-5 kata)")
        print("  2.  Teks kalimat (1 kalimat panjang)")
        print("  3.  Teks paragraf")
        print("  4.  Dokumen .txt")
        print("  5.  Dokumen .docx")
        print("  6.  Dokumen .pdf")
        print("  7.  Gambar .jpg")
        print("  8.  Gambar .png")
        print("  9.  Audio .mp3")
        print("  10. Video .mp4")
        print("  L.  Lihat klien online")
        print("  0.  Kembali / Tutup koneksi")
        print("─"*50)
        choice = input("  Pilihan: ").strip()

        if choice == "0":
            safe_print("  Menutup koneksi...")
            break

        if choice.upper() == "L":
            with clients_lock:
                safe_print(f"  Klien online: {list(known_clients)}")
            continue

        # Validasi pilihan
        if choice not in ("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"):
            safe_print("  [!] Pilihan tidak valid.")
            continue

        # Pilih mode pengiriman
        print("\n  MODE PENGIRIMAN:")
        print("  1. Unicast   (A -> B)")
        print("  2. Multicast (A -> B, C, ...)")
        print("  3. Broadcast (A -> Semua)")
        print("  0. Batal")
        mode_choice = input("  Mode: ").strip()
        if mode_choice == "0":
            safe_print("  [i] Dibatalkan.")
            continue
        mode_map = {"1": "unicast", "2": "multicast", "3": "broadcast"}
        mode = mode_map.get(mode_choice)
        if not mode:
            safe_print("  [!] Pilihan mode tidak valid.")
            continue

        targets = prompt_targets(mode)
        if targets is None:
            safe_print("  [i] Dibatalkan.")
            continue

        if mode in ("unicast", "multicast") and not targets:
            safe_print("  [!] Tidak ada target yang valid.")
            continue

        # ── Kirim teks ──
        if choice in ("1", "2", "3"):
            subtype_map = {"1": "short", "2": "sentence", "3": "paragraph"}
            subtype = subtype_map[choice]
            content = prompt_text_input(subtype)
            if content is None:
                safe_print("  [i] Dibatalkan.")
                continue
            header = {
                "type":    "text",
                "subtype": subtype,
                "content": content,
                "size":    len(content),
                "mode":    mode,
            }
            if mode == "unicast":
                header["target"] = targets[0]
            elif mode == "multicast":
                header["targets"] = targets
            try:
                send_header(sock, header)
                safe_print("  [+] Pesan teks dikirim, menunggu ACK...")
            except Exception as e:
                safe_print(f"  [!] Gagal kirim: {e}")

        # ── Kirim file ──
        elif choice in FILE_TYPE_MAP:
            exts, label = FILE_TYPE_MAP[choice]
            filepath = prompt_file_path(exts, label)
            if filepath is None:
                safe_print("  [i] Dibatalkan.")
                continue
            filename = os.path.basename(filepath)
            filesize = os.path.getsize(filepath)
            ext      = os.path.splitext(filename)[1].lstrip(".").lower()

            header = {
                "type":     "file",
                "subtype":  ext,
                "filename": filename,
                "size":     filesize,
                "mode":     mode,
            }
            if mode == "unicast":
                header["target"] = targets[0]
            elif mode == "multicast":
                header["targets"] = targets

            try:
                send_header(sock, header)
                sent = 0
                last_print = 0
                with open(filepath, "rb") as f:
                    while True:
                        chunk = f.read(BUFFER_SIZE)
                        if not chunk:
                            break
                        sock.sendall(chunk)
                        sent += len(chunk)
                        
                        if filesize > 0:
                            now = time.time()
                            if now - last_print > 0.1:  # Update terminal maksimal 10x per detik
                                pct = (sent / filesize) * 100
                                print(f"\r  Progress kirim: {sent}/{filesize} "
                                      f"bytes ({pct:.1f}%)", end="", flush=True)
                                last_print = now
                                
                print(f"\r  Progress kirim: {filesize}/{filesize} bytes (100.0%)", flush=True)
                safe_print(f"  [+] File '{filename}' dikirim, menunggu ACK...")
            except Exception as e:
                safe_print(f"  [!] Gagal kirim file: {e}")

#  KONEKSI & REGISTRASI
def connect_and_run(host, port, mode_label):
    """Hubungkan ke server, registrasi, lalu masuk ke menu pengiriman."""
    print(f"\n[CLIENT - {mode_label}]")
    username = input("  Masukkan username Anda: ").strip()
    while not username:
        safe_print("  [!] Username tidak boleh kosong.")
        username = input("  Masukkan username Anda: ").strip()

    print(f"  Menghubungkan ke {host}:{port} ...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
    except ConnectionRefusedError:
        safe_print(f"[!] Koneksi ditolak. Pastikan server berjalan di {host}:{port}")
        return
    except Exception as e:
        safe_print(f"[!] Error koneksi: {e}")
        return

    # ── Registrasi ──
    try:
        send_header(sock, {"type": "register", "username": username})
    except Exception as e:
        safe_print(f"[!] Gagal kirim registrasi: {e}")
        sock.close()
        return

    # ── Mulai thread listener ──
    t = threading.Thread(
        target=listener_thread,
        args=(sock, username),
        daemon=True,
        name=f"Listener-{username}",
    )
    t.start()
    time.sleep(0.5)  # beri waktu listener memproses register_ack

    safe_print(f"\n[+] Berhasil terhubung sebagai '{username}'")
    safe_print(f"    Server: {host}:{port}  |  Mode: {mode_label}")

    # ── Menu pengiriman ──
    try:
        menu_message(sock, username)
    finally:
        sock.close()

#  MENU UTAMA
def menu_main():
    """Tampilkan menu utama dan pilih mode koneksi."""
    print("\n" + "="*55)
    print("        CLIENT SOCKET PROGRAMMING")
    print("="*55)
    print("  Pilih Server Tujuan:")
    print("  1. Server Single-thread (Port 9090)")
    print("  2. Server Multi-thread  (Port 9091)")
    print("  0. Keluar")
    print("="*55)

    choice = input("  Pilihan: ").strip()

    if choice == "0":
        print("Keluar.")
        sys.exit(0)

    if choice not in ("1", "2"):
        safe_print("[!] Pilihan tidak valid.")
        return

    host = input("  Host server (default: 127.0.0.1): ").strip() or "127.0.0.1"

    if choice == "1":
        port_input = input(f"  Port (default: {PORT_SINGLE}): ").strip()
        port = int(port_input) if port_input.isdigit() else PORT_SINGLE
        connect_and_run(host, port, "Server Single-thread")
    elif choice == "2":
        port_input = input(f"  Port (default: {PORT_MULTI}): ").strip()
        port = int(port_input) if port_input.isdigit() else PORT_MULTI
        connect_and_run(host, port, "Server Multi-thread")

def main():
    while True:
        menu_main()
        again = input("\nKembali ke menu utama? (y/n): ").strip().lower()
        if again != "y":
            print("Program selesai.")
            break

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        input("Tekan Enter untuk keluar...")
