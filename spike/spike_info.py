import socket
import re

HOST, PORT = "rtserve.iris.washington.edu", 18000

sock = socket.create_connection((HOST, PORT), timeout=15)
sock.sendall(b"INFO STREAMS\r\n")

chunks = []
sock.settimeout(10)
try:
    while True:
        data = sock.recv(65536)
        if not data:
            break
        chunks.append(data)
except socket.timeout:
    pass
sock.close()

blob = b"".join(chunks)
print(f"total bytes received: {len(blob)}")
text = blob.decode("latin-1", errors="replace")

with open("spike/streams_dump.xml", "w") as f:
    f.write(text)

for needle in ["Madrid", "Reykjan", "Iceland"]:
    matches = [m.start() for m in re.finditer(needle, text, re.IGNORECASE)]
    print(f"'{needle}': {len(matches)} occurrences")
    for m in matches[:5]:
        print("  ...", text[max(0, m-120):m+40].replace("\n", " "))
