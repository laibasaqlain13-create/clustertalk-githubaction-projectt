"""Create or promote an admin account in the ClusterTalk auth database."""
import sqlite3
import hashlib
import os

db_path = "clustertalk-auth.db"

def hash_password(plaintext):
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", plaintext.encode("utf-8"), salt, 100_000)
    return f"{salt.hex()}:{dk.hex()}"

conn = sqlite3.connect(db_path, timeout=5)
conn.execute("PRAGMA busy_timeout=5000;")

# Check if admin exists
row = conn.execute("SELECT username, role FROM users WHERE username = ?", ("admin",)).fetchone()

if row is None:
    # Create new admin account
    pw_hash = hash_password("admin123")
    conn.execute(
        "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, 'admin', ?)",
        ("admin", pw_hash, __import__("time").time()),
    )
    print("Created admin account: admin / admin123")
else:
    # Promote existing user to admin
    conn.execute("UPDATE users SET role = 'admin' WHERE username = ?", ("admin",))
    print(f"Promoted {row[0]} to admin (password: admin123)")

conn.commit()
conn.close()