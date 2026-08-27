"""Create or promote an admin account in the ClusterTalk auth database."""
import sqlite3
import hashlib
import os

from node.database import default_database_path

db_path = default_database_path()
admin_username = "admin"
admin_password = "123456"

def hash_password(plaintext):
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", plaintext.encode("utf-8"), salt, 100_000)
    return f"{salt.hex()}:{dk.hex()}"

conn = sqlite3.connect(db_path, timeout=5)
conn.execute("PRAGMA busy_timeout=5000;")

# Check if admin exists
row = conn.execute("SELECT username, role FROM users WHERE username = ?", (admin_username,)).fetchone()

if row is None:
    # Create new admin account
    pw_hash = hash_password(admin_password)
    conn.execute(
        "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, 'admin', ?)",
        (admin_username, pw_hash, __import__("time").time()),
    )
    print(f"Created admin account: {admin_username} / {admin_password}")
else:
    # Promote existing user to admin
    conn.execute("UPDATE users SET role = 'admin' WHERE username = ?", ("admin",))
    print(f"Promoted {row[0]} to admin (password: {admin_password})")

conn.commit()
conn.close()