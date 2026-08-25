import os
import subprocess
import sys

if __name__ == "__main__":
	# Ensure node/server.py runs continuously
	subprocess.run([sys.executable, "node/server.py"])
