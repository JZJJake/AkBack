import os
import time
import socket
import logging
import platform
import subprocess
import requests
import atexit

class TdxServerManager:
    """
    Manages the lifecycle of the local TDX API server.
    """
    def __init__(self, port=8080):
        self.port = port
        self.process = None
        self.base_url = f"http://localhost:{port}"
        self.logger = logging.getLogger("TdxServerManager")

        # Determine executable path
        self.project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.web_dir = os.path.join(self.project_root, "external", "tdx-api", "web")

        system = platform.system()
        if system == "Windows":
            self.executable = os.path.join(self.web_dir, "server.exe")
        else:
            self.executable = os.path.join(self.web_dir, "server")

        # Register cleanup
        atexit.register(self.stop)

    def _is_port_open(self):
        """Check if port is listening."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', self.port)) == 0

    def _check_server_status(self):
        """Ping /api/server-status."""
        try:
            resp = requests.get(f"{self.base_url}/api/server-status", timeout=1)
            return resp.status_code == 200
        except:
            return False

    def start(self):
        """Start the server if not already running."""
        if self._is_port_open():
            self.logger.info(f"Port {self.port} is open. Checking if it's TDX server...")
            if self._check_server_status():
                self.logger.info("TDX Server is already running. Attaching to it.")
                return True
            else:
                self.logger.error(f"Port {self.port} is occupied by an unknown service.")
                raise RuntimeError(f"Port {self.port} occupied by unknown service")

        # Start process
        if not os.path.exists(self.executable):
            self.logger.error(f"TDX Server executable not found at {self.executable}")
            raise FileNotFoundError(f"TDX Server executable not found at {self.executable}")

        # Ensure executable permissions on Unix
        if platform.system() != "Windows":
            if not os.access(self.executable, os.X_OK):
                self.logger.info(f"Setting executable permissions for {self.executable}")
                os.chmod(self.executable, 0o755)

        self.logger.info(f"Starting TDX Server from {self.web_dir}...")
        try:
            self.process = subprocess.Popen(
                [self.executable],
                cwd=self.web_dir,
                stdout=subprocess.DEVNULL, # Suppress output or redirect to log?
                stderr=subprocess.DEVNULL
            )

            # Wait for startup
            start_time = time.time()
            while time.time() - start_time < 10:
                if self._check_server_status():
                    self.logger.info("TDX Server started successfully.")
                    return True
                time.sleep(0.5)

            # Timeout
            self.stop()
            self.logger.error("Timed out waiting for TDX Server to start.")
            raise RuntimeError("TDX Server startup timeout")

        except Exception as e:
            self.logger.error(f"Failed to start TDX Server: {e}")
            raise

    def stop(self):
        """Stop the server if we started it."""
        if self.process:
            self.logger.info("Stopping TDX Server...")
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            except Exception as e:
                self.logger.error(f"Error stopping server: {e}")
            finally:
                self.process = None
