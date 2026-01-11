import subprocess
import sys
import os
import signal
import time


def main():
    root = os.path.dirname(os.path.abspath(__file__))
    web_dir = os.path.join(root, 'web')

    procs = []

    def spawn(cmd, cwd=None):
        return subprocess.Popen(cmd, cwd=cwd, stdin=subprocess.PIPE, stdout=sys.stdout, stderr=sys.stderr)

    try:
        # Start static server for web
        http_cmd = [sys.executable, '-m', 'http.server', '8080']
        http = spawn(http_cmd, cwd=web_dir)
        procs.append(http)
        print('Started web on http://localhost:8080')

        # Start FastAPI app
        api_cmd = [sys.executable, 'main.py']
        api = spawn(api_cmd, cwd=root)
        procs.append(api)
        print('Started API on http://127.0.0.1:8000')

        # Wait for either to exit
        while True:
            exited = [p for p in procs if p.poll() is not None]
            if exited:
                code = exited[0].returncode
                print(f"Process exited with code {code}; shutting down...")
                break
            time.sleep(0.3)

    except KeyboardInterrupt:
        print('Interrupted; shutting down...')
    finally:
        for p in procs:
            if p.poll() is None:
                try:
                    p.terminate()
                except Exception:
                    pass
        # Give them a moment, then force kill if needed
        deadline = time.time() + 3
        for p in procs:
            while p.poll() is None and time.time() < deadline:
                time.sleep(0.1)
            if p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass


if __name__ == '__main__':
    main()
