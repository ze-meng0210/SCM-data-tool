import os
import sys
from pathlib import Path

import uvicorn


PROJECT_ROOT = Path(__file__).resolve().parent
APP_DIR = PROJECT_ROOT / "程序文件"

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from web_app import app  # noqa: E402


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
