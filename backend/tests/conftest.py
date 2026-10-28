import os
import sys
import asyncio

# Tests drive /admin/reset and the unlimited demo path, which need demo mode.
os.environ.setdefault("DEMO_MODE", "1")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
