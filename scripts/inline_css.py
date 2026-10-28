"""
Inline frontend/styles.css (minified) into frontend/index.html.

Why: a render-blocking stylesheet costs one extra network round trip before first paint, which is the
largest single cost on a slow mobile connection. styles.css stays the source of truth; run this after
editing it:  python scripts/inline_css.py
"""
import re
from pathlib import Path

root = Path(__file__).resolve().parent.parent / "frontend"
css = (root / "styles.css").read_text(encoding="utf-8")

css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)       # comments
css = re.sub(r"\s+", " ", css)                         # whitespace
css = re.sub(r"\s*([{};,>])\s*", r"\1", css)           # around punctuation
css = re.sub(r":\s+", ":", css)
css = css.replace(";}", "}").strip()

html_path = root / "index.html"
html = html_path.read_text(encoding="utf-8")
block = f"<style data-inline-css>{css}</style>"

if '<link rel="stylesheet" href="styles.css">' in html:
    html = html.replace('<link rel="stylesheet" href="styles.css">', block)
else:
    html, n = re.subn(r"<style data-inline-css>.*?</style>", lambda _: block, html, flags=re.S)
    assert n == 1, "no stylesheet link or inline block found in index.html"

html_path.write_text(html, encoding="utf-8")
print(f"inlined {len(css) / 1024:.1f} KB of CSS into index.html ({len(html) / 1024:.1f} KB total)")
