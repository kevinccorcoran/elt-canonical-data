#!/usr/bin/env python3
"""Inject the schema-color script into a freshly generated dbt docs index.html.

Runs in the docs container after `dbt docs generate` (and after the canonical
manifest prune). Idempotent: skips if the marker is already present. dbt
regenerates index.html on every boot, so this re-injects each start.

Usage: python inject_schema_colors.py <index.html> <schema_colors.js>
"""
import sys

MARK = "schema-color-inject"


def main():
    html_path, js_path = sys.argv[1], sys.argv[2]
    with open(js_path, encoding="utf-8") as f:
        js = f.read()
    with open(html_path, encoding="utf-8") as f:
        html = f.read()

    if MARK in html:
        print("schema-color: already injected, skipping")
        return

    tag = '<script id="%s">\n%s\n</script>' % (MARK, js)
    if "</body>" in html:
        html = html.replace("</body>", tag + "\n</body>", 1)
    else:                       # extremely defensive; dbt always emits </body>
        html = html + tag
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print("schema-color: injected into", html_path)


if __name__ == "__main__":
    main()
