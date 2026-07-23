#!/usr/bin/env python3
"""Patch httpuv for rstudio/httpuv#171: segfault when an aborted connection
reaches a request handler with the request's R environment (_env, a
shared_ptr) never initialized -- HttpRequest::env() dereferences it blind and
kills the R process. Bit the Buy List dashboard repeatedly on 2026-07-22/23
(native crash, no R frames; confirmed by a gdb backtrace: requestToEnv(
pEnv=0x0) assigning REQUEST_METHOD).

Patch D (httprequest.cpp) is the total fix: env() lazily initializes _env so
no call site can ever dereference null. A/B/C (webapplication.cpp) stay as
defense-in-depth at the site the gdb trap actually caught.

Usage (inside the airflow-shiny container, as root):
  cd /tmp && rm -rf hp && mkdir hp && cd hp
  Rscript -e 'download.packages("httpuv", destdir=".", repos="https://cloud.r-project.org")'
  tar xf httpuv_*.tar.gz
  python3 /opt/airflow/scripts/patch_httpuv_171.py httpuv
  R CMD INSTALL httpuv

NOTE: an image rebuild reinstalls stock httpuv from CRAN (see Dockerfile);
re-run this patch after any rebuild until the fix lands upstream.
"""
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
applied = []


def patch(path, anchor, replacement, tag):
    text = path.read_text()
    if anchor in text:
        path.write_text(text.replace(anchor, replacement, 1))
        applied.append(tag)


webapp = root / "src" / "webapplication.cpp"
httpreq = root / "src" / "httprequest.cpp"

# A: null guard before the dereference in requestToEnv
patch(
    webapp,
    "  using namespace Rcpp;\n\n  Environment& env = *pEnv;",
    "  using namespace Rcpp;\n\n"
    "  // rstudio/httpuv#171: an aborted connection can reach here with the\n"
    "  // request env never allocated; dereferencing NULL kills the R process\n"
    "  if (pEnv == NULL) return;\n"
    "  Environment& env = *pEnv;",
    "A:requestToEnv-null-guard",
)

# B: missing `return` after the _onHeaders.isNULL() early callback, plus a
# checked env pointer for the rest of onHeaders
patch(
    webapp,
    "    callback(null_ptr);\n  }\n\n  requestToEnv(pRequest, &pRequest->env());",
    "    callback(null_ptr);\n"
    "    return;\n"
    "  }\n\n"
    "  requestToEnv(pRequest, &pRequest->env());",
    "B:onHeaders-missing-return",
)

# D: the total fix -- env() lazily initializes _env instead of dereferencing
# a null shared_ptr (covers onHeaders, onBodyData, getResponse, onWSOpen)
patch(
    httpreq,
    "Rcpp::Environment& HttpRequest::env() {\n"
    "  ASSERT_MAIN_THREAD()\n"
    "  return *_env;\n"
    "}",
    "Rcpp::Environment& HttpRequest::env() {\n"
    "  ASSERT_MAIN_THREAD()\n"
    "  // rstudio/httpuv#171: aborted connections can reach handlers with _env\n"
    "  // never initialized (or already reset); lazily (re)create it instead of\n"
    "  // dereferencing a null shared_ptr and killing the R process.\n"
    "  if (!_env) {\n"
    "    _initializeEnv();\n"
    "  }\n"
    "  return *_env;\n"
    "}",
    "D:env-lazy-init",
)

if "D:env-lazy-init" not in applied:
    print("PATCH FAILED: env() anchor not found in httprequest.cpp")
    sys.exit(1)

print("patched:", ", ".join(applied))
