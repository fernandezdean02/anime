from src.main import app

# Entry point for Railway is `uvicorn serve:app` — see railway.json
# This file only exists as an import shim so serve.py can do `from api import app`.
#
# Do NOT add `uvicorn.run(...)` at module level here. If you need a local
# dev entry point, use the guarded block below — never unindented at module scope.

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=True)
