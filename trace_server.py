"""Serve the complete UzoAgro application and shared Neon-backed API locally."""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app import app as backend
from pathlib import Path

app = backend
# Only the local server replaces the API root health response with the homepage.
# Reuse the original app so its startup and middleware execute once.
app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']

@app.get('/')
def homepage():
    return FileResponse(Path(__file__).parent / 'frontend' / 'index.html')

app.mount("/", StaticFiles(directory=Path(__file__).parent / "frontend", html=True), name="frontend")
