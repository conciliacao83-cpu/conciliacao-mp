"""
Servidor FastAPI que:
  - Expõe GET /ping para o UptimeRobot (mantém o Render ativo)
  - Inicia o Streamlit como subprocesso na porta 8501
  - Faz proxy de todas as requisições HTTP e WebSocket para o Streamlit
"""
import asyncio
import os
import subprocess
import sys

import httpx
import uvicorn
import websockets
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import Response

app = FastAPI()
STREAMLIT_PORT = 8501


@app.on_event("startup")
async def start_streamlit():
    subprocess.Popen(
        [
            sys.executable, "-m", "streamlit", "run", "app.py",
            "--server.port", str(STREAMLIT_PORT),
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
        ]
    )
    await asyncio.sleep(5)  # aguarda o Streamlit iniciar


@app.get("/ping")
async def ping():
    return {"status": "ok"}


@app.websocket("/{path:path}")
async def ws_proxy(ws: WebSocket, path: str):
    await ws.accept()
    query = ws.scope.get("query_string", b"").decode()
    url = f"ws://localhost:{STREAMLIT_PORT}/{path}"
    if query:
        url += f"?{query}"
    try:
        async with websockets.connect(url) as upstream:

            async def c2u():
                try:
                    while True:
                        await upstream.send(await ws.receive_bytes())
                except Exception:
                    pass

            async def u2c():
                try:
                    async for msg in upstream:
                        if isinstance(msg, bytes):
                            await ws.send_bytes(msg)
                        else:
                            await ws.send_text(msg)
                except Exception:
                    pass

            await asyncio.gather(c2u(), u2c())
    except Exception:
        pass
    finally:
        await ws.close()


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"],
)
async def http_proxy(request: Request, path: str):
    url = f"http://localhost:{STREAMLIT_PORT}/{path}"
    query = request.url.query
    if query:
        url += f"?{query}"
    async with httpx.AsyncClient() as client:
        resp = await client.request(
            method=request.method,
            url=url,
            headers={
                k: v
                for k, v in request.headers.items()
                if k.lower() not in ("host", "content-length")
            },
            content=await request.body(),
            follow_redirects=True,
        )
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers={
            k: v
            for k, v in resp.headers.items()
            if k.lower() not in ("content-encoding", "transfer-encoding", "content-length")
        },
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
