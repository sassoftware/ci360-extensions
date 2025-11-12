import httpx
from fastapi import Request
from scr_gateway import SCRGateway

async def get_http_client(request: Request) -> httpx.AsyncClient:
  return request.app.state.http_client

def get_scr_gateway(request: Request) -> SCRGateway:
  return request.app.state.scr_gateway
