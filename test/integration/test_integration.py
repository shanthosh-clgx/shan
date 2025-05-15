# test_aiohttp_integration.py
import asyncio
import pytest
import aiohttp
from aiohttp import web

async def mock_handler(request):
    return web.Response(text="Hello from Mock!", status=200)

async def create_mock_app():
    app = web.Application()
    app.add_routes([web.get('/test', mock_handler)])
    return app

@pytest.fixture
async def aiohttp_client():
    app = await create_mock_app()
    client = aiohttp.ClientSession()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8081)
    await site.start()
    yield client
    await client.close()
    await site.stop()
    await runner.cleanup()

async def test_simple_aiohttp_integration(aiohttp_client):
    async with aiohttp_client.get('http://localhost:8081/test') as response:
        assert response.status == 200
        text = await response.text()
        assert text == "Hello from Mock!"