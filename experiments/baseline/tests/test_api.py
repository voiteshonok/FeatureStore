from httpx import ASGITransport, AsyncClient

from baseline.api import create_app
from baseline.settings import Settings


async def test_health_does_not_touch_postgres() -> None:
    app = create_app(Settings(metrics_port=0))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
