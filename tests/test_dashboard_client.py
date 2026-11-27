from unittest.mock import Mock, patch

from dashboard.client import StreamGuardAPI


def test_get_builds_url_and_query() -> None:
    response = Mock()
    response.json.return_value = {"status": "ok"}
    with patch("dashboard.client.requests.get", return_value=response) as get:
        result = StreamGuardAPI("http://api:8000/").get("/health", limit=5)

    assert result == {"status": "ok"}
    get.assert_called_once_with(
        "http://api:8000/health", params={"limit": 5}, headers={}, timeout=3.0
    )
    response.raise_for_status.assert_called_once()


def test_post_sends_json_payload() -> None:
    response = Mock()
    response.json.return_value = {"is_anomaly": True}
    payload = {"sensor_id": "sensor-1", "temperature": 99.0}
    with patch("dashboard.client.requests.post", return_value=response) as post:
        result = StreamGuardAPI("http://api:8000").post("/predict", payload)

    assert result == {"is_anomaly": True}
    post.assert_called_once_with("http://api:8000/predict", json=payload, headers={}, timeout=3.0)
    response.raise_for_status.assert_called_once()


def test_endpoint_helpers_use_expected_paths() -> None:
    client = StreamGuardAPI("http://api:8000")
    with patch.object(client, "get", return_value=[]) as get:
        client.readings(250)
        client.anomalies(20)

    assert get.call_args_list[0].args == ("/readings/recent",)
    assert get.call_args_list[0].kwargs == {"limit": 250}
    assert get.call_args_list[1].args == ("/anomalies",)
    assert get.call_args_list[1].kwargs == {"limit": 20}


def test_api_key_is_sent_without_leaking_into_url() -> None:
    response = Mock()
    response.json.return_value = []
    with patch("dashboard.client.requests.get", return_value=response) as get:
        StreamGuardAPI("http://api:8000", api_key="secret-key").get("/readings/recent")

    get.assert_called_once_with(
        "http://api:8000/readings/recent",
        params={},
        headers={"X-API-Key": "secret-key"},
        timeout=3.0,
    )
