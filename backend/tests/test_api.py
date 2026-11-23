import os
import sys

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

os.environ.pop('REPRINT_OFFLINE', None)
os.environ['AUTH_KEY'] = 'test-key'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.tables import schema  # noqa: E402
from src.services import handovers  # noqa: E402


class FakeCache:
    def ping(self):
        return True

    def lpush(self, *args):
        return 1

    def ltrim(self, *args):
        return True


@pytest.fixture()
def client(tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path}/api.db',
                           connect_args={'check_same_thread': False, 'timeout': 30})
    schema.create_all(engine)
    monkeypatch.setenv('SEED_DEMO', 'false')
    import src.main as main
    import src.routes.workflow as routes
    import src.services.security as security
    import src.services.workflow as workflow
    for module in (handovers, workflow, routes, security, main):
        if hasattr(module, 'engine'):
            monkeypatch.setattr(module, 'engine', engine)
    if hasattr(main, 'engine'):
        monkeypatch.setattr(main, 'engine', engine)
    monkeypatch.setattr(routes, 'cache', FakeCache())
    monkeypatch.setattr(workflow, 'cache', FakeCache())
    handovers._locks.clear()
    with TestClient(main.app) as test_client:
        test_client.engine = engine
        yield test_client


def seed_users(engine):
    from src.constants.workflow import Role
    from src.models.tables import users
    from src.services.security import hash_password
    with engine.begin() as conn:
        for role in Role:
            conn.execute(users.insert().values(
                username=role.value, role=role.value,
                password_hash=hash_password('Demo-Walkthrough-2026')))


def login(client, username):
    response = client.post('/login', json={'username': username, 'password': 'Demo-Walkthrough-2026'})
    assert response.status_code == 200, response.text
    return {'Authorization': f"Bearer {response.json()['token']}"}


def submit_run(client, headers):
    from src.services.domain import SPEC
    case = client.post('/cases', headers=headers, json={
        'title': '样例', 'payload': SPEC['sample']})
    assert case.status_code == 201, case.text
    run = client.post(f"/cases/{case.json()['id']}/runs", headers=headers)
    assert run.status_code == 202, run.text
    return run.json()['id']


def finish_run(client, run_id):
    import src.services.workflow as workflow
    from src.services.domain import solve
    with client.engine.connect() as conn:
        from sqlalchemy import select
        from src.models.tables import cases, runs
        row = conn.execute(select(runs.c.case_id).where(runs.c.id == run_id)).first()
        payload = conn.execute(select(cases.c.payload).where(cases.c.id == row[0])).scalar_one()
    claim = workflow.claim()
    assert claim and claim[0] == run_id
    workflow.finish(run_id, claim[1], result=solve(payload, None))
    from sqlalchemy import select
    from src.models.tables import evidence
    with client.engine.connect() as conn:
        return conn.execute(select(evidence.c.id).where(evidence.c.run_id == run_id)).scalar_one()


def test_sign_creates_handover_and_receipt(client, monkeypatch):
    seed_users(client.engine)
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    reviewer = login(client, 'reviewer')
    operator = login(client, 'operator')
    run_id = submit_run(client, operator)
    evidence_id = finish_run(client, run_id)

    view = client.get(f'/evidence/{evidence_id}/handovers', headers=reviewer)
    assert view.status_code == 200
    assert view.json()['outlets'][0]['state'] == 'none'

    signed = client.post(f'/evidence/{evidence_id}/review', headers=reviewer, json={'note': '通过'})
    assert signed.status_code == 200, signed.text
    assert {row['outlet'] for row in signed.json()['handovers']['created']} == {'东城窗口', '自助打印'}

    view = client.get(f'/evidence/{evidence_id}/handovers', headers=reviewer).json()
    assert view['received_all'] is False
    handovers_outlets = [item for item in view['outlets'] if item['state'] != 'none']
    assert {item['state'] for item in handovers_outlets} == {'dispatched'}

    for item in handovers_outlets:
        number = item['awaiting_receipt']
        first = client.post(f'/handovers/{number}/receipt', headers=reviewer)
        assert first.status_code == 200, first.text
        assert first.json()['state'] == 'received'
        assert first.json()['received_by'] == 'reviewer'
        second = client.post(f'/handovers/{number}/receipt', headers=reviewer)
        assert second.status_code == 200
        assert second.json()['received_at'] == first.json()['received_at']

    view = client.get(f'/evidence/{evidence_id}/handovers', headers=reviewer).json()
    assert view['received_all'] is True
    assert all(item['received'] is not False for item in view['outlets'])


def test_offline_sign_succeeds_and_admin_can_filter(client, monkeypatch):
    seed_users(client.engine)
    monkeypatch.setenv('REPRINT_OFFLINE', '1')
    reviewer = login(client, 'reviewer')
    operator = login(client, 'operator')
    evidence_id = finish_run(client, submit_run(client, operator))

    signed = client.post(f'/evidence/{evidence_id}/review', headers=reviewer, json={'note': '通过'})
    assert signed.status_code == 200, signed.text
    assert signed.json()['handovers']['dispatch'][0]['reason'] == 'offline'

    rows = client.get('/handovers', headers=reviewer, params={'state': 'pending'}).json()
    assert {row['outlet'] for row in rows} == {'东城窗口', '自助打印'}
    numbers = {row['number'] for row in rows}

    monkeypatch.delenv('REPRINT_OFFLINE')
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    retried = client.post(f'/evidence/{evidence_id}/handovers', headers=reviewer)
    assert retried.status_code == 200
    assert {row['number'] for row in retried.json()['results'] if row['sent']} == numbers
    assert client.get('/handovers', headers=reviewer, params={'state': 'pending'}).json() == []


def test_viewer_cannot_sign_or_retry(client, monkeypatch):
    seed_users(client.engine)
    viewer = login(client, 'viewer')
    assert client.post('/evidence/1/review', headers=viewer, json={'note': 'x'}).status_code == 403
    assert client.post('/evidence/1/handovers', headers=viewer).status_code == 403
    assert client.get('/handovers', headers=viewer).status_code == 200
