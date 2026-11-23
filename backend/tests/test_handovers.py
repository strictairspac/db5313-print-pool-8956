import os
import sys
import threading

import pytest
from sqlalchemy import create_engine, select

os.environ.setdefault('AUTH_KEY', 'test-key')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.constants.workflow import HandoverState  # noqa: E402
from src.models.tables import cases, evidence, handover_batches, runs, schema  # noqa: E402
from src.services import handovers, workflow  # noqa: E402


@pytest.fixture()
def db(tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path}/test.db',
                           connect_args={'check_same_thread': False, 'timeout': 30})
    schema.create_all(engine)
    for module in (handovers, workflow):
        monkeypatch.setattr(module, 'engine', engine)
    handovers._locks.clear()
    return engine


def make_evidence(engine, outlet='东城窗口', tickets=None):
    if tickets is None:
        tickets = [{'取号单': 'PT-003', '网点': outlet, '逾时时刻（毫秒）': 30}]
    report = {'summary': 'x', 'metrics': {}, 'sections': [
        {'title': '逾时处理清单', 'rows': tickets},
        {'title': '网点容量', 'rows': [{'网点': '东城窗口'}, {'网点': '西站窗口'}, {'网点': '自助打印'}]},
    ]}
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(
            slug=str(os.urandom(8).hex()), title='t', payload={}, author='op',
            created_at='t')).inserted_primary_key[0]
        run_id = conn.execute(runs.insert().values(
            case_id=case_id, status='complete', owner='op', created_at='t')).inserted_primary_key[0]
        evidence_id = conn.execute(evidence.insert().values(
            run_id=run_id, report=report, digest='0' * 64)).inserted_primary_key[0]
    return evidence_id


def batches(engine, evidence_id=None):
    with engine.connect() as conn:
        query = select(handover_batches).order_by(handover_batches.c.id)
        if evidence_id is not None:
            query = query.where(handover_batches.c.evidence_id == evidence_id)
        return [dict(row) for row in conn.execute(query).mappings()]


def test_sign_offline_still_records_pending_handovers(db, monkeypatch):
    monkeypatch.setenv('REPRINT_OFFLINE', '1')
    tickets = [{'取号单': 'PT-003', '网点': '东城窗口', '逾时时刻（毫秒）': 30},
               {'取号单': 'PT-006', '网点': '自助打印', '逾时时刻（毫秒）': 43}]
    identity = make_evidence(db, tickets=tickets)
    result = workflow.review(identity, '核对通过', 'reviewer')
    assert result['reviewer'] == 'reviewer'
    rows = batches(db, identity)
    assert {row['outlet'] for row in rows} == {'东城窗口', '自助打印'}
    assert all(row['state'] == HandoverState.PENDING for row in rows)
    assert all(row['number'].startswith('HO-') for row in rows)
    # 网点容量里没有逾时的网点不产生交接。
    assert '西站窗口' not in {row['outlet'] for row in rows}


def test_sign_online_dispatches_per_outlet(db, monkeypatch):
    monkeypatch.delenv('REPRINT_OFFLINE', raising=False)
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    identity = make_evidence(db)
    workflow.review(identity, '核对通过', 'reviewer')
    rows = batches(db, identity)
    assert len(rows) == 1
    assert rows[0]['state'] == HandoverState.DISPATCHED


def test_same_outlet_waits_for_receipt_before_next_batch(db, monkeypatch):
    monkeypatch.delenv('REPRINT_OFFLINE', raising=False)
    sent = []
    monkeypatch.setattr(handovers, 'deliver', lambda batch: sent.append(batch['number']) or True)
    first = make_evidence(db)
    second = make_evidence(db)
    workflow.review(first, '第一批', 'reviewer')
    workflow.review(second, '第二批', 'reviewer')

    rows = batches(db)
    rows.sort(key=lambda row: row['order_seq'])
    assert [row['order_seq'] for row in rows] == [1, 2]
    assert rows[0]['state'] == HandoverState.DISPATCHED
    # 后一批已建立待交接记录，但未发送，也无法越过队首。
    assert rows[1]['state'] == HandoverState.PENDING
    assert sent == [rows[0]['number']]

    handovers.receive(rows[0]['number'], '补打终端')
    rows = batches(db)
    assert [row['state'] for row in rows] == [HandoverState.RECEIVED, HandoverState.DISPATCHED]
    assert sent == [batches(db)[0]['number'], batches(db)[1]['number']]


def test_outlets_do_not_block_each_other(db, monkeypatch):
    monkeypatch.delenv('REPRINT_OFFLINE', raising=False)
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    east = make_evidence(db, tickets=[{'取号单': 'PT-003', '网点': '东城窗口', '逾时时刻（毫秒）': 30}])
    west = make_evidence(db, tickets=[{'取号单': 'PT-006', '网点': '西站窗口', '逾时时刻（毫秒）': 43}])
    workflow.review(east, '东城', 'reviewer')
    workflow.review(west, '西站', 'reviewer')
    for row in batches(db):
        assert row['state'] == HandoverState.DISPATCHED


def test_stalled_reprint_system_does_not_block_sign(db, monkeypatch):
    """投递挂起时签署仍能提交，交接记录留在待交接。"""
    monkeypatch.delenv('REPRINT_OFFLINE', raising=False)

    def slow(batch):
        raise TimeoutError('补打系统无响应')

    monkeypatch.setattr(handovers, 'deliver', slow)
    identity = make_evidence(db)
    result = workflow.review(identity, '核对通过', 'reviewer')
    assert result['reviewer'] == 'reviewer'
    rows = batches(db, identity)
    assert len(rows) == 1
    assert rows[0]['state'] == HandoverState.PENDING


def test_batch_number_is_stable_for_same_evidence_and_outlet(db, monkeypatch):
    monkeypatch.setenv('REPRINT_OFFLINE', '1')
    identity = make_evidence(db)
    workflow.review(identity, '核对通过', 'reviewer')
    number = batches(db, identity)[0]['number']
    assert number == handovers.batch_number(identity, '东城窗口')
    # 重试和断线不改号。
    handovers.retry_outlet('东城窗口')
    assert batches(db, identity)[0]['number'] == number


def test_receipt_of_later_batch_does_not_release_out_of_order(db, monkeypatch):
    """后一批先回执时只标记该批，队首未回执前不会重发或乱序放行。"""
    monkeypatch.setenv('REPRINT_OFFLINE', '1')
    first = make_evidence(db)
    second = make_evidence(db)
    workflow.review(first, '第一批', 'reviewer')
    workflow.review(second, '第二批', 'reviewer')
    rows = sorted(batches(db), key=lambda row: row['order_seq'])

    receipt = handovers.receive(rows[1]['number'], '补打终端')
    assert receipt['state'] == HandoverState.RECEIVED
    remaining = sorted(batches(db), key=lambda row: row['order_seq'])
    assert remaining[0]['state'] == HandoverState.PENDING
    assert remaining[1]['state'] == HandoverState.RECEIVED


def test_retry_of_later_batch_does_not_preempt_queue_head(db, monkeypatch):
    monkeypatch.setenv('REPRINT_OFFLINE', '1')
    first = make_evidence(db)
    second = make_evidence(db)
    workflow.review(first, '第一批', 'reviewer')
    workflow.review(second, '第二批', 'reviewer')

    monkeypatch.delenv('REPRINT_OFFLINE', raising=False)
    sent = []
    monkeypatch.setattr(handovers, 'deliver', lambda batch: sent.append(batch['number']) or True)
    results = handovers.retry_evidence(second)
    rows = sorted(batches(db), key=lambda row: row['order_seq'])
    # 队列整体还是待交接，重试后一批只会补发队首，后一批仍留队。
    assert rows[0]['state'] == HandoverState.DISPATCHED
    assert rows[1]['state'] == HandoverState.PENDING
    assert sent == [rows[0]['number']]
    assert results[0]['sent'] is True
    assert results[0]['number'] == rows[0]['number']
    # 队首回执前，重试后一批仍不发送。
    results = handovers.retry_evidence(second)
    assert sent == [rows[0]['number']]
    assert results[0]['reason'] == 'awaiting_receipt'
    assert results[0]['number'] == rows[0]['number']


def test_sign_is_single_use_and_creates_one_batch(db, monkeypatch):
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    identity = make_evidence(db)
    workflow.review(identity, '第一次', 'reviewer')
    with pytest.raises(Exception) as excinfo:
        workflow.review(identity, '第二次', 'reviewer')
    assert getattr(excinfo.value, 'status_code', None) == 409
    assert len(batches(db, identity)) == 1


def test_evidence_without_timeouts_has_no_handover(db, monkeypatch):
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    identity = make_evidence(db, tickets=[])
    result = workflow.review(identity, '无逾时', 'reviewer')
    assert result['handovers']['created'] == []
    assert batches(db, identity) == []


def test_duplicate_receipt_is_idempotent(db, monkeypatch):
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    identity = make_evidence(db)
    workflow.review(identity, '核对通过', 'reviewer')
    number = batches(db, identity)[0]['number']

    first = handovers.receive(number, '补打终端')
    second = handovers.receive(number, '补打终端')
    assert first['state'] == HandoverState.RECEIVED
    assert first['duplicate'] is False
    assert second['duplicate'] is True
    assert second['received_at'] == first['received_at']
    with db.connect() as conn:
        actions = [row[0] for row in conn.execute(
            select(workflow.audit.c.action).where(workflow.audit.c.reference == number))]
    assert actions.count('handover_receive') == 1


def test_retry_keeps_batch_number(db, monkeypatch):
    monkeypatch.setenv('REPRINT_OFFLINE', '1')
    identity = make_evidence(db)
    workflow.review(identity, '核对通过', 'reviewer')
    original = batches(db, identity)[0]
    before = len(batches(db))

    monkeypatch.delenv('REPRINT_OFFLINE', raising=False)
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    handovers.retry_outlet(original['outlet'])
    rows = batches(db)
    assert len(rows) == before
    assert rows[0]['number'] == original['number']
    assert rows[0]['state'] == HandoverState.DISPATCHED


def test_evidence_view_reports_receipt_state(db, monkeypatch):
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    identity = make_evidence(db, tickets=[
        {'取号单': 'PT-003', '网点': '东城窗口', '逾时时刻（毫秒）': 30}])
    workflow.review(identity, '核对通过', 'reviewer')
    view = handovers.evidence_view(identity, {'sections': [
        {'title': '网点容量', 'rows': [{'网点': '东城窗口'}, {'网点': '西站窗口'}]}]})
    assert view['received_all'] is False
    east = next(item for item in view['outlets'] if item['outlet'] == '东城窗口')
    west = next(item for item in view['outlets'] if item['outlet'] == '西站窗口')
    assert east['received'] is False
    assert east['awaiting_receipt'] == east['batches'][0]['number']
    assert west['state'] == 'none'
    handovers.receive(east['awaiting_receipt'], '补打终端')
    view = handovers.evidence_view(identity, {'sections': []})
    assert all(item['received'] is not False for item in view['outlets'])
    assert view['received_all'] is True


def test_concurrent_signs_keep_effective_order(db, monkeypatch):
    monkeypatch.delenv('REPRINT_OFFLINE', raising=False)
    monkeypatch.setattr(handovers, 'deliver', lambda batch: True)
    identities = [make_evidence(db) for _ in range(4)]
    barrier = threading.Barrier(len(identities))
    errors = []

    def sign(identity, label):
        try:
            barrier.wait()
            workflow.review(identity, label, 'reviewer')
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    order = []
    threads = []
    for index, identity in enumerate(identities):
        thread = threading.Thread(target=sign, args=(identity, f'batch-{index}'))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    rows = sorted(batches(db), key=lambda row: row['order_seq'])
    assert [row['order_seq'] for row in rows] == [1, 2, 3, 4]
    assert all(row['outlet'] == '东城窗口' for row in rows)
    # 只有队首被发送，其余等待回执，顺序即签署生效顺序。
    assert [row['state'] for row in rows].count(HandoverState.DISPATCHED) == 1
    for row in rows[:-1]:
        handovers.receive(row['number'], '补打终端')
        order.append(row['number'])
    assert len({row['number'] for row in rows}) == 4
