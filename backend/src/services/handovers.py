"""派发逾时单给网点补打系统。

签署复核时只为有逾时的网点建立「待交接」记录，发送与回执独立进行：

* 每个网点一条交接通道，签署生效顺序由通道行锁串行分配，批次号随记录一次生成。
* 同一网点只有通道队首可以发送；队首批次未收到回执时，后续批次保持待交接。
* 不同网点互不阻塞。发送失败（补打系统断线）不改变待交接状态，重试沿用原批次号。
* 回执按批次号幂等，重复回执返回首次结果，不会放行两次。
"""

import hashlib
import json
import os
import threading
import urllib.error
import urllib.request

from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from src.config.settings import engine
from src.constants.workflow import HANDOVER_LABELS, HandoverState
from src.models.tables import audit, handover_batches, handover_lanes
from src.utils.time import utc_time

TIMEOUT_SECTION = '逾时处理清单'
TENANT_SECTION = '网点容量'
OFFLINE_ENV = 'REPRINT_OFFLINE'
REPRINT_URL_ENV = 'REPRINT_URL'


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action, reference=str(reference),
                                       created_at=utc_time()))

_locks = {}
_locks_guard = threading.Lock()


def outlet_lock(outlet):
    with _locks_guard:
        return _locks.setdefault(outlet, threading.RLock())


def batch_number(evidence_id, outlet):
    token = hashlib.sha256(f'{evidence_id}:{outlet}'.encode('utf-8')).hexdigest()[:10].upper()
    return f'HO-{evidence_id:06d}-{token}'


def _timeout_rows(report):
    for section in report.get('sections', []):
        if section.get('title') == TIMEOUT_SECTION:
            return section.get('rows') or []
    return []


def _tenant_rows(report):
    for section in report.get('sections', []):
        if section.get('title') == TENANT_SECTION:
            return [row.get('网点') for row in section.get('rows') or [] if row.get('网点')]
    return []


def _group(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row['网点'], []).append(row)
    return grouped


def evidence_outlets(report):
    """该凭证需要交接的网点（按名称排序）。"""
    return sorted(_group(_timeout_rows(report)))


def _ensure_lanes(conn, outlets):
    existing = {row[0] for row in conn.execute(
        select(handover_lanes.c.outlet).where(handover_lanes.c.outlet.in_(outlets)))}
    for outlet in outlets:
        if outlet in existing:
            continue
        try:
            with conn.begin_nested():
                conn.execute(handover_lanes.insert().values(outlet=outlet, last_order=0))
        except IntegrityError:
            # 另一个事务已为该网点建立通道，直接复用。
            pass


def create_for_evidence(conn, evidence_row):
    """在签署事务内建立待交接记录；返回本次新建的批次。

    逐条按网点名称升序取得通道行锁，令同一网点的签署按生效顺序分配 `order_seq`：
    后一笔签署要等前一笔提交后才拿得到锁，因此无法抢先；升序取锁也避免互相等待。
    """
    grouped = _group(_timeout_rows(evidence_row['report']))
    if not grouped:
        return []
    outlets = sorted(grouped)
    _ensure_lanes(conn, outlets)
    lanes = {}
    for outlet in outlets:
        row = conn.execute(select(handover_lanes).where(
            handover_lanes.c.outlet == outlet).with_for_update()).mappings().first()
        lanes[outlet] = row['last_order']
    created = []
    for outlet in outlets:
        order_seq = lanes[outlet] + 1
        conn.execute(update(handover_lanes).where(handover_lanes.c.outlet == outlet)
                     .values(last_order=order_seq))
        tickets = grouped[outlet]
        number = batch_number(evidence_row['id'], outlet)
        identity = conn.execute(handover_batches.insert().values(
            number=number, evidence_id=evidence_row['id'], outlet=outlet, order_seq=order_seq,
            state=HandoverState.PENDING, tickets=tickets, ticket_count=len(tickets),
            created_at=utc_time())).inserted_primary_key[0]
        log(conn, evidence_row.get('reviewer') or 'system', 'handover_create', number)
        created.append({'id': identity, 'number': number, 'outlet': outlet,
                        'order_seq': order_seq, 'ticket_count': len(tickets)})
    return created


class _MultiLock:
    def __init__(self, outlets):
        self.outlets = sorted(set(outlets))

    def __enter__(self):
        self.locks = [outlet_lock(outlet) for outlet in self.outlets]
        for lock in self.locks:
            lock.acquire()
        return self

    def __exit__(self, *exc):
        for lock in reversed(self.locks):
            lock.release()
        return False


def lanes_lock(outlets):
    """进程内按网点串行化。锁先于事务取得，避免与投递线程锁序相反。

    跨进程的先后仍由 `handover_lanes` 的行锁保证。
    """
    return _MultiLock(outlets)


def reprint_offline():
    return os.getenv(OFFLINE_ENV, '').strip().lower() in {'1', 'true', 'yes', 'on'}


def deliver(batch):
    """把批次交给补打系统。

    配置 `REPRINT_URL` 时向该地址 POST 批次；未配置或设置 `REPRINT_OFFLINE` 时视为断线。
    断线只影响本次投递，交接记录已随签署落库，稍后可重试。
    """
    if reprint_offline():
        raise ConnectionError('补打系统连接失败')
    url = os.getenv(REPRINT_URL_ENV, '').strip()
    if not url:
        raise ConnectionError('未配置补打系统地址')
    body = json.dumps(_public(batch), ensure_ascii=False).encode('utf-8')
    request = urllib.request.Request(url, data=body, method='POST',
                                     headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            if response.status >= 300:
                raise ConnectionError(f'补打系统返回 {response.status}')
    except urllib.error.HTTPError as exc:
        raise ConnectionError(f'补打系统返回 {exc.code}') from None
    return True


def _head(conn, outlet):
    return conn.execute(select(handover_batches).where(
        handover_batches.c.outlet == outlet,
        handover_batches.c.state != HandoverState.RECEIVED,
    ).order_by(handover_batches.c.order_seq).limit(1)).mappings().first()


def dispatch_outlet(outlet, resend=False):
    """按通道顺序发送队首批次。返回结果描述，不抛出投递异常。

    队首未收到回执时只允许重发该批次（沿用原批次号），不会越过它发送后续批次。
    投递在事务外进行，补打系统挂起或断线不会占住通道行锁、也不会拖住签署。
    """
    try:
        with outlet_lock(outlet):
            # 短暂持锁读取队首，等签署事务提交后才看到新批次。
            with engine.begin() as conn:
                conn.execute(select(handover_lanes.c.outlet).where(
                    handover_lanes.c.outlet == outlet).with_for_update()).first()
                head = _head(conn, outlet)
                if head is None:
                    return {'outlet': outlet, 'sent': False, 'reason': 'empty'}
                if head['state'] == HandoverState.DISPATCHED and not resend:
                    return {'outlet': outlet, 'sent': False, 'reason': 'awaiting_receipt',
                            'number': head['number'], 'state': head['state']}
            try:
                deliver(dict(head))
            except Exception as exc:
                # 补打系统断线：保持原状态，重试时仍使用同一批次号。
                return {'outlet': outlet, 'sent': False, 'reason': 'offline',
                        'number': head['number'], 'state': head['state'], 'detail': str(exc)[:200]}
            with engine.begin() as conn:
                if head['state'] == HandoverState.PENDING:
                    count = conn.execute(handover_batches.update().where(
                        handover_batches.c.id == head['id'],
                        handover_batches.c.state == HandoverState.PENDING,
                    ).values(state=HandoverState.DISPATCHED, dispatched_at=utc_time())).rowcount
                    if not count:
                        # 投递期间另一派发端已更新或已收到回执，以库内状态为准。
                        current = conn.execute(select(handover_batches.c.state).where(
                            handover_batches.c.id == head['id'])).scalar_one()
                        return {'outlet': outlet, 'sent': False,
                                'reason': 'received' if current == HandoverState.RECEIVED
                                else 'awaiting_receipt',
                                'number': head['number'], 'state': current}
                    log(conn, 'dispatcher', 'handover_dispatch', head['number'])
                else:
                    log(conn, 'dispatcher', 'handover_resend', head['number'])
                    current = conn.execute(select(handover_batches.c.state).where(
                        handover_batches.c.id == head['id'])).scalar_one()
                    if current == HandoverState.RECEIVED:
                        return {'outlet': outlet, 'sent': False, 'reason': 'received',
                                'number': head['number'], 'state': current}
            return {'outlet': outlet, 'sent': True, 'number': head['number'],
                    'state': HandoverState.DISPATCHED}
    except Exception as exc:
        return {'outlet': outlet, 'sent': False, 'reason': 'error', 'detail': str(exc)[:200]}


def dispatch_all(outlets=None, resend=False):
    if outlets is None:
        with engine.connect() as conn:
            outlets = [row[0] for row in conn.execute(
                select(handover_batches.c.outlet).distinct().order_by(handover_batches.c.outlet))]
    return [dispatch_outlet(outlet, resend=resend) for outlet in sorted(outlets)]


def retry_outlet(outlet):
    """回执丢失或补打系统重连后重发队首批次，批次号不变。"""
    return dispatch_outlet(outlet, resend=True)


def retry_pending():
    """补打系统恢复后重发各网点仍待交接的队首批次。

    已派发、等待回执的批次不自动重发：回执按交接号幂等，需要时由值班重试。
    """
    if not os.getenv(REPRINT_URL_ENV, '').strip() or reprint_offline():
        return []
    with engine.connect() as conn:
        outlets = [row[0] for row in conn.execute(
            select(handover_batches.c.outlet).distinct().order_by(handover_batches.c.outlet))]
    results = []
    for outlet in outlets:
        with engine.connect() as conn:
            head = _head(conn, outlet)
        if head is not None and head['state'] == HandoverState.PENDING:
            results.append(dispatch_outlet(outlet))
    return results


def retry_evidence(evidence_id):
    """补打系统重连或回执丢失后重试该凭证的交接。

    队首属于本凭证时重发（交接号不变）；队首属于更早的签署时只报告阻塞的交接号，
    不会越过它发送。队首本身仍在待交接时按顺序补发队首。
    """
    results = []
    for outlet in pending_outlets(evidence_id):
        with engine.connect() as conn:
            head = _head(conn, outlet)
        if head is None:
            continue
        if head['state'] == HandoverState.PENDING:
            # 队首（含更早签署的批次）尚未发送，按顺序补发队首。
            results.append(dispatch_outlet(outlet))
        elif head['evidence_id'] == evidence_id:
            results.append(dispatch_outlet(outlet, resend=True))
        else:
            results.append({'outlet': outlet, 'sent': False, 'reason': 'awaiting_receipt',
                            'number': head['number'], 'state': head['state']})
    return results


def pending_outlets(evidence_id):
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(
            select(handover_batches.c.outlet).where(
                handover_batches.c.evidence_id == evidence_id,
                handover_batches.c.state != HandoverState.RECEIVED,
            ).distinct().order_by(handover_batches.c.outlet))]


def dispatch_evidence(evidence_id):
    """签署后尽力交接。任何失败都不影响已提交的签署结果。"""
    try:
        return dispatch_all(pending_outlets(evidence_id))
    except Exception:
        return []


def receive(number, actor):
    """补打系统回执。按批次号幂等：重复回执返回首次结果，不会重复放行。

    接收方按交接号去重，重复回执不改首次结果，重发同一批次也不会生成新交接号。
    回执本身证明批次已送达，即使本地仍是待交接（投递成功但落库失败）也按号接收；
    接收后放行同网点下一批，顺序仍由通道队首决定。
    """
    with engine.connect() as conn:
        known = conn.execute(select(handover_batches.c.outlet).where(
            handover_batches.c.number == number)).first()
    if known is None:
        raise HTTPException(404, '找不到该交接批次')
    outlet = known[0]
    with outlet_lock(outlet):
        with engine.begin() as conn:
            conn.execute(select(handover_lanes.c.outlet).where(
                handover_lanes.c.outlet == outlet).with_for_update()).first()
            batch = conn.execute(select(handover_batches).where(
                handover_batches.c.number == number).with_for_update()).mappings().first()
            if batch is None:
                raise HTTPException(404, '找不到该交接批次')
            first = batch['state'] != HandoverState.RECEIVED
            if first:
                conn.execute(handover_batches.update().where(
                    handover_batches.c.id == batch['id'],
                    handover_batches.c.state != HandoverState.RECEIVED,
                ).values(state=HandoverState.RECEIVED, received_at=utc_time(), received_by=actor))
                log(conn, actor, 'handover_receive', number)
                batch = conn.execute(select(handover_batches).where(
                    handover_batches.c.id == batch['id'])).mappings().first()
            result = dict(batch)
            result['duplicate'] = not first
        # 回执提交后才放行下一批，避免持锁投递。
        result['next'] = dispatch_outlet(outlet)
        return result


def listing(evidence_id=None, outlet=None, state=None):
    query = select(handover_batches).order_by(
        handover_batches.c.evidence_id, handover_batches.c.outlet, handover_batches.c.order_seq)
    if evidence_id is not None:
        query = query.where(handover_batches.c.evidence_id == evidence_id)
    if outlet:
        query = query.where(handover_batches.c.outlet == outlet)
    if state:
        query = query.where(handover_batches.c.state == state)
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(query).mappings()]


def _public(batch):
    return {
        'number': batch['number'],
        'outlet': batch['outlet'],
        'order_seq': batch['order_seq'],
        'state': batch['state'],
        'state_label': HANDOVER_LABELS.get(HandoverState(batch['state']), batch['state']),
        'ticket_count': batch['ticket_count'],
        'tickets': batch['tickets'],
        'created_at': batch['created_at'],
        'dispatched_at': batch['dispatched_at'],
        'received_at': batch['received_at'],
        'received_by': batch['received_by'],
    }


def evidence_view(identity, report):
    """值班查询：按凭证列出各网点交接与回执状态。"""
    batches = listing(evidence_id=identity)
    grouped = {}
    for batch in batches:
        grouped.setdefault(batch['outlet'], []).append(batch)
    order = []
    for outlet in _tenant_rows(report) + list(grouped):
        if outlet not in order:
            order.append(outlet)
    outlets = []
    for outlet in order:
        outlet_batches = grouped.get(outlet, [])
        if outlet_batches:
            received = all(batch['state'] == HandoverState.RECEIVED for batch in outlet_batches)
            waiting = next((batch for batch in outlet_batches
                            if batch['state'] != HandoverState.RECEIVED), None)
            state = (HandoverState.RECEIVED if received
                     else HandoverState.DISPATCHED if any(
                         batch['state'] == HandoverState.DISPATCHED for batch in outlet_batches)
                     else HandoverState.PENDING)
            outlets.append({
                'outlet': outlet,
                'received': received,
                'state': state,
                'state_label': HANDOVER_LABELS[state],
                'awaiting_receipt': None if received or waiting is None else waiting['number'],
                'ticket_count': sum(batch['ticket_count'] for batch in outlet_batches),
                'batches': [_public(batch) for batch in outlet_batches],
            })
        else:
            outlets.append({'outlet': outlet, 'received': None, 'state': 'none',
                            'state_label': '无逾时', 'awaiting_receipt': None,
                            'ticket_count': 0, 'batches': []})
    return {
        'evidence_id': identity,
        'outlets': outlets,
        'received_all': all(item['received'] is not False for item in outlets),
        'batches': [_public(batch) for batch in batches],
    }
