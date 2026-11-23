from fastapi import APIRouter, Depends
from sqlalchemy import text

from src.config.settings import cache, engine
from src.controllers.workflow import download
from src.models.tables import audit, cases, evidence, runs
from src.schemas.workflow import CaseForm, LoginForm, ReceiptForm, ReviewForm
from src.services import domain, handovers, workflow
from src.services.security import current_user, login, operator, reviewer

router = APIRouter()


@router.get('/health')
def health():
    with engine.connect() as conn:
        conn.execute(text('SELECT 1')).scalar_one()
    cache.ping()
    return {'ok': True}


@router.post('/login')
def sign_in(form: LoginForm):
    return login(form.username, form.password)


@router.get('/spec')
def spec(_=Depends(current_user)):
    return domain.SPEC


@router.get('/cases')
def list_cases(_=Depends(current_user)):
    return workflow.recent(cases)


@router.post('/cases', status_code=201)
def create_case(form: CaseForm, actor=Depends(operator)):
    return workflow.make_case(form.title, form.payload, actor)


@router.get('/runs')
def list_runs(_=Depends(current_user)):
    return workflow.recent(runs)


@router.post('/cases/{case_id}/runs', status_code=202)
def start_run(case_id: int, actor=Depends(operator)):
    return workflow.enqueue(case_id, actor)


@router.get('/evidence')
def list_evidence(_=Depends(current_user)):
    return workflow.recent(evidence)


@router.get('/evidence/{identity}/download')
def export_evidence(identity: int, _=Depends(current_user)):
    return download(identity)


@router.post('/evidence/{identity}/review')
def review_evidence(identity: int, form: ReviewForm, actor=Depends(reviewer)):
    return workflow.review(identity, form.note, actor)


@router.get('/handovers')
def list_handovers(evidence_id: int | None = None, outlet: str | None = None,
                   state: str | None = None, _=Depends(current_user)):
    return handovers.listing(evidence_id=evidence_id, outlet=outlet, state=state)


@router.post('/evidence/{identity}/handovers')
def redispatch_evidence(identity: int, _=Depends(reviewer)):
    """补打系统重连或回执丢失后，重试队首批次。批次号保持不变。"""
    with engine.connect() as conn:
        workflow.found(conn, evidence, identity)
    return {'results': handovers.retry_evidence(identity)}


@router.get('/evidence/{identity}/handovers')
def evidence_handovers(identity: int, _=Depends(current_user)):
    """值班接口：按凭证查看各网点是否已收到逾时单。"""
    with engine.connect() as conn:
        row = workflow.found(conn, evidence, identity)
    return handovers.evidence_view(identity, row['report'])


@router.post('/handovers/{number}/receipt')
def receive_handover(number: str, form: ReceiptForm | None = None,
                     actor=Depends(current_user)):
    """补打系统回执。按批次号幂等，重复回执返回首次结果。"""
    received_by = (form.received_by if form and form.received_by else actor['sub'])
    batch = handovers.receive(number, received_by)
    return {'number': batch['number'], 'outlet': batch['outlet'], 'state': batch['state'],
            'received_at': batch['received_at'], 'received_by': batch['received_by'],
            'ticket_count': batch['ticket_count'], 'next': batch.get('next')}


@router.get('/audit')
def list_audit(_=Depends(current_user)):
    return workflow.recent(audit)
