import {heading, panel} from '../components/common/heading.js';
import {entries, records} from '../components/common/records.js';
import {handoverLabels} from '../constants/workflow.js';
import {topic} from '../constants/project.js';
import {date, el, esc} from '../utils/dom.js';
import {submit} from '../hooks/submit.js';

const stateLabel = (row)=>handoverLabels[row.state] || row.state;

export function handoverPanel(view, role) {
  const rows = view.outlets.filter(row=>row.batches.length);
  if (!rows.length) return panel('交接与补打回执', '<p class="empty">本次凭证没有逾时单，无需交接。</p>');
  const body = rows.flatMap(outlet=>outlet.batches.map(row=>[
    esc(outlet.outlet), `<code>${esc(row.number)}</code>`, esc(stateLabel(row)),
    esc(row.ticket_count), esc(row.dispatched_at ? date(row.dispatched_at) : '—'),
    esc(row.received_at ? `${row.received_by ? row.received_by + ' · ' : ''}${date(row.received_at)}` : '—')]));
  const waiting = rows.filter(row=>!row.received).length;
  const note = view.received_all
    ? '<p class="note">全部网点已回执。</p>'
    : `<p class="note">${waiting} 个网点尚未回执，同一网点的后续批次会在回执后放行。</p>`;
  const retry = role==='reviewer' && !view.received_all
    ? '<button class="secondary" type="button" id="retry-handover">重试队首派发</button>' : '';
  return panel('交接与补打回执',
    `${records(['网点','交接号','状态','逾时单','派发时间','回执'], body)}${note}<div class="actions">${retry}</div>`);
}

export function evidencePage(state, identity) {
  const current = state.evidence.find(row=>row.id===identity) || state.evidence[0];
  if (!current) return heading(topic.modules[3]) + '<p class="empty">执行完成后，可查看、复核和导出证据。</p>';
  const {report} = current;
  const metrics = Object.entries(report.metrics).map(([label,value])=>`<div><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`).join('');
  const sections = report.sections.map(x=>panel(x.title, entries(x.rows))).join('');
  const review = current.reviewer ? `<p data-testid="reviewed">${esc(current.reviewer)} · ${esc(date(current.reviewed_at))}</p><p>${esc(current.review_note)}</p>`
    : state.session.role==='reviewer'?'<form id="review-form"><label>复核意见<textarea name="note" maxlength="500" required></textarea></label><button>签署复核</button></form>'
    : '<p class="note">待复核员签署</p>';
  const history = state.evidence.map(row=>[`<a href="#evidence/${row.id}">执行 ${row.run_id}</a>`, esc(row.report.summary), esc(row.reviewer || '待复核')]);
  return `${heading(topic.modules[3], report.summary, `<button id="download" data-id="${current.id}">导出证据</button>`)}
    <div class="spread evidence-top"><dl class="metrics">${metrics}</dl><aside class="brief"><h2>复核签署</h2>${review}<details><summary>结果校验码</summary><code class="digest">${esc(current.digest)}</code></details></aside></div>
    <div id="handover-slot"></div>
    ${sections}<details><summary>历史证据</summary>${records(['执行','结论','复核'], history)}</details>`;
}

export function bindEvidence(state, api, update, fail) {
  const button = document.getElementById('download');
  if (!button) return;
  const identity = button.dataset.id;
  const current = state.evidence.find(row=>row.id===Number(identity));
  const slot = document.getElementById('handover-slot');
  const loadHandovers = async()=>{
    if (!slot) return;
    try {
      slot.innerHTML = handoverPanel(await api(`/evidence/${identity}/handovers`), state.session.role);
      document.getElementById('retry-handover')?.addEventListener('click', async event=>{
        event.currentTarget.disabled = true;
        try {
          await api(`/evidence/${identity}/handovers`, {method:'POST'});
          await loadHandovers();
        } catch (error) { fail(error); event.currentTarget.disabled = false; }
      });
    } catch (error) { fail(error); }
  };
  button.addEventListener('click', async()=>{
    try {
      const response = await api(`/evidence/${identity}/download`, {response:true});
      const url = URL.createObjectURL(await response.blob());
      const link = el('a');
      link.href = url;
      link.download = `evidence-${identity}.json`;
      link.click();
      setTimeout(()=>URL.revokeObjectURL(url), 1000);
    } catch (error) { fail(error); }
  });
  if (current?.reviewer) void loadHandovers();
  submit('review-form', async data=>{
    await api(`/evidence/${identity}/review`, {method:'POST', data:{note:data.get('note')}});
    await update();
  }, fail);
}
