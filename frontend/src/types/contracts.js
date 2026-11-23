/** @typedef {{id:number, title:string, payload:object, author:string}} BusinessCase */
/** @typedef {{id:number, case_id:number, status:'queued'|'running'|'complete'|'failed'}} Run */
/** @typedef {{id:number, run_id:number, digest:string, report:{summary:string, metrics:object, sections:Array}}} Evidence */
/** @typedef {{number:string, outlet:string, order_seq:number, state:'pending'|'dispatched'|'received', ticket_count:number, tickets:Array, received_at:string|null, received_by:string|null}} HandoverBatch */
/** @typedef {{evidence_id:number, outlets:Array<{outlet:string, received:boolean|null, state:string, awaiting_receipt:string|null, batches:HandoverBatch[]}>, received_all:boolean}} HandoverView */
