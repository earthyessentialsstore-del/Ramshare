// RamShare relay: anyone can chat, anyone can lend their laptop. One shared credit balance per browser.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000, DB = path.join(__dirname, 'credits.json');
const START_CREDITS = 1000, MAX_QUEUE = 50;

let accounts = {}; try { accounts = JSON.parse(fs.readFileSync(DB)); } catch {}
let dirty = false; setInterval(() => { if (dirty) { fs.writeFile(DB, JSON.stringify(accounts), () => {}); dirty = false; } }, 5000);
const bal = cid => (cid in accounts ? accounts[cid] : (accounts[cid] = START_CREDITS));
const add = (cid, n) => { accounts[cid] = Math.max(0, bal(cid) + n); dirty = true; };

const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (e, d) => {
    if (e) { res.writeHead(500); return res.end('missing index.html'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(d);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const providers = () => [...wss.clients].filter(c => c.provider);
const jobs = new Map(), queue = [];
let seq = 1;

function broadcast() {
  const p = providers(), s = { type: 'stats', providers: p.length, busy: p.filter(x => x.busy).length, queued: queue.length };
  wss.clients.forEach(c => send(c, s));
}
function dispatch() {
  while (queue.length) {
    const q = queue[0], p = providers().find(p => !p.busy && p !== q.ws);
    if (!p) break;
    queue.shift(); if (q.ws.readyState !== 1) continue;
    const jobId = 'j' + seq++; p.busy = true;
    jobs.set(jobId, { consumer: q.ws, provider: p, id: q.id });
    send(p, { type: 'job', jobId, messages: q.messages });
  }
  broadcast();
}
function endJob(jobId) { const j = jobs.get(jobId); if (j) { j.provider.busy = false; jobs.delete(jobId); } }

wss.on('connection', ws => {
  ws.cid = null; ws.provider = false; ws.busy = false; ws.pending = false;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'hello' && /^[a-z0-9-]{8,64}$/i.test(m.cid || '')) {
      ws.cid = m.cid; send(ws, { type: 'ready', credits: bal(ws.cid) }); return broadcast();
    }
    if (!ws.cid) return;

    if (m.type === 'provider_on') { ws.provider = true; dispatch(); return; }
    if (m.type === 'provider_off') { ws.provider = false; if (!ws.busy) broadcast(); return; }

    if (m.type === 'chat') {
      const msgs = (Array.isArray(m.messages) ? m.messages : []).slice(-20)
        .map(x => ({ role: x.role === 'assistant' ? 'assistant' : 'user', content: String(x.content || '').slice(0, 4000) }));
      if (!msgs.length) return;
      if (bal(ws.cid) <= 0) return send(ws, { type: 'error', id: m.id, message: 'You are out of credits. Share your laptop to earn more.' });
      if (ws.pending) return send(ws, { type: 'error', id: m.id, message: 'Wait for the current answer to finish.' });
      if (!providers().length) return send(ws, { type: 'error', id: m.id, message: 'Nobody is sharing a laptop right now. Share yours to start the network.' });
      if (queue.length >= MAX_QUEUE) return send(ws, { type: 'error', id: m.id, message: 'The network is busy. Try again shortly.' });
      ws.pending = true; queue.push({ ws, id: m.id, messages: msgs }); return dispatch();
    }

    if (ws.provider && m.jobId) {
      const j = jobs.get(m.jobId); if (!j || j.provider !== ws) return;
      if (m.type === 'chunk') send(j.consumer, { type: 'chunk', id: j.id, text: String(m.text || '') });
      if (m.type === 'done') {
        const t = Math.min(Math.max(1, m.tokens | 0), 2000);
        add(j.consumer.cid, -t); add(ws.cid, t);
        send(j.consumer, { type: 'done', id: j.id, credits: bal(j.consumer.cid) });
        send(ws, { type: 'credits', credits: bal(ws.cid) });
        j.consumer.pending = false; endJob(m.jobId); dispatch();
      }
      if (m.type === 'error') {
        send(j.consumer, { type: 'error', id: j.id, message: 'That laptop failed. Please ask again.' });
        j.consumer.pending = false; endJob(m.jobId); dispatch();
      }
    }
  });
  ws.on('close', () => {
    ws.provider = false;
    for (let i = queue.length - 1; i >= 0; i--) if (queue[i].ws === ws) queue.splice(i, 1);
    for (const [k, j] of jobs) if (j.provider === ws || j.consumer === ws) {
      if (j.provider === ws) { send(j.consumer, { type: 'error', id: j.id, message: 'The laptop went offline mid-answer. Please ask again.' }); j.consumer.pending = false; }
      endJob(k);
    }
    dispatch();
  });
});
server.listen(PORT, () => console.log('RamShare running on port ' + PORT));
