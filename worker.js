// halpurple-hub: SSE(サーバー送信イベント)の「配達所」。
// サイト本体(Pages)から Durable Object として呼ばれます。直接URLでは何も返しません。
import { DurableObject } from 'cloudflare:workers';

const enc = new TextEncoder();
const MAX_PER_USER = 4; // 1人あたりの同時接続の上限(古いものから閉じる)

export class Hub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.c = new Map(); // uid -> Set<{w}>
    this.hb = 0;
  }

  drop(uid, cl) {
    const set = this.c.get(uid);
    if (!set) return;
    set.delete(cl);
    try { cl.w.close().catch(() => {}); } catch {}
    if (!set.size) this.c.delete(uid);
    if (!this.c.size && this.hb) { clearInterval(this.hb); this.hb = 0; }
  }

  send(uid, cl, text) {
    cl.w.write(enc.encode(text)).catch(() => this.drop(uid, cl));
  }

  beat() {
    if (this.hb) return;
    // 20秒ごとに「生きてる」合図を送る(途中で切れるのを防ぎ、切れた接続を掃除する)
    this.hb = setInterval(() => {
      for (const [uid, set] of [...this.c]) for (const cl of [...set]) this.send(uid, cl, ': ping\n\n');
    }, 20000);
  }

  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === '/sub') {
      const uid = +u.searchParams.get('uid') || 0;
      if (!uid) return new Response('bad', { status: 400 });
      const ts = new TransformStream();
      const cl = { w: ts.writable.getWriter() };
      let set = this.c.get(uid);
      if (!set) this.c.set(uid, (set = new Set()));
      while (set.size >= MAX_PER_USER) this.drop(uid, set.values().next().value);
      set.add(cl);
      this.send(uid, cl, 'retry: 5000\n\ndata: {"t":"hi"}\n\n');
      req.signal.addEventListener('abort', () => this.drop(uid, cl));
      this.beat();
      return new Response(ts.readable, {
        headers: {
          'content-type': 'text/event-stream; charset=utf-8',
          'cache-control': 'no-store, no-transform',
          'x-accel-buffering': 'no',
        },
      });
    }
    if (u.pathname === '/pub' && req.method === 'POST') {
      let b;
      try { b = await req.json(); } catch { return new Response('bad', { status: 400 }); }
      const text = 'data: ' + JSON.stringify(b.ev || {}) + '\n\n';
      let n = 0;
      for (const uid of (b.to || []).slice(0, 200)) {
        const set = this.c.get(+uid);
        if (!set) continue;
        for (const cl of [...set]) { this.send(+uid, cl, text); n++; }
      }
      return new Response(String(n));
    }
    return new Response('not found', { status: 404 });
  }
}

// 外から直接アクセスされても何も返さない(Pages からの Durable Object 経由だけが入口)
export default { fetch() { return new Response('', { status: 404 }); } };
