import { rejectInProduction } from '@/lib/localDevelopmentProxy';

export const runtime = 'nodejs';

type UIMsgPart = { type: string; text?: string };
type UIMessage = { role: string; parts?: UIMsgPart[]; content?: string };

function uiToOpenAIContent(messages: UIMessage[]): { role: string; content: string }[] {
  const out: { role: string; content: string }[] = [];
  for (const m of messages || []) {
    const role = m?.role;
    if (!role) continue;
    let content = '';
    if (Array.isArray(m.parts)) {
      content = m.parts.filter((p) => p?.type === 'text').map((p) => p.text || '').join('');
    } else if (typeof m.content === 'string') {
      content = m.content;
    }
    out.push({ role, content });
  }
  return out;
}

export async function POST(req: Request) {
  const productionRejection = rejectInProduction();
  if (productionRejection) {
    return productionRejection;
  }

  let body: any;
  try {
    body = await req.json();
  } catch (e) {
    console.error('[chat-proxy] invalid json', e);
    return new Response('Invalid JSON', { status: 400 });
  }

  const { messages, turn_id: turnId } = body || {};
  if (!Array.isArray(messages) || messages.length === 0) {
    return new Response('Missing messages', { status: 400 });
  }
  if (typeof turnId !== 'string' || turnId.length === 0) {
    return new Response('Missing turn id', { status: 400 });
  }

  const serverBase = process.env.PY_SERVER_URL || 'http://localhost:8001';
  const serverPath = process.env.PY_CHAT_PATH || '/api/v1/chat/send';
  const bearerToken = process.env.OPENPOKE_WEB_BEARER_TOKEN;
  if (!bearerToken) {
    return new Response('Chat authentication is not configured', { status: 503 });
  }
  const url = `${serverBase.replace(/\/$/, '')}${serverPath}`;

  const payload = {
    system: '',
    messages: uiToOpenAIContent(messages),
    stream: false,
    turn_id: turnId,
  };

  try {
    const upstream = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'text/plain, */*',
        Authorization: `Bearer ${bearerToken}`,
      },
      body: JSON.stringify(payload),
    });
    const text = await upstream.text();
    return new Response(text, {
      status: upstream.status,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    });
  } catch (e: any) {
    console.error('[chat-proxy] upstream error', e);
    return new Response(e?.message || 'Upstream error', { status: 502 });
  }
}
