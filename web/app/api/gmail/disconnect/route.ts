import { readJsonObject, rejectInProduction } from '@/lib/localDevelopmentProxy';

export const runtime = 'nodejs';

export async function POST(req: Request) {
  const productionRejection = rejectInProduction();
  if (productionRejection) {
    return productionRejection;
  }

  const body = await readJsonObject(req);
  if (body instanceof Response) {
    return body;
  }

  const userId = process.env.OPENPOKE_LOCAL_COMPOSIO_USER_ID || '';
  const connectionId = typeof body.connectionId === 'string' ? body.connectionId : '';
  const connectionRequestId =
    typeof body.connectionRequestId === 'string' ? body.connectionRequestId : '';

  const serverBase = process.env.PY_SERVER_URL || 'http://localhost:8001';
  const url = `${serverBase.replace(/\/$/, '')}/api/v1/gmail/disconnect`;
  const payload: any = {};
  if (userId) payload.user_id = userId;
  if (connectionId) payload.connection_id = connectionId;
  if (connectionRequestId) payload.connection_request_id = connectionRequestId;

  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await resp.json().catch(() => ({}));
    return new Response(JSON.stringify(data), {
      status: resp.status,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
  } catch (e: any) {
    return new Response(
      JSON.stringify({ ok: false, error: 'Upstream error', detail: e?.message || String(e) }),
      { status: 502, headers: { 'Content-Type': 'application/json; charset=utf-8' } }
    );
  }
}
