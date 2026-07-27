export function rejectInProduction(): Response | null {
  if (process.env.NODE_ENV !== 'production') {
    return null;
  }

  return Response.json(
    { error: 'Local development authentication is unavailable in production' },
    { status: 503 },
  );
}

export async function readJsonObject(
  request: Request,
): Promise<Record<string, unknown> | Response> {
  const contentType = request.headers.get('content-type')?.toLowerCase() ?? '';
  if (!contentType.startsWith('application/json')) {
    return Response.json(
      { ok: false, error: 'Content-Type must be application/json' },
      { status: 415 },
    );
  }

  try {
    const body: unknown = await request.json();
    if (body === null || typeof body !== 'object' || Array.isArray(body)) {
      throw new TypeError('JSON body must be an object');
    }
    return body as Record<string, unknown>;
  } catch {
    return Response.json(
      { ok: false, error: 'Request body must be a valid JSON object' },
      { status: 400 },
    );
  }
}
