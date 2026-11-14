import { pathToFileURL } from 'node:url';
export const MARKETPLACE_URL = 'https://forestmarket-ireland.niall783.workers.dev/';
export const HEARTBEAT_URL = 'https://forestmarket-external-monitor.niall783.workers.dev/heartbeat';
const healthCodes = new Set(['auth_backup_fresh','auth_backup_missing','auth_backup_stale','auth_backup_artifact_missing','auth_backup_artifact_invalid','auth_backup_health_unconfirmed','auth_backup_health_unarmed','auth_backup_configuration_invalid','auth_backup_scope_mismatch']);
const codes = new Set(['external_recorded', 'external_duplicate', 'external_superseded']);
async function exchange(fetcher, url, options, limit) {
    const abort = new AbortController(); let timer, reader;
    const work = (async () => {
        const response = await fetcher(url, { ...options, signal: abort.signal, redirect: 'error', credentials: 'omit', cache: 'no-store' });
        const chunks = []; let size = 0;
        if (response.body) { reader = response.body.getReader(); for (;;) { const item = await reader.read(); if (item.done) break; size += item.value.length; if (size > limit) throw Error('bounded_body'); chunks.push(item.value); } }
        const bytes = new Uint8Array(size); let at = 0; for (const chunk of chunks) { bytes.set(chunk, at); at += chunk.length; }
        return { status: response.status, text: new TextDecoder('utf-8', { fatal: true }).decode(bytes) };
    })();
    try { return await Promise.race([work, new Promise((_, reject) => { timer = setTimeout(() => { abort.abort(); reject(Error('bounded_deadline')); }, 15000); })]); }
    finally { clearTimeout(timer); abort.abort(); if (reader) void reader.cancel().catch(() => {}); }
}
export async function runExternalAvailability(env, { clock = Date.now, fetcher = fetch } = {}) {
    const mode = env.GITHUB_REF === 'refs/heads/main' ? 'public' : null;
    const runId = Number(env.GITHUB_RUN_ID), attempt = Number(env.GITHUB_RUN_ATTEMPT);
    if (!mode || env.GITHUB_REPOSITORY !== 'niall783/forestmarket-uptime' || env.GITHUB_ACTOR !== 'niall783' || !/^[1-9][0-9]*$/.test(env.GITHUB_RUN_ID ?? '') || !Number.isSafeInteger(runId) || !/^[1-9][0-9]*$/.test(env.GITHUB_RUN_ATTEMPT ?? '') || !Number.isSafeInteger(attempt) || !/^[a-f0-9]{40}$/.test(env.GITHUB_SHA ?? '') || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(env.EXTERNAL_MONITOR_EPOCH ?? '') || !/^[A-Za-z0-9_-]{43}$/.test(env.EXTERNAL_MONITOR_TOKEN ?? '') || ![undefined, '0'].includes(env.TEST_FAILURE)) return { code: 'external_consumer_configuration_invalid', exitCode: 1 };
    let failed = false;
    try { const response = await exchange(fetcher, MARKETPLACE_URL, { method: 'GET', headers: { accept: 'text/html' } }, 65536); failed = response.status !== 200 || !response.text.includes('ForestMarket'); }
    catch { failed = true; }
    const observedAt = clock(); if (!Number.isSafeInteger(observedAt) || observedAt < 0) return { code: 'external_consumer_clock_invalid', exitCode: 1 };
    const payload = { version: 1, repository: env.GITHUB_REPOSITORY, actor: env.GITHUB_ACTOR, mode, epoch: env.EXTERNAL_MONITOR_EPOCH, runId, attempt, headSha: env.GITHUB_SHA, ref: env.GITHUB_REF, test: false, outcome: failed ? 'failed' : 'ok', observedAt };
    try {
        const response = await exchange(fetcher, HEARTBEAT_URL, { method: 'POST', headers: { authorization: `Bearer ${env.EXTERNAL_MONITOR_TOKEN}`, 'content-type': 'application/json' }, body: JSON.stringify(payload) }, 1024);
        const ack = JSON.parse(response.text);
        if (!ack || Object.keys(ack).sort().join(',') !== 'code,healthCode,outcome' || !healthCodes.has(ack.healthCode) || !['ok','failed'].includes(ack.outcome)) throw Error('unconfirmed');
        if (response.status === 503 && ack.code === 'external_health_changed' && ack.outcome === 'failed') return { code: 'external_backup_attention', healthCode: ack.healthCode, exitCode: 1 };
        if (response.status !== 200 || !codes.has(ack.code) || ack.outcome === 'ok' && ack.healthCode !== 'auth_backup_fresh') throw Error('unconfirmed');
        return { code: failed ? 'external_probe_failed' : ack.outcome === 'failed' ? 'external_backup_attention' : ack.code, healthCode: ack.healthCode, exitCode: failed || ack.outcome === 'failed' ? 1 : 0 };
    } catch { return { code: failed ? 'external_probe_and_heartbeat_failed' : 'external_heartbeat_unconfirmed', exitCode: 1 }; }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    let result;
    try { result = process.argv.length === 2 ? await runExternalAvailability(process.env) : { code: 'external_consumer_configuration_invalid', exitCode: 1 }; }
    catch { result = { code: 'external_consumer_failed', exitCode: 1 }; }
    if (result.exitCode) console.error('::error title=ForestMarket external check::Availability check failed.');
    else console.log('Availability check completed.');
    process.exitCode = result.exitCode;
}
