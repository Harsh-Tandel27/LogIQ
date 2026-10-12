"use client";

import { Fragment, useState } from "react";
import { buttonPrimary, ErrorNote, field, PageHeader, Panel, SeverityBadge, StatusBadge } from "@/components/bits";
import { IncidentReplay } from "@/components/IncidentReplay";
import { clock, fmt, postJson, timeAgo, usePoll, type IncidentDetail } from "@/lib/api";

function Chain({ chain, root }: { chain: string[]; root: string }) {
  return (
    <ol className="flex flex-wrap items-center gap-x-1 gap-y-1 text-[13px]">
      {chain.map((service, i) => (
        <Fragment key={service}>
          {i > 0 && (
            <li aria-hidden className="text-ink-3">
              <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M4.5 2.5 8 6l-3.5 3.5" stroke="currentColor" strokeWidth="1.4" /></svg>
            </li>
          )}
          <li className={service === root ? "font-medium text-crit" : "text-ink-2"}>{service}</li>
        </Fragment>
      ))}
    </ol>
  );
}

function ResolveForm({ id, onDone }: { id: string; onDone: () => void }) {
  const [fix, setFix] = useState("");
  const [state, setState] = useState<"idle" | "saving" | "failed">("idle");

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setState("saving");
    try {
      await postJson(`/api/incidents/${id}/resolve`, { fix });
      setState("idle");
      onDone();
    } catch {
      setState("failed");
    }
  }

  return (
    <form onSubmit={submit} className="space-y-2">
      <label htmlFor="fix" className="block text-[13px] text-ink-2">
        What fixed it? <span className="text-ink-3">(optional, saved as a runbook)</span>
      </label>
      <textarea id="fix" value={fix} onChange={(e) => setFix(e.target.value)} rows={3} className={field} />
      <button type="submit" disabled={state === "saving"} className={buttonPrimary}>
        {state === "saving" ? "Resolving…" : "Resolve incident"}
      </button>
      {state === "failed" && <p className="text-[13px] text-crit">Couldn’t resolve. The incident may already be closed.</p>}
    </form>
  );
}

function Legend() {
  const swatch = "inline-block h-2.5 w-2.5 rounded-sm";
  return (
    <ul className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-ink-2">
      <li className="flex items-center gap-1.5"><span className={`${swatch} bg-crit`} />Root cause</li>
      <li className="flex items-center gap-1.5"><span className={`${swatch} border-2 border-warn bg-warn-wash`} />Logging errors</li>
      <li className="flex items-center gap-1.5"><span className={`${swatch} border-2 border-crit bg-warn-wash`} />Traced</li>
      <li className="flex items-center gap-1.5"><span className={`${swatch} border border-rule-strong bg-surface`} />Healthy</li>
      <li className="text-ink-3">Arrows point to the dependency</li>
    </ul>
  );
}

export function IncidentView({ id }: { id: string }) {
  const { data, error, reload } = usePoll<IncidentDetail>(`/api/incidents/${id}`, 3000);
  const crumbs = [{ href: "/dashboard", label: "Overview" }, { href: "/dashboard#incidents", label: "Incidents" }, { label: id }];

  if (!data) {
    return (
      <>
        <PageHeader crumbs={crumbs} title={id} />
        <div className="p-4 sm:p-6">
          <ErrorNote error={error} />
          {!error && <p className="text-ink-3">Loading…</p>}
        </div>
      </>
    );
  }

  const { incident, alerts, graph } = data;
  const open = incident.status === "open";
  const stats = [
    { label: "Error logs", value: fmt.format(incident.total_logs), sub: `${fmt.format(incident.total_logs - incident.alert_ids.length)} duplicates suppressed` },
    { label: "Alerts", value: String(incident.alert_ids.length), sub: "after grouping" },
    { label: "Services affected", value: String(incident.affected_services.length), sub: `of ${graph.nodes.length}` },
    { label: "Slack", value: incident.slack_sent ? "1 message" : "Not sent", sub: incident.slack_sent ? "sent once" : "webhook not set" },
  ];

  return (
    <>
      <PageHeader
        crumbs={crumbs}
        title={`${incident.root_cause} failure`}
        meta={
          <>
            <SeverityBadge severity={incident.severity} />
            <StatusBadge status={incident.status} />
            <span className="text-xs text-ink-3">opened {timeAgo(incident.created_at)}</span>
          </>
        }
      />

      <div className="grid gap-4 p-4 sm:p-6 xl:grid-cols-[minmax(0,1fr)_340px]">
        <div className="min-w-0 space-y-4">
          <ErrorNote error={error} />
          <div className="grid grid-cols-2 gap-px overflow-hidden rounded-md border border-rule bg-rule sm:grid-cols-4">
            {stats.map((s) => (
              <div key={s.label} className="bg-surface px-4 py-3">
                <p className="text-xs text-ink-3">{s.label}</p>
                <p className="mt-1 text-xl font-semibold tabular-nums">{s.value}</p>
                <p className="text-xs text-ink-3">{s.sub}</p>
              </div>
            ))}
          </div>

          <Panel title="Dependency trace" meta={<Legend />}>
            <IncidentReplay incident={incident} alerts={alerts} graph={graph} />
          </Panel>

          <Panel title="Grouped alerts" meta={`${alerts.length} alerts from ${incident.affected_services.length} services`} flush>
            <div className="overflow-x-auto">
              <table className="data-table min-w-[720px] text-[13px]">
                <thead>
                  <tr>
                    <th>Service</th>
                    <th>Severity</th>
                    <th className="num">Logs</th>
                    <th>Templates merged into this alert</th>
                  </tr>
                </thead>
                <tbody>
                  {alerts.map((a) => (
                    <tr key={a.id}>
                      <td className={`whitespace-nowrap font-medium ${a.service === incident.root_cause ? "text-crit" : ""}`}>{a.service}</td>
                      <td className="whitespace-nowrap">
                        <SeverityBadge severity={a.severity} />
                        <span className="ml-2 text-xs text-ink-3" title="Severity model confidence">{Math.round(a.severity_confidence * 100)}%</span>
                      </td>
                      <td className="num">{fmt.format(a.count)}</td>
                      <td className="font-mono text-xs text-ink-2">
                        {a.variants.map((v) => <div key={v}>{v}</div>)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>
        </div>

        <aside className="space-y-4">
          <Panel title="Details">
            <dl className="space-y-2.5 text-[13px]">
              <div>
                <dt className="text-xs text-ink-3">Root cause</dt>
                <dd className="font-medium text-crit">{incident.root_cause}</dd>
              </div>
              <div>
                <dt className="text-xs text-ink-3">Failure path</dt>
                <dd><Chain chain={incident.chain} root={incident.root_cause} /></dd>
              </div>
              <div>
                <dt className="text-xs text-ink-3">Opened</dt>
                <dd>{new Date(incident.created_at).toLocaleString()}</dd>
              </div>
              {!open && incident.resolved_at && (
                <div>
                  <dt className="text-xs text-ink-3">Resolved</dt>
                  <dd>{new Date(incident.resolved_at).toLocaleString()}</dd>
                  {incident.resolution && <dd className="mt-1 text-ink-2">{incident.resolution}</dd>}
                </div>
              )}
            </dl>
          </Panel>

          <Panel title="Suggested fixes" meta="from past incidents">
            {incident.recommendations && incident.recommendations.length > 0 ? (
              <ol className="space-y-3">
                {incident.recommendations.map((r, i) => (
                  <li key={r.id} className={i > 0 ? "border-t border-rule pt-3" : ""}>
                    <div className="flex items-baseline justify-between gap-2">
                      <p className="text-[13px] font-medium">{r.title}</p>
                      <span className="shrink-0 font-mono text-[11px] text-ink-3" title="Cosine similarity to this incident">{r.score.toFixed(2)}</span>
                    </div>
                    <p className="mt-1 text-[13px] leading-relaxed text-ink-2">{r.fix}</p>
                    <p className="mt-1 font-mono text-[11px] text-ink-3">{r.id}</p>
                  </li>
                ))}
              </ol>
            ) : (
              <p className="text-[13px] text-ink-3">No matching runbook. Add the fix when you resolve this incident.</p>
            )}
          </Panel>

          {open && (
            <Panel title="Resolve">
              <ResolveForm id={incident.id} onDone={reload} />
            </Panel>
          )}

          <Panel title="Timeline">
            <ol className="space-y-1.5 text-[13px]">
              {incident.timeline?.map((t, i) => (
                <li key={i} className="grid grid-cols-[4.5rem_1fr] gap-2">
                  <span className="font-mono text-xs text-ink-3">{clock(t.at)}</span>
                  <span>{t.event}</span>
                </li>
              ))}
            </ol>
          </Panel>
        </aside>
      </div>
    </>
  );
}
