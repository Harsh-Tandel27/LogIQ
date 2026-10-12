"use client";

import Link from "next/link";
import { useState } from "react";
import { Empty, ErrorNote, PageHeader, Panel, SeverityBadge, StatusBadge } from "@/components/bits";
import { HistoryCharts } from "@/components/HistoryCharts";
import { ThroughputChart } from "@/components/ThroughputChart";
import { fmt, timeAgo, usePoll, type Alert, type History, type Overview } from "@/lib/api";

// Matches the correlator's burst threshold (api/correlator.py MIN_LOGS).
const BURST_LOGS = 25;

function OpenIncidentBanner({ overview }: { overview: Overview }) {
  const open = overview.incidents.filter((i) => i.status === "open");
  if (open.length === 0) return null;
  return (
    <div className="space-y-2">
      {open.map((i) => (
        <div key={i.id} className="flex flex-wrap items-center gap-x-4 gap-y-1 rounded-md border border-crit/30 border-l-4 border-l-crit bg-surface px-3 py-2">
          <StatusBadge status="open" />
          <span className="font-mono text-xs text-ink-3">{i.id}</span>
          <span className="font-medium">{i.root_cause} failure</span>
          <SeverityBadge severity={i.severity} />
          <span className="text-[13px] text-ink-2">
            {fmt.format(i.total_logs)} logs, {i.affected_services.length} {i.affected_services.length === 1 ? "service" : "services"}, opened {timeAgo(i.created_at)}
          </span>
          <Link href={`/incident/${i.id}`} className="ml-auto text-[13px] font-medium text-brand hover:underline">
            View incident
          </Link>
        </div>
      ))}
    </div>
  );
}

function Stats({ totals }: { totals: Overview["totals"] }) {
  const dropPct = totals.received ? Math.round((totals.bloom_dropped / totals.received) * 100) : 0;
  const items = [
    { label: "Logs ingested", value: fmt.format(totals.received), sub: "from Kafka" },
    { label: "Dropped by Bloom filter", value: fmt.format(totals.bloom_dropped), sub: `${dropPct}% healthy` },
    { label: "Error logs", value: fmt.format(totals.errors), sub: "kept for grouping" },
    { label: "Alerts", value: fmt.format(totals.alerts), sub: totals.logs_per_alert ? `1 per ${fmt.format(totals.logs_per_alert)} errors` : "none yet" },
    { label: "Incidents", value: fmt.format(totals.incidents), sub: `${totals.open_incidents} open` },
  ];
  return (
    <div className="grid grid-cols-2 gap-px overflow-hidden rounded-md border border-rule bg-rule sm:grid-cols-3 lg:grid-cols-5 [&>*:last-child]:col-span-2 lg:[&>*:last-child]:col-span-1">
      {items.map((s) => (
        <div key={s.label} className="bg-surface px-4 py-3">
          <p className="text-xs text-ink-3">{s.label}</p>
          <p className="mt-1 text-2xl font-semibold tabular-nums">{s.value}</p>
          <p className="text-xs text-ink-3">{s.sub}</p>
        </div>
      ))}
    </div>
  );
}

function ServiceLoad({ alerts }: { alerts: Alert[] }) {
  const byService = new Map<string, number>();
  for (const a of alerts) byService.set(a.service, (byService.get(a.service) ?? 0) + a.count);
  const rows = [...byService.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
  const max = Math.max(1, ...rows.map(([, n]) => n));
  if (rows.length === 0) return <Empty>No open alerts.</Empty>;
  return (
    <ul className="space-y-2">
      {rows.map(([service, n]) => (
        <li key={service} className="text-[13px]">
          <div className="flex justify-between">
            <span>{service}</span>
            <span className="tabular-nums text-ink-2">{fmt.format(n)}</span>
          </div>
          <div className="mt-1 h-1.5 rounded-sm bg-sunken">
            <div className="h-full rounded-sm bg-[var(--series-2)]" style={{ width: `${(n / max) * 100}%` }} />
          </div>
        </li>
      ))}
    </ul>
  );
}

function AlertRows({ alerts }: { alerts: Alert[] }) {
  if (alerts.length === 0) return <Empty>Nothing here.</Empty>;
  return (
    <div className="overflow-x-auto">
      <table className="data-table min-w-[760px] text-[13px]">
        <thead>
          <tr>
            <th>Service</th>
            <th>Severity</th>
            <th className="num">Logs</th>
            <th>Sample message</th>
            <th>Last seen</th>
            <th>Incident</th>
          </tr>
        </thead>
        <tbody>
          {alerts.map((a) => (
            <tr key={a.id}>
              <td className="whitespace-nowrap font-medium">{a.service}</td>
              <td><SeverityBadge severity={a.severity} /></td>
              <td className="num">{fmt.format(a.count)}</td>
              <td className="max-w-[520px] truncate font-mono text-xs text-ink-2" title={a.message}>{a.message}</td>
              <td className="whitespace-nowrap text-ink-3">{timeAgo(a.last_seen)}</td>
              <td className="whitespace-nowrap">
                {a.incident_id ? (
                  <Link href={`/incident/${a.incident_id}`} className="font-mono text-xs text-brand hover:underline">{a.incident_id}</Link>
                ) : (
                  <span className="text-ink-3">–</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

type AlertTab = "all" | "bursts" | "sporadic";

function AlertsPanel({ alerts }: { alerts: Alert[] }) {
  // alerts arrive newest first
  const bursts = alerts.filter((a) => a.incident_id || a.count >= BURST_LOGS).sort((a, b) => b.count - a.count);
  const sporadic = alerts.filter((a) => !a.incident_id && a.count < BURST_LOGS);
  const [tab, setTab] = useState<AlertTab>("all");
  const tabs: { key: AlertTab; label: string; rows: Alert[]; meta: string }[] = [
    { key: "all", label: "All", rows: alerts, meta: "newest first" },
    { key: "bursts", label: "Bursts", rows: bursts, meta: "25+ logs, can page" },
    { key: "sporadic", label: "Sporadic", rows: sporadic, meta: "under 25 logs, never paged" },
  ];
  const current = tabs.find((t) => t.key === tab)!;
  return (
    <Panel
      id="alerts"
      title={
        <span className="flex items-center gap-4">
          <span>Open alerts</span>
          <span role="tablist" className="flex gap-1">
            {tabs.map((t) => (
              <button
                key={t.key}
                role="tab"
                aria-selected={tab === t.key}
                onClick={() => setTab(t.key)}
                className={`rounded px-2 py-0.5 text-xs ${tab === t.key ? "bg-brand-wash font-medium text-brand" : "font-normal text-ink-2 hover:bg-sunken"}`}
              >
                {t.label} {t.rows.length}
              </button>
            ))}
          </span>
        </span>
      }
      meta={current.meta}
      flush
    >
      <AlertRows alerts={current.rows} />
    </Panel>
  );
}

export default function Dashboard() {
  const { data: overview, error } = usePoll<Overview>("/api/overview", 3000);
  const { data: history } = usePoll<History>("/api/history", 30000);

  return (
    <>
      <PageHeader
        title="Overview"
        meta={
          <span className="flex items-center gap-1.5 text-xs text-ink-3">
            <span aria-hidden className="live-dot h-1.5 w-1.5 rounded-full bg-good" />
            Live, refreshes every 3s
          </span>
        }
      />
      <div className="space-y-4 p-4 sm:p-6">
        <ErrorNote error={error} />
        {!overview ? (
          !error && <p className="text-ink-3">Loading…</p>
        ) : (
          <>
            <OpenIncidentBanner overview={overview} />
            <Stats totals={overview.totals} />

            <div className="grid gap-4 lg:grid-cols-3">
              <Panel title="Throughput" meta="per 3s micro-batch, last 10 min" className="lg:col-span-2">
                <ThroughputChart data={overview.throughput} />
              </Panel>
              <Panel title="Error logs by service" meta="open alerts">
                <ServiceLoad alerts={overview.alerts} />
              </Panel>
            </div>

            <Panel title="Incidents" meta="one per root cause" flush id="incidents">
              {overview.incidents.length === 0 ? (
                <Empty>
                  No incidents yet. Crash a service from the <Link href="/simulate" className="text-brand hover:underline">Simulator</Link>.
                </Empty>
              ) : (
                <div className="overflow-x-auto">
                  <table className="data-table min-w-[760px] text-[13px]">
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Root cause</th>
                        <th>Severity</th>
                        <th>Status</th>
                        <th className="num">Logs</th>
                        <th className="num">Alerts</th>
                        <th className="num">Services</th>
                        <th>Opened</th>
                      </tr>
                    </thead>
                    <tbody>
                      {overview.incidents.map((i) => (
                        <tr key={i.id}>
                          <td>
                            <Link href={`/incident/${i.id}`} className="font-mono text-xs text-brand hover:underline">{i.id}</Link>
                          </td>
                          <td className="font-medium">{i.root_cause}</td>
                          <td><SeverityBadge severity={i.severity} /></td>
                          <td><StatusBadge status={i.status} /></td>
                          <td className="num">{fmt.format(i.total_logs)}</td>
                          <td className="num">{i.alert_ids.length}</td>
                          <td className="num">{i.affected_services.length}</td>
                          <td className="whitespace-nowrap text-ink-3">{timeAgo(i.created_at)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>

            <AlertsPanel alerts={overview.alerts} />

            {history ? <HistoryCharts history={history} /> : null}
          </>
        )}
      </div>
    </>
  );
}
