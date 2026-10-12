"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Empty, Panel } from "@/components/bits";
import { fmt, timeAgo, type History } from "@/lib/api";

const tooltipStyle = { background: "var(--surface)", border: "1px solid var(--rule)", borderRadius: 4, color: "var(--ink)", fontSize: 12 };

export function HistoryCharts({ history }: { history: History }) {
  if (!history.last_run) {
    return (
      <Panel title="Last 7 days" meta="batch layer">
        <Empty>
          No batch results yet. Run <code className="font-mono">make batch</code> to aggregate the logs archived in HDFS.
        </Empty>
      </Panel>
    );
  }

  const perHour = new Map<string, number>();
  for (const row of history.hourly) perHour.set(row.hour, (perHour.get(row.hour) ?? 0) + row.errors);
  const hours = [...perHour.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([hour, errors]) => ({
      hour,
      errors,
      label: new Date(hour).toLocaleString([], { weekday: "short", hour: "2-digit", minute: "2-digit", hour12: false }),
    }));
  const weekTotal = hours.reduce((sum, h) => sum + h.errors, 0);
  const maxService = Math.max(1, ...history.by_service.map((s) => s.errors));
  const runMeta = `MapReduce over HDFS, ran ${timeAgo(history.last_run.ran_at)}, ${fmt.format(history.last_run.logs_scanned)} logs in ${history.last_run.duration_sec}s`;

  return (
    <div className="grid gap-4 lg:grid-cols-3">
      <Panel title={`Error logs per hour, last ${history.last_run.days} days`} meta={runMeta} className="lg:col-span-2">
        <p className="text-xs text-ink-3">
          Total <span className="font-semibold text-ink tabular-nums">{fmt.format(weekTotal)}</span>
        </p>
        <div className="mt-2 h-56">
          <ResponsiveContainer>
            <BarChart data={hours} margin={{ top: 4, right: 4, bottom: 0, left: 0 }} barCategoryGap={1}>
              <CartesianGrid stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="label" tick={{ fill: "var(--ink-3)", fontSize: 11 }} tickLine={false} axisLine={{ stroke: "var(--rule)" }} minTickGap={60} />
              <YAxis tick={{ fill: "var(--ink-3)", fontSize: 11 }} tickLine={false} axisLine={false} width={44} tickFormatter={(v) => fmt.format(v)} />
              <Tooltip cursor={{ fill: "var(--sunken)" }} contentStyle={tooltipStyle} formatter={(value) => [fmt.format(Number(value)), "Error logs"]} />
              <Bar dataKey="errors" fill="var(--series-1)" radius={[2, 2, 0, 0]} isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Panel>

      <Panel title="By service" meta="last 7 days">
        <ul className="space-y-1.5">
          {history.by_service.map((s) => (
            <li key={s.service} className="grid grid-cols-[7rem_1fr_3.5rem] items-center gap-2 text-[13px]">
              <span className="truncate">{s.service}</span>
              <span className="h-1.5 rounded-sm bg-sunken">
                <span className="block h-full rounded-sm bg-[var(--series-1)]" style={{ width: `${(s.errors / maxService) * 100}%` }} />
              </span>
              <span className="text-right tabular-nums text-ink-2">{fmt.format(s.errors)}</span>
            </li>
          ))}
        </ul>
      </Panel>

      <Panel title="Most frequent error templates" meta="last 7 days" flush className="lg:col-span-3">
        <div className="overflow-x-auto">
          <table className="data-table min-w-[640px] text-[13px]">
            <thead>
              <tr>
                <th className="num">Count</th>
                <th>Service</th>
                <th>Template</th>
              </tr>
            </thead>
            <tbody>
              {history.top_templates.map((t) => (
                <tr key={t.service + t.template}>
                  <td className="num w-20">{fmt.format(t.count)}</td>
                  <td className="w-36 whitespace-nowrap">{t.service}</td>
                  <td className="font-mono text-xs text-ink-2">{t.template}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
