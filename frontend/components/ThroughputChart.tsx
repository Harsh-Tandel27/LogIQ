"use client";

import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Empty } from "@/components/bits";
import { clock, fmt, type Overview } from "@/lib/api";

const SERIES = [
  { key: "received", label: "Logs received", color: "var(--series-1)" },
  { key: "errors", label: "Error logs kept", color: "var(--series-2)" },
] as const;

export function ThroughputChart({ data }: { data: Overview["throughput"] }) {
  if (data.length === 0) return <Empty>No batches in the last 10 minutes.</Empty>;
  const rows = data.map((d) => ({ ...d, label: clock(d.at) }));
  return (
    <div>
      <ul className="mb-1 flex gap-4 text-xs text-ink-2" aria-label="Legend">
        {SERIES.map((s) => (
          <li key={s.key} className="flex items-center gap-1.5">
            <span className="inline-block h-0.5 w-3 rounded" style={{ background: s.color }} />
            {s.label}
          </li>
        ))}
      </ul>
      <div className="h-52">
        <ResponsiveContainer>
          <AreaChart data={rows} margin={{ top: 6, right: 4, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--rule)" vertical={false} />
            <XAxis dataKey="label" tick={{ fill: "var(--ink-3)", fontSize: 11 }} tickLine={false} axisLine={{ stroke: "var(--rule)" }} minTickGap={40} />
            <YAxis tick={{ fill: "var(--ink-3)", fontSize: 11 }} tickLine={false} axisLine={false} width={44} tickFormatter={(v) => fmt.format(v)} />
            <Tooltip
              contentStyle={{ background: "var(--surface)", border: "1px solid var(--rule)", borderRadius: 4, color: "var(--ink)", fontSize: 12 }}
              labelStyle={{ color: "var(--ink-2)" }}
              formatter={(value, name) => [fmt.format(Number(value)), SERIES.find((s) => s.key === name)?.label ?? name]}
            />
            {SERIES.map((s) => (
              <Area key={s.key} type="monotone" dataKey={s.key} stroke={s.color} strokeWidth={1.75} fill={s.color} fillOpacity={0.1} isAnimationActive={false} />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
