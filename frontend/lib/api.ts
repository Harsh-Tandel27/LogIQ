"use client";

import { useCallback, useEffect, useState } from "react";

export type Severity = "High" | "Medium" | "Low";

export interface Alert {
  id: string;
  service: string;
  level: string;
  message: string;
  template: string;
  variants: string[];
  count: number;
  severity: Severity;
  severity_confidence: number;
  first_seen: string;
  last_seen: string;
  status: "open" | "closed";
  incident_id: string | null;
}

export interface Recommendation {
  id: string;
  title: string;
  service: string;
  fix: string;
  score: number;
}

export interface Incident {
  id: string;
  root_cause: string;
  chain: string[];
  status: "open" | "resolved";
  severity: Severity;
  created_at: string;
  resolved_at?: string;
  resolution?: string;
  alert_ids: string[];
  affected_services: string[];
  total_logs: number;
  slack_sent?: boolean;
  notified_at?: string;
  recommendations?: Recommendation[];
  timeline?: { at: string; event: string }[];
}

export interface Overview {
  totals: {
    received: number;
    bloom_dropped: number;
    errors: number;
    alerts: number;
    incidents: number;
    open_incidents: number;
    logs_per_alert: number | null;
  };
  throughput: { at: string; received: number; bloom_dropped: number; errors: number; alerts_created: number }[];
  incidents: Incident[];
  alerts: Alert[];
}

export interface History {
  hourly: { hour: string; service: string; errors: number }[];
  by_service: { service: string; errors: number }[];
  levels: { day: string; level: string; count: number }[];
  top_templates: { service: string; template: string; count: number }[];
  last_run: { ran_at: string; logs_scanned: number; duration_sec: number; days: number } | null;
}

export interface GraphNode {
  name: string;
  kind: string;
  tier: number;
  status: "root" | "affected" | "checked" | "healthy";
}

export interface GraphEdge {
  source: string;
  target: string;
  on_path: boolean;
  /** Error travelling from a failed dependency up to its caller (replay only). */
  spread?: boolean;
}

export interface IncidentDetail {
  incident: Incident;
  alerts: Alert[];
  graph: { nodes: GraphNode[]; edges: GraphEdge[] };
}

export interface LogLine {
  id: string;
  timestamp: string;
  service: string;
  level: string;
  message: string;
}

export interface TerminalGenerator {
  pid: number;
  mode: string;
  rate: number;
  sent: number;
  started_at: string;
  last_beat: string;
}

export interface SimulatorState {
  terminal_generators: TerminalGenerator[];
  running: boolean;
  rate: number;
  error_ratio: number;
  sent: number;
  started_at: string | null;
  recent: LogLine[];
  events: { at: string; event: string }[];
  blast_radius: Record<string, string[]>;
}

/** POST JSON and return the parsed body, or throw with the API's error detail. */
export async function postJson<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : `${res.status} ${res.statusText}`);
  return data as T;
}

/** Fetch `url` now and then every `intervalMs`. */
export function usePoll<T>(url: string, intervalMs: number) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      setData(await res.json());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [url]);

  useEffect(() => {
    // Initial fetch, then poll. setState happens after the await, not synchronously.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    load();
    const id = setInterval(load, intervalMs);
    return () => clearInterval(id);
  }, [load, intervalMs]);

  return { data, error, reload: load };
}

export const fmt = new Intl.NumberFormat("en-US");

export function timeAgo(iso: string): string {
  const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  return `${Math.round(seconds / 86400)}d ago`;
}

export function clock(iso: string): string {
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}
