"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { buttonDanger, buttonPrimary, buttonSecondary, Empty, ErrorNote, field, PageHeader, Panel } from "@/components/bits";
import { ServiceGraph } from "@/components/ServiceGraph";
import { clock, fmt, postJson, timeAgo, usePoll, type GraphNode, type IncidentDetail, type SimulatorState } from "@/lib/api";

type Graph = { nodes: Omit<GraphNode, "status">[]; edges: { source: string; target: string }[] };

const LEVELS = ["ERROR", "FATAL", "WARN", "INFO", "DEBUG"];
const LEVEL_STYLE: Record<string, string> = {
  FATAL: "text-crit font-medium",
  ERROR: "text-crit",
  WARN: "text-warn",
  INFO: "text-ink-3",
  DEBUG: "text-ink-3",
};
const CRASH_LABELS = { root: "Will crash", affected: "Will fail because of the crash", checked: "Will fail because of the crash", healthy: "Not affected" };

function Slider(props: {
  id: string;
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  display: string;
  onChange: (v: number) => void;
}) {
  return (
    <div>
      <div className="flex items-baseline justify-between text-[13px]">
        <label htmlFor={props.id} className="text-ink-2">{props.label}</label>
        <span className="tabular-nums text-ink-2">{props.display}</span>
      </div>
      <input
        id={props.id}
        type="range"
        min={props.min}
        max={props.max}
        step={props.step}
        value={props.value}
        onChange={(e) => props.onChange(Number(e.target.value))}
        className="mt-1 w-full accent-[var(--brand)]"
      />
    </div>
  );
}

function Traffic({ state, onError }: { state: SimulatorState; onError: (e: string | null) => void }) {
  const [rate, setRate] = useState(state.rate);
  const [ratio, setRatio] = useState(Math.round(state.error_ratio * 100));
  const [busy, setBusy] = useState(false);
  const first = useRef(true);

  async function apply(running: boolean) {
    setBusy(true);
    try {
      await postJson("/api/simulator/traffic", { running, rate, error_ratio: ratio / 100 });
      onError(null);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  // While traffic runs, slider changes apply after a short pause.
  useEffect(() => {
    if (first.current) {
      first.current = false;
      return;
    }
    if (!state.running) return;
    const t = setTimeout(() => {
      postJson("/api/simulator/traffic", { running: true, rate, error_ratio: ratio / 100 }).catch((e) => onError(String(e)));
    }, 400);
    return () => clearTimeout(t);
    // Only react to slider changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rate, ratio]);

  return (
    <div className="space-y-4">
      <Slider id="rate" label="Logs per second" value={rate} min={0} max={1000} step={25} display={fmt.format(rate)} onChange={setRate} />
      <Slider id="ratio" label="Sporadic errors" value={ratio} min={0} max={20} step={1} display={`${ratio}%`} onChange={setRatio} />
      <p className="text-xs text-ink-3">
        Random one-off bugs across all services. They show as small alerts and never page. To trigger an incident, crash a service.
      </p>
      <div className="flex items-center gap-4">
        <button
          type="button"
          disabled={busy}
          onClick={() => apply(!state.running)}
          className={state.running ? buttonSecondary : buttonPrimary}
        >
          {state.running ? "Stop traffic" : "Start traffic"}
        </button>
        <span className="text-[13px] text-ink-2" role="status">
          {state.running && state.started_at ? (
            <>
              <span aria-hidden className="live-dot mr-1.5 inline-block h-1.5 w-1.5 rounded-full bg-good" />
              Running since {timeAgo(state.started_at)}.{" "}
              <Link href="/dashboard#alerts" className="text-brand hover:underline">See alerts</Link>
            </>
          ) : (
            "Stopped"
          )}
        </span>
      </div>
    </div>
  );
}

function CrashPanel({ graph, blast, onError }: { graph: Graph; blast: Record<string, string[]>; onError: (e: string | null) => void }) {
  const [target, setTarget] = useState("UserDB");
  const [size, setSize] = useState(1000);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const affectedKey = (blast[target] ?? []).join(",");
  const affected = useMemo(() => (affectedKey ? affectedKey.split(",") : []), [affectedKey]);

  // Rebuild the graph only when the selection changes, not on every 1 s poll,
  // so React Flow keeps its measured node sizes.
  const view: IncidentDetail["graph"] = useMemo(
    () => ({
      nodes: graph.nodes.map((n) => ({ ...n, status: n.name === target ? "root" : affected.includes(n.name) ? "affected" : "healthy" })),
      edges: graph.edges.map((e) => ({ ...e, on_path: false })),
    }),
    [graph, target, affected],
  );

  async function crash() {
    setBusy(true);
    setResult(null);
    try {
      const res = await postJson<{ sent: number; per_service: Record<string, number> }>("/api/simulator/crash", { service: target, size });
      setResult(`Sent ${fmt.format(res.sent)} error logs from ${Object.keys(res.per_service).length} services. An incident should open in about 10 seconds.`);
      onError(null);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <ServiceGraph graph={view} labels={CRASH_LABELS} onSelect={setTarget} height={420} />
      <div className="mt-3 grid gap-4 sm:grid-cols-[1fr_15rem] sm:items-end">
        <div>
          <p className="text-xs text-ink-3">Target (click a node to change)</p>
          <p className="text-base font-semibold text-crit">{target}</p>
          <p className="mt-0.5 text-[13px] text-ink-2">
            {affected.length > 0 ? `Also fails: ${affected.join(", ")}` : "Nothing depends on this service, so only its own errors are logged."}
          </p>
        </div>
        <div className="space-y-3">
          <Slider id="size" label="Burst size" value={size} min={100} max={5000} step={100} display={fmt.format(size)} onChange={setSize} />
          <button type="button" disabled={busy} onClick={crash} className={`${buttonDanger} w-full`}>
            {busy ? "Crashing…" : `Crash ${target}`}
          </button>
        </div>
      </div>
      {result && (
        <p role="status" className="mt-3 rounded bg-sunken px-3 py-2 text-[13px]">
          {result}{" "}
          <Link href="/dashboard" className="font-medium text-brand hover:underline">
            Open overview
          </Link>
        </p>
      )}
    </div>
  );
}

function CustomLog({ services, onError }: { services: string[]; onError: (e: string | null) => void }) {
  const [service, setService] = useState("PaymentAPI");
  const [level, setLevel] = useState("ERROR");
  const [message, setMessage] = useState("Card processor rejected batch settlement");
  const [repeat, setRepeat] = useState(50);
  const [vary, setVary] = useState(true);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  async function send(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setResult(null);
    try {
      const res = await postJson<{ sent: number }>("/api/simulator/logs", { service, level, message, repeat, vary_ids: vary });
      setResult(`Sent ${fmt.format(res.sent)} ${res.sent === 1 ? "log" : "logs"}.`);
      onError(null);
    } catch (err) {
      onError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <form onSubmit={send} className="space-y-3">
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label htmlFor="svc" className="text-[13px] text-ink-2">Service</label>
          <select id="svc" value={service} onChange={(e) => setService(e.target.value)} className={field}>
            {services.map((s) => <option key={s}>{s}</option>)}
          </select>
        </div>
        <div>
          <label htmlFor="lvl" className="text-[13px] text-ink-2">Level</label>
          <select id="lvl" value={level} onChange={(e) => setLevel(e.target.value)} className={field}>
            {LEVELS.map((l) => <option key={l}>{l}</option>)}
          </select>
        </div>
      </div>
      <div>
        <label htmlFor="msg" className="text-[13px] text-ink-2">Message</label>
        <input id="msg" value={message} onChange={(e) => setMessage(e.target.value)} required maxLength={500} className={`${field} font-mono text-xs`} />
      </div>
      <div className="grid grid-cols-2 items-end gap-3">
        <div>
          <label htmlFor="rep" className="text-[13px] text-ink-2">Copies</label>
          <input id="rep" type="number" min={1} max={5000} value={repeat} onChange={(e) => setRepeat(Number(e.target.value))} className={field} />
        </div>
        <label className="flex items-center gap-2 pb-1.5 text-[13px]">
          <input type="checkbox" checked={vary} onChange={(e) => setVary(e.target.checked)} className="accent-[var(--brand)]" />
          Random request id
        </label>
      </div>
      <p className="text-xs text-ink-3">INFO and DEBUG are dropped by the Bloom filter. 25+ Medium/High errors open an incident.</p>
      <div className="flex items-center gap-4">
        <button type="submit" disabled={busy} className={buttonPrimary}>
          {busy ? "Sending…" : "Send logs"}
        </button>
        {result && <span role="status" className="text-[13px] text-ink-2">{result}</span>}
      </div>
    </form>
  );
}

function StreamSwitch({ state, onError }: { state: SimulatorState; onError: (e: string | null) => void }) {
  const [busy, setBusy] = useState(false);
  const terminals = state.terminal_generators;
  const live = state.running || terminals.length > 0;

  async function stopAll() {
    setBusy(true);
    try {
      await postJson("/api/simulator/stop-all", {});
      onError(null);
    } catch (e) {
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className={`flex flex-wrap items-center gap-x-4 gap-y-2 rounded-md border bg-surface px-3 py-2 ${live ? "border-crit/30 border-l-4 border-l-crit" : "border-rule"}`}>
      <div className="flex min-w-0 flex-1 flex-wrap items-center gap-x-4 gap-y-1 text-[13px]" role="status">
        {live ? (
          <>
            <span className="flex items-center gap-1.5 font-medium">
              <span aria-hidden className="live-dot h-1.5 w-1.5 rounded-full bg-crit" />
              Streaming
            </span>
            {state.running && <span className="text-ink-2">this page: {fmt.format(state.rate)} logs/s</span>}
            {terminals.map((t) => (
              <span key={t.pid} className="text-ink-2">
                terminal pid {t.pid}: {fmt.format(t.rate)} logs/s, {fmt.format(t.sent)} sent
              </span>
            ))}
          </>
        ) : (
          <span className="text-ink-3">No streams running</span>
        )}
      </div>
      <button type="button" onClick={stopAll} disabled={busy || !live} className={live ? buttonDanger : buttonSecondary}>
        {busy ? "Stopping…" : "Stop all streams"}
      </button>
    </div>
  );
}

export default function SimulatePage() {
  const { data: state, error } = usePoll<SimulatorState>("/api/simulator", 1000);
  const { data: graph } = usePoll<Graph>("/api/graph", 60000);
  const [actionError, setActionError] = useState<string | null>(null);

  return (
    <>
      <PageHeader
        title="Simulator"
        meta={state && <span className="text-xs text-ink-3">{fmt.format(state.sent)} logs sent from here since API start</span>}
      />
      <div className="space-y-4 p-4 sm:p-6">
        <ErrorNote error={error} />
        {actionError && (
          <p role="alert" className="rounded border border-crit/30 bg-crit-wash px-3 py-2 text-[13px] text-crit">{actionError}</p>
        )}
        {state && <StreamSwitch state={state} onError={setActionError} />}

        {state && graph && (
          <>
            <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
              <Panel title="Crash a service" meta="the service and everything that calls it log errors">
                <CrashPanel graph={graph} blast={state.blast_radius} onError={setActionError} />
              </Panel>
              <div className="space-y-4">
                <Panel title="Background traffic" meta="normal requests, all 14 services">
                  <Traffic state={state} onError={setActionError} />
                </Panel>
                <Panel title="Send a log">
                  <CustomLog services={graph.nodes.map((n) => n.name).sort()} onError={setActionError} />
                </Panel>
              </div>
            </div>

            <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
              <Panel title="Sent logs" meta="latest 50, bursts sampled" flush>
                {state.recent.length === 0 ? (
                  <Empty>Nothing sent yet.</Empty>
                ) : (
                  <ol className="max-h-80 overflow-y-auto font-mono text-xs">
                    {state.recent.map((l) => (
                      <li key={l.id} className="grid grid-cols-[4.5rem_3rem_7rem_1fr] gap-2 border-b border-rule px-3 py-1 last:border-b-0">
                        <span className="text-ink-3">{clock(l.timestamp)}</span>
                        <span className={LEVEL_STYLE[l.level] ?? ""}>{l.level}</span>
                        <span className="truncate">{l.service}</span>
                        <span className="truncate text-ink-2" title={l.message}>{l.message}</span>
                      </li>
                    ))}
                  </ol>
                )}
              </Panel>
              <Panel title="Activity">
                {state.events.length === 0 ? (
                  <p className="text-[13px] text-ink-3">No actions yet.</p>
                ) : (
                  <ol className="space-y-1.5 text-[13px]">
                    {state.events.map((e, i) => (
                      <li key={i} className="grid grid-cols-[4.5rem_1fr] gap-2">
                        <span className="font-mono text-xs text-ink-3">{clock(e.at)}</span>
                        <span>{e.event}</span>
                      </li>
                    ))}
                  </ol>
                )}
              </Panel>
            </div>
          </>
        )}
      </div>
    </>
  );
}
