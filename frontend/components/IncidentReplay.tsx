"use client";

import { useEffect, useMemo, useState } from "react";
import { buttonSecondary } from "@/components/bits";
import { ServiceGraph } from "@/components/ServiceGraph";
import { fmt, type Alert, type GraphNode, type Incident, type IncidentDetail } from "@/lib/api";

type Frame = {
  phase: "before" | "spread" | "trace" | "done";
  caption: string;
  nodes: Record<string, GraphNode["status"]>;
  spread: Set<string>; // edge ids "source>target"
  path: Set<string>;
};

const STEP_MS = 1100;
const edgeId = (source: string, target: string) => `${source}>${target}`;

/**
 * The incident as a story, one step at a time:
 *   1. the failure starts in the root service and climbs to every service that calls it;
 *   2. LogIQ walks the dependency graph from the top of the chain down to the root cause.
 */
function buildFrames(incident: Incident, alerts: Alert[], graph: IncidentDetail["graph"]): Frame[] {
  const root = incident.root_cause;
  const affected = new Set(incident.affected_services);
  affected.add(root);
  const logs: Record<string, number> = {};
  for (const a of alerts) logs[a.service] = (logs[a.service] ?? 0) + a.count;
  const count = (s: string) => (logs[s] ? `${fmt.format(logs[s])} error logs` : "errors");

  // Order of spread: breadth-first from the root, up through callers that are failing.
  const order: { service: string; from?: string }[] = [{ service: root }];
  const seen = new Set([root]);
  for (let i = 0; i < order.length; i++) {
    for (const e of graph.edges) {
      if (e.target === order[i].service && affected.has(e.source) && !seen.has(e.source)) {
        seen.add(e.source);
        order.push({ service: e.source, from: e.target });
      }
    }
  }
  for (const s of affected) if (!seen.has(s)) order.push({ service: s });

  const frames: Frame[] = [];
  let nodes: Frame["nodes"] = {};
  let spread = new Set<string>();
  let path = new Set<string>();
  const push = (phase: Frame["phase"], caption: string) =>
    frames.push({ phase, caption, nodes: { ...nodes }, spread: new Set(spread), path: new Set(path) });

  push("before", "Before the incident every service is healthy.");

  for (const step of order) {
    nodes[step.service] = "affected";
    if (step.from) {
      spread.add(edgeId(step.service, step.from));
      push("spread", `${step.service} calls ${step.from}, which is failing, so it starts logging ${count(step.service)}.`);
    } else {
      push("spread", `${step.service} starts failing and logs ${count(step.service)}.`);
    }
  }

  const total = alerts.reduce((sum, a) => sum + a.count, 0);
  push(
    "trace",
    `The engineer would see ${fmt.format(total)} error logs from ${affected.size} services. LogIQ groups them into ${alerts.length} alerts and traces them through Neo4j, starting at ${incident.chain[0]}.`,
  );

  const chain = incident.chain;
  spread = new Set();
  for (let i = 0; i < chain.length - 1; i++) {
    nodes = { ...nodes, [chain[i]]: "checked" };
    path.add(edgeId(chain[i], chain[i + 1]));
    push("trace", `${chain[i]} depends on ${chain[i + 1]}, which is also failing. Keep going down.`);
  }

  nodes = { ...nodes, [root]: "root" };
  path = new Set(path);
  push("done", `${root} is failing and everything it depends on is healthy. Root cause: ${root}.`);
  return frames;
}

function prefersReducedMotion() {
  return typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

export function IncidentReplay({ incident, alerts, graph }: { incident: Incident; alerts: Alert[]; graph: IncidentDetail["graph"] }) {
  // Rebuild only when the incident's shape changes, not on every poll.
  const key = [incident.root_cause, incident.chain.join(","), incident.affected_services.join(","), alerts.map((a) => `${a.service}:${a.count}`).join(",")].join("|");
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const frames = useMemo(() => buildFrames(incident, alerts, graph), [key]);
  const last = frames.length - 1;

  const [step, setStep] = useState(() => (prefersReducedMotion() ? last : 0));
  const [playing, setPlaying] = useState(() => !prefersReducedMotion());
  const current = frames[Math.min(step, last)];

  useEffect(() => {
    if (!playing) return;
    if (step >= last) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setPlaying(false);
      return;
    }
    const t = setTimeout(() => setStep((s) => s + 1), STEP_MS);
    return () => clearTimeout(t);
  }, [playing, step, last]);

  const view: IncidentDetail["graph"] = useMemo(
    () => ({
      nodes: graph.nodes.map((n) => ({ ...n, status: current.nodes[n.name] ?? "healthy" })),
      edges: graph.edges.map((e) => ({
        ...e,
        on_path: current.path.has(edgeId(e.source, e.target)),
        spread: current.spread.has(edgeId(e.source, e.target)),
      })),
    }),
    [graph, current],
  );

  const phaseLabel = { before: "Before", spread: "Failure spreading", trace: "Tracing back", done: "Root cause found" }[current.phase];

  return (
    <div>
      <ServiceGraph graph={view} height={500} />
      <div className="mt-3 flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-1">
          <button type="button" className={buttonSecondary} aria-label="Previous step" onClick={() => { setPlaying(false); setStep((s) => Math.max(0, s - 1)); }} disabled={step === 0}>
            <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden><path d="M7.5 2.5 4 6l3.5 3.5" stroke="currentColor" strokeWidth="1.5" fill="none" /></svg>
          </button>
          {step >= last ? (
            <button type="button" className={buttonSecondary} onClick={() => { setStep(0); setPlaying(true); }}>Replay</button>
          ) : (
            <button type="button" className={`${buttonSecondary} w-[4.5rem]`} onClick={() => setPlaying((p) => !p)}>{playing ? "Pause" : "Play"}</button>
          )}
          <button type="button" className={buttonSecondary} aria-label="Next step" onClick={() => { setPlaying(false); setStep((s) => Math.min(last, s + 1)); }} disabled={step >= last}>
            <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden><path d="M4.5 2.5 8 6l-3.5 3.5" stroke="currentColor" strokeWidth="1.5" fill="none" /></svg>
          </button>
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-baseline justify-between gap-2 text-xs">
            <span className={`font-medium ${current.phase === "done" ? "text-crit" : "text-ink-2"}`}>{phaseLabel}</span>
            <span className="tabular-nums text-ink-3">{step + 1}/{frames.length}</span>
          </div>
          <div className="mt-1 h-1 overflow-hidden rounded-sm bg-sunken" aria-hidden>
            <div className="h-full bg-brand transition-[width] duration-300 motion-reduce:transition-none" style={{ width: `${(step / last) * 100}%` }} />
          </div>
        </div>
      </div>
      <p className="mt-2 min-h-[2.5rem] text-[13px] text-ink" aria-live="polite">{current.caption}</p>
    </div>
  );
}
