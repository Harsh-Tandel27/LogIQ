"use client";

import { useEffect, useMemo, useState } from "react";
import { applyNodeChanges, Background, Handle, MarkerType, Position, ReactFlow, type Edge, type Node, type NodeProps } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { GraphNode, IncidentDetail } from "@/lib/api";

type StatusLabels = Record<GraphNode["status"], string>;
type ServiceData = GraphNode & { labels: StatusLabels } & Record<string, unknown>;

const DEFAULT_LABELS: StatusLabels = {
  root: "Root cause",
  affected: "Failing because of a dependency",
  checked: "Traced: failing because of a dependency",
  healthy: "Healthy",
};

const NODE_STYLE: Record<GraphNode["status"], string> = {
  root: "border-crit bg-crit text-white root-pulse",
  affected: "border-warn bg-warn-wash text-ink",
  checked: "border-crit bg-warn-wash text-ink",
  healthy: "border-rule-strong bg-surface text-ink",
};

const DOT_STYLE: Record<GraphNode["status"], string> = {
  root: "bg-white",
  affected: "bg-warn",
  checked: "bg-crit",
  healthy: "bg-good",
};

function ServiceNode({ data }: NodeProps<Node<ServiceData>>) {
  return (
    <div
      className={`w-36 rounded border-[1.5px] px-2.5 py-1.5 transition-colors duration-300 motion-reduce:transition-none ${NODE_STYLE[data.status]}`}
      title={data.labels[data.status]}
    >
      <Handle type="target" position={Position.Top} className="!h-1.5 !w-1.5 !border-0 !bg-transparent" />
      <div className="flex items-center gap-1.5">
        <span
          aria-hidden
          className={`inline-block h-2 w-2 rounded-full ${DOT_STYLE[data.status]}`}
        />
        <span className="text-[13px] font-medium">{data.name}</span>
      </div>
      <div className={`text-xs ${data.status === "root" ? "text-white/85" : "text-ink-3"}`}>
        {data.kind}
        <span className="sr-only">, {data.labels[data.status]}</span>
      </div>
      <Handle type="source" position={Position.Bottom} className="!h-1.5 !w-1.5 !border-0 !bg-transparent" />
    </div>
  );
}

const nodeTypes = { service: ServiceNode };

interface ServiceGraphProps {
  graph: IncidentDetail["graph"];
  labels?: StatusLabels;
  onSelect?: (service: string) => void;
  height?: number;
}

export function ServiceGraph({ graph, labels = DEFAULT_LABELS, onSelect, height = 560 }: ServiceGraphProps) {
  const { nodes, edges } = useMemo(() => {
    // Layered layout: callers on top, databases and external systems at the bottom.
    const byTier = new Map<number, GraphNode[]>();
    for (const n of graph.nodes) byTier.set(n.tier, [...(byTier.get(n.tier) ?? []), n]);
    const widest = Math.max(...[...byTier.values()].map((row) => row.length));
    const gapX = 172;

    const nodes: Node<ServiceData>[] = graph.nodes.map((n) => {
      const row = byTier.get(n.tier)!;
      const offset = ((widest - row.length) * gapX) / 2;
      return {
        id: n.name,
        type: "service",
        position: { x: offset + row.indexOf(n) * gapX, y: n.tier * 118 },
        data: { ...n, labels } as ServiceData,
        draggable: false,
      };
    });

    const edges: Edge[] = graph.edges.map((e) => {
      const color = e.on_path ? "var(--crit)" : e.spread ? "var(--warn)" : "var(--ink-3)";
      const lit = e.on_path || e.spread;
      return {
        id: `${e.source}-${e.target}`,
        source: e.source,
        target: e.target,
        animated: lit,
        style: { stroke: color, strokeWidth: lit ? 2.5 : 1.2, opacity: lit ? 1 : 0.55 },
        markerEnd: { type: MarkerType.ArrowClosed, color },
      };
    });
    return { nodes, edges };
  }, [graph, labels]);

  // Controlled nodes: React Flow writes measured sizes back through onNodesChange.
  // On new data, keep each node's measurements so the graph never blanks while re-measuring.
  const [rfNodes, setRfNodes] = useState(nodes);
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setRfNodes((prev) =>
      nodes.map((n) => {
        const old = prev.find((p) => p.id === n.id);
        return old ? { ...old, data: n.data, position: n.position } : n;
      }),
    );
  }, [nodes]);

  return (
    <div className={`rounded border border-rule bg-[#fafbfc] ${onSelect ? "graph-clickable" : ""}`} style={{ height }}>
      <ReactFlow
        nodes={rfNodes}
        edges={edges}
        onNodesChange={(changes) => setRfNodes((ns) => applyNodeChanges(changes, ns))}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.12 }}
        nodesConnectable={false}
        onNodeClick={onSelect ? (_, node) => onSelect(node.id) : undefined}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#d9dee4" gap={20} size={1} />
      </ReactFlow>
    </div>
  );
}
