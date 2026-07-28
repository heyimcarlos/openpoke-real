"use client";

// Portable React Flow primitives for systems-lab-ui.
import {
  BaseEdge,
  EdgeLabelRenderer,
  Handle,
  Position,
  getSmoothStepPath,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import {
  Activity,
  Braces,
  Box,
  CircleDashed,
  Database,
  ExternalLink,
  Layers3,
} from "lucide-react";

export type SystemNodeKind =
  | "durable"
  | "runtime"
  | "interface"
  | "optional"
  | "pure"
  | "external";

export type SystemNodeState = "idle" | "active" | "complete" | "attention";

export type SystemNodeData = {
  kind: SystemNodeKind;
  state?: SystemNodeState;
  eyebrow: string;
  title: string;
  detail: string;
  facts?: string[];
  identity?: string;
};

export type SystemEdgeKind =
  | "call"
  | "return"
  | "commit"
  | "claim"
  | "relation";

export type SystemEdgeData = {
  kind: SystemEdgeKind;
  label?: string;
  sequence?: number;
  active?: boolean;
  labelOffsetX?: number;
  labelOffsetY?: number;
};

export type SystemCanvasNode = Node<SystemNodeData, "system">;
export type SystemCanvasEdge = Edge<SystemEdgeData, "system">;

const KIND_STYLES: Record<SystemNodeKind, string> = {
  durable:
    "rounded-md border-2 border-slate-800 bg-white shadow-[7px_7px_0_0_#dbe4ee]",
  runtime: "rounded-2xl border border-slate-300 bg-white shadow-lg",
  interface:
    "rounded-xl border-4 border-double border-slate-500 bg-white shadow-sm",
  optional: "rounded-xl border-2 border-dashed border-slate-400 bg-white",
  pure: "rounded-lg border border-slate-300 bg-slate-50",
  external: "rounded-full border-2 border-dotted border-slate-400 bg-white",
};

const STATE_STYLES: Record<SystemNodeState, string> = {
  idle: "ring-0",
  active: "ring-4 ring-blue-200",
  complete: "ring-4 ring-emerald-100",
  attention: "ring-4 ring-red-200",
};

const STATE_DOT: Record<SystemNodeState, string> = {
  idle: "bg-slate-300",
  active: "bg-blue-500",
  complete: "bg-emerald-500",
  attention: "bg-red-500",
};

const EDGE_COLOR: Record<SystemEdgeKind, string> = {
  call: "#2563eb",
  return: "#db2777",
  commit: "#ea580c",
  claim: "#0f766e",
  relation: "#64748b",
};

const KIND_ICON = {
  durable: Database,
  runtime: Activity,
  interface: Braces,
  optional: CircleDashed,
  pure: Box,
  external: ExternalLink,
} satisfies Record<SystemNodeKind, typeof Layers3>;

function classes(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(" ");
}

export function SystemNode({ data, selected }: NodeProps<SystemCanvasNode>) {
  const state = data.state ?? "idle";
  const Icon = KIND_ICON[data.kind];

  return (
    <article
      className={classes(
        "relative w-[230px] px-4 py-3 text-slate-900 transition",
        KIND_STYLES[data.kind],
        STATE_STYLES[state],
        selected && "outline outline-2 outline-offset-4 outline-blue-500",
      )}
    >
      <Handle type="target" position={Position.Left} id="left-in" />
      <Handle type="source" position={Position.Left} id="left-out" />
      <Handle type="target" position={Position.Right} id="right-in" />
      <Handle type="source" position={Position.Right} id="right-out" />
      <Handle type="target" position={Position.Top} id="top-in" />
      <Handle type="source" position={Position.Top} id="top-out" />
      <Handle type="target" position={Position.Bottom} id="bottom-in" />
      <Handle type="source" position={Position.Bottom} id="bottom-out" />

      <header className="mb-2 flex items-center justify-between gap-3">
        <span className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-[0.18em] text-slate-500">
          <Icon aria-hidden className="size-3.5" />
          {data.kind}
        </span>
        <span
          aria-label={state}
          className={classes("size-2.5 rounded-full", STATE_DOT[state])}
        />
      </header>

      <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
        {data.eyebrow}
      </p>
      <h3 className="mt-0.5 text-base font-semibold leading-tight">
        {data.title}
      </h3>
      <p className="mt-2 text-xs leading-5 text-slate-600">{data.detail}</p>

      {data.facts?.length ? (
        <ul className="mt-3 space-y-1 border-t border-slate-200 pt-2 text-[11px] text-slate-600">
          {data.facts.slice(0, 3).map((fact) => (
            <li key={fact}>{fact}</li>
          ))}
        </ul>
      ) : null}

      {data.identity ? (
        <footer className="mt-3 overflow-hidden text-ellipsis whitespace-nowrap border-t border-slate-200 pt-2 font-mono text-[10px] text-slate-400">
          {data.identity}
        </footer>
      ) : null}
    </article>
  );
}

export function SystemEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  data,
}: EdgeProps<SystemCanvasEdge>) {
  const edgeData = data ?? { kind: "relation" };
  const color = EDGE_COLOR[edgeData.kind];
  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 16,
    offset: 28,
  });

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        style={{
          stroke: color,
          strokeWidth: edgeData.active ? 3 : 1.5,
          strokeDasharray: edgeData.active ? "8 7" : undefined,
          opacity: edgeData.active ? 1 : 0.35,
        }}
      />

      {edgeData.active ? (
        <circle r="5" fill={color}>
          <animateMotion dur="1.2s" path={path} repeatCount="indefinite" />
        </circle>
      ) : null}

      {edgeData.label ? (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan pointer-events-none absolute rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[10px] font-semibold text-slate-700 shadow-sm"
            style={{
              transform: `translate(-50%, -50%) translate(${
                labelX + (edgeData.labelOffsetX ?? 0)
              }px, ${labelY + (edgeData.labelOffsetY ?? 0)}px)`,
              opacity: edgeData.active ? 1 : 0.68,
            }}
          >
            {edgeData.sequence ? `${edgeData.sequence}. ` : ""}
            {edgeData.label}
          </div>
        </EdgeLabelRenderer>
      ) : null}
    </>
  );
}

export const systemNodeTypes = { system: SystemNode };
export const systemEdgeTypes = { system: SystemEdge };
