'use client';

import {
  Background,
  BackgroundVariant,
  MarkerType,
  ReactFlow,
  applyNodeChanges,
  type NodeChange,
  type ReactFlowInstance,
} from '@xyflow/react';
import {
  ChevronLeft,
  ChevronRight,
  Eye,
  EyeOff,
  Focus,
  Lock,
  Maximize2,
  Pause,
  Play,
  RotateCcw,
  Unlock,
} from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  systemEdgeTypes,
  systemNodeTypes,
  type SystemCanvasEdge,
  type SystemCanvasNode,
  type SystemEdgeKind,
  type SystemNodeData,
} from './SystemCanvasPrimitives';

type ScenarioId = 'today' | 'workflow' | 'recovery' | 'scale';
type ViewMode = 'flowing' | 'full';
type Position = { x: number; y: number };

type LabStep = {
  title: string;
  narration: string;
  edgeId: string;
  attentionNode?: string;
};

type LabScenario = {
  id: ScenarioId;
  label: string;
  kicker: string;
  title: string;
  summary: string;
  proofs: string[];
  nodes: SystemCanvasNode[];
  edges: SystemCanvasEdge[];
  steps: LabStep[];
};

const edge = (
  id: string,
  source: string,
  target: string,
  kind: SystemEdgeKind,
  label: string,
  sequence: number,
  handles: { sourceHandle?: string; targetHandle?: string } = {},
): SystemCanvasEdge => ({
  id,
  source,
  target,
  sourceHandle: `${handles.sourceHandle ?? 'right'}-out`,
  targetHandle: `${handles.targetHandle ?? 'left'}-in`,
  type: 'system',
  markerEnd: { type: MarkerType.ArrowClosed },
  data: { kind, label, sequence },
});

const node = (
  id: string,
  position: Position,
  data: SystemNodeData,
): SystemCanvasNode => ({
  id,
  position,
  type: 'system',
  data,
});

const TODAY_POSITIONS: Record<string, Position> = {
  client: { x: 0, y: 70 },
  api: { x: 310, y: 70 },
  ledger: { x: 650, y: 70 },
  workers: { x: 990, y: 70 },
  policy: { x: 1330, y: 70 },
  executor: { x: 1650, y: 70 },
  result: { x: 990, y: 430 },
  recovery: { x: 650, y: 430 },
};

const SCALE_POSITIONS: Record<string, Position> = {
  client: { x: 0, y: 80 },
  lb: { x: 290, y: 80 },
  api: { x: 580, y: 80 },
  threads: { x: 880, y: 80 },
  orchestrator: { x: 1200, y: 80 },
  command: { x: 1520, y: 80 },
  kernel: { x: 1840, y: 80 },
  steps: { x: 1840, y: 430 },
  dispatcher: { x: 1520, y: 430 },
  executors: { x: 1200, y: 430 },
  wait: { x: 880, y: 430 },
  outbox: { x: 580, y: 430 },
  rabbit: { x: 290, y: 430 },
  delivery: { x: 0, y: 430 },
};

const WORKFLOW_POSITIONS: Record<string, Position> = {
  client: { x: 0, y: 70 },
  api: { x: 300, y: 70 },
  threads: { x: 600, y: 70 },
  orchestrator: { x: 900, y: 70 },
  command: { x: 1200, y: 70 },
  kernel: { x: 1500, y: 70 },
  steps: { x: 1800, y: 70 },
  executor: { x: 1500, y: 440 },
  wait: { x: 1200, y: 440 },
  signal: { x: 900, y: 440 },
  result: { x: 600, y: 440 },
};

const RECOVERY_POSITIONS: Record<string, Position> = {
  step: { x: 0, y: 70 },
  worker1: { x: 300, y: 70 },
  sdk: { x: 600, y: 70 },
  snapshot: { x: 900, y: 70 },
  wait: { x: 1200, y: 70 },
  signal: { x: 1500, y: 70 },
  worker2: { x: 1200, y: 440 },
  codec: { x: 900, y: 440 },
  kernel: { x: 600, y: 440 },
  result: { x: 300, y: 440 },
};

const TODAY_NODES = [
  node('client', TODAY_POSITIONS.client, {
    kind: 'external',
    eyebrow: 'Public boundary',
    title: 'Web or SMS client',
    detail: 'Sends a business message, never a raw execution task.',
    facts: ['JWT or verified channel binding', 'Idempotency key: turn_id'],
    identity: 'actor=user-7',
  }),
  node('api', TODAY_POSITIONS.api, {
    kind: 'interface',
    eyebrow: 'FastAPI control plane',
    title: 'Authenticated API',
    detail: 'Verifies the Principal and translates trusted intent into work.',
    facts: ['Tenant derived from auth', 'Returns 202 after durable acceptance'],
    identity: 'tenant=tenant-a',
  }),
  node('ledger', TODAY_POSITIONS.ledger, {
    kind: 'durable',
    eyebrow: 'PostgreSQL authority',
    title: 'Task ledger + queue',
    detail: 'Owns acceptance, status, attempts, leases, results, and admission.',
    facts: ['50 outstanding / tenant', '2 active / tenant', '8 active globally'],
    identity: 'task_id + lease_generation',
  }),
  node('workers', TODAY_POSITIONS.workers, {
    kind: 'runtime',
    eyebrow: 'Bounded compute',
    title: 'Execution worker pool',
    detail: 'Workers race for one claim using SKIP LOCKED and a finite lease.',
    facts: ['2 slots / process', '3 attempts maximum', '90 second execution cap'],
    identity: 'worker_id',
  }),
  node('policy', TODAY_POSITIONS.policy, {
    kind: 'pure',
    eyebrow: 'Deterministic policy',
    title: 'Executor routing',
    detail: 'Selects synthetic or reasoning execution from trusted task policy.',
    facts: ['No model decides authority', 'Unknown kinds fail closed'],
  }),
  node('executor', TODAY_POSITIONS.executor, {
    kind: 'runtime',
    eyebrow: 'Disposable work',
    title: 'Task executor',
    detail: 'Runs synthetic work for tests or one bounded Agents SDK call.',
    facts: ['No live LLMs in load tests', 'Process may disappear safely'],
    identity: 'attempt=task + generation',
  }),
  node('result', TODAY_POSITIONS.result, {
    kind: 'durable',
    eyebrow: 'Fenced commit',
    title: 'Authoritative result',
    detail: 'Only the current unexpired lease may commit the winning result.',
    facts: ['Stale writes rejected', 'Terminal state is inspectable'],
    identity: 'status=completed',
  }),
  node('recovery', TODAY_POSITIONS.recovery, {
    kind: 'optional',
    eyebrow: 'Failure path',
    title: 'Lease expiry + retry',
    detail: 'A fresh worker reclaims abandoned work without trusting the old process.',
    facts: ['PostgreSQL clock', 'Generation increments', 'Attempt 3 dead-letters'],
    identity: 'failure=lease_expired',
  }),
];

const TODAY_EDGES = [
  edge('today-1', 'client', 'api', 'call', 'submit message', 1),
  edge('today-2', 'api', 'ledger', 'commit', 'accept task', 2),
  edge('today-3', 'ledger', 'workers', 'claim', 'lease work', 3),
  edge('today-4', 'workers', 'policy', 'call', 'resolve executor', 4),
  edge('today-5', 'policy', 'executor', 'call', 'execute', 5),
  edge('today-6', 'executor', 'result', 'return', 'typed result', 6, {
    sourceHandle: 'bottom',
    targetHandle: 'right',
  }),
  edge('today-7', 'result', 'ledger', 'commit', 'fenced completion', 7, {
    sourceHandle: 'left',
    targetHandle: 'bottom',
  }),
  edge('today-8', 'workers', 'recovery', 'relation', 'process failure', 8, {
    sourceHandle: 'bottom',
    targetHandle: 'right',
  }),
  edge('today-9', 'recovery', 'ledger', 'claim', 'reclaim attempt', 9, {
    sourceHandle: 'top',
    targetHandle: 'bottom',
  }),
];

const WORKFLOW_NODES = [
  node('client', WORKFLOW_POSITIONS.client, {
    kind: 'external',
    eyebrow: 'Public boundary',
    title: 'Web or SMS message',
    detail: 'The user submits a business message, not a raw task or graph.',
    facts: ['Verified JWT or channel binding', 'Tenant comes from Principal'],
    identity: 'actor + tenant',
  }),
  node('api', WORKFLOW_POSITIONS.api, {
    kind: 'interface',
    eyebrow: 'Stateless control plane',
    title: 'Authenticated API',
    detail: 'Appends the message durably before acknowledging the request.',
    facts: ['Idempotent ingress', 'Raw execution API stays internal'],
  }),
  node('threads', WORKFLOW_POSITIONS.threads, {
    kind: 'durable',
    eyebrow: 'Conversation authority',
    title: 'Thread + Agent Run',
    detail: 'Owns ordered messages and one recoverable orchestrator turn.',
    facts: ['Frozen message cutoff', '50 context items', '2 delegations per run'],
    identity: 'thread_id + run_id',
  }),
  node('orchestrator', WORKFLOW_POSITIONS.orchestrator, {
    kind: 'runtime',
    eyebrow: 'Disposable reasoning',
    title: 'Orchestrator worker',
    detail: 'Rehydrates one turn, then replies, delegates, or starts a workflow.',
    facts: ['One active run per Thread', 'Separate capacity pool'],
    identity: 'run lease generation',
  }),
  node('command', WORKFLOW_POSITIONS.command, {
    kind: 'interface',
    eyebrow: 'Authority boundary',
    title: 'Typed start command',
    detail: 'Selects a team-published definition and supplies validated input.',
    facts: ['Explicit version', 'No invented Steps or routes'],
    identity: 'definition_key@version',
  }),
  node('kernel', WORKFLOW_POSITIONS.kernel, {
    kind: 'durable',
    eyebrow: 'PostgreSQL kernel',
    title: 'Workflow Instance',
    detail: 'Serializes authoritative transitions for one workflow execution.',
    facts: ['Immutable definition', 'Ordered trace events', 'Atomic transitions'],
    identity: 'instance_id',
  }),
  node('steps', WORKFLOW_POSITIONS.steps, {
    kind: 'durable',
    eyebrow: 'Dependency state',
    title: 'Steps + task attempts',
    detail:
      'Roots are queued, descendants stay blocked until every prerequisite succeeds.',
    facts: ['Sequential and parallel', 'AND dependencies', 'Queue order is irrelevant'],
    identity: 'step_id + task lease',
  }),
  node('executor', WORKFLOW_POSITIONS.executor, {
    kind: 'runtime',
    eyebrow: 'Bounded execution',
    title: 'Worker + executor',
    detail: 'Claims runnable work under tenant and global capacity limits.',
    facts: ['2 active per tenant', '8 active globally', '3 attempts maximum'],
    identity: 'task_id + lease_generation',
  }),
  node('wait', WORKFLOW_POSITIONS.wait, {
    kind: 'optional',
    eyebrow: 'Durable pause',
    title: 'Wait',
    detail: 'Blocks a predefined route without keeping a worker alive.',
    facts: ['Storage, not compute', 'Survives restart'],
    identity: 'wait_id',
  }),
  node('signal', WORKFLOW_POSITIONS.signal, {
    kind: 'interface',
    eyebrow: 'Authenticated decision',
    title: 'Exact Signal',
    detail: 'A later user turn satisfies one exact Wait and one allowed route.',
    facts: ['Same tenant and actor', 'Fresh user action', 'Replay-safe'],
    identity: 'signal_id + wait_id',
  }),
  node('result', WORKFLOW_POSITIONS.result, {
    kind: 'durable',
    eyebrow: 'Visible outcome',
    title: 'Fenced completion',
    detail: 'The kernel records the winner, releases successors, or completes.',
    facts: ['Stale result rejected', 'Same Thread provenance'],
    identity: 'workflow event sequence',
  }),
];

const WORKFLOW_EDGES = [
  edge('workflow-1', 'client', 'api', 'call', 'business message', 1),
  edge('workflow-2', 'api', 'threads', 'commit', 'append message', 2),
  edge('workflow-3', 'threads', 'orchestrator', 'claim', 'lease turn', 3),
  edge('workflow-4', 'orchestrator', 'command', 'call', 'typed command', 4),
  edge('workflow-5', 'command', 'kernel', 'commit', 'start pinned instance', 5),
  edge('workflow-6', 'kernel', 'steps', 'commit', 'materialize graph', 6),
  edge('workflow-7', 'steps', 'executor', 'claim', 'claim runnable step', 7, {
    sourceHandle: 'bottom',
    targetHandle: 'right',
  }),
  edge('workflow-8', 'executor', 'kernel', 'return', 'fenced result', 8, {
    sourceHandle: 'top',
    targetHandle: 'bottom',
  }),
  edge('workflow-9', 'kernel', 'wait', 'commit', 'open wait', 9, {
    sourceHandle: 'bottom',
    targetHandle: 'top',
  }),
  edge('workflow-10', 'signal', 'wait', 'call', 'satisfy exact wait', 10),
  edge('workflow-11', 'wait', 'kernel', 'commit', 'release route', 11, {
    sourceHandle: 'top',
    targetHandle: 'bottom',
  }),
  edge('workflow-12', 'kernel', 'result', 'commit', 'record outcome', 12, {
    sourceHandle: 'bottom',
    targetHandle: 'right',
  }),
];

const RECOVERY_NODES = [
  node('step', RECOVERY_POSITIONS.step, {
    kind: 'durable',
    eyebrow: 'Logical work',
    title: 'Same Workflow Step',
    detail: 'One logical Step survives across two separately leased Attempts.',
    facts: ['Attempt 1: running → blocked', 'Attempt 2: runnable → completed'],
    identity: 'step_id + task_id',
  }),
  node('worker1', RECOVERY_POSITIONS.worker1, {
    kind: 'runtime',
    eyebrow: 'Disposable process',
    title: 'Worker, Attempt 1',
    detail: 'Claims the Step and runs bounded reasoning until approval is required.',
    facts: ['Lease generation 1', 'Completion authority is temporary'],
    identity: 'worker=before-restart',
  }),
  node('sdk', RECOVERY_POSITIONS.sdk, {
    kind: 'runtime',
    eyebrow: 'Temporary coordination',
    title: 'Agents SDK manager',
    detail: 'Uses two specialists as tools, then reaches one approval interruption.',
    facts: ['Typed inputs and result', 'Bounded requests and child calls'],
    identity: 'agent_definition=v1',
  }),
  node('snapshot', RECOVERY_POSITIONS.snapshot, {
    kind: 'durable',
    eyebrow: 'Write-once Attempt data',
    title: 'Versioned RunState',
    detail: 'Stores enough SDK state to resume in a different process.',
    facts: [
      'Codec + SDK + agent versions',
      'Usage counters + SHA-256',
      'Secret-shaped state rejected',
    ],
    identity: 'attempt=1 + lease_generation=1',
  }),
  node('wait', RECOVERY_POSITIONS.wait, {
    kind: 'optional',
    eyebrow: 'Durable pause',
    title: 'Approval Wait',
    detail: 'Blocks the Step and immediately releases the worker capacity slot.',
    facts: ['No sleeping process', 'Old lease is revoked'],
    identity: 'wait_id',
  }),
  node('signal', RECOVERY_POSITIONS.signal, {
    kind: 'interface',
    eyebrow: 'Authenticated authority',
    title: 'Exact approve Signal',
    detail: 'A valid user action satisfies only the published approval Wait.',
    facts: ['Tenant and actor checked', 'Audit note, not routing'],
    identity: 'signal_key=approve',
  }),
  node('worker2', RECOVERY_POSITIONS.worker2, {
    kind: 'runtime',
    eyebrow: 'Fresh process',
    title: 'Worker, Attempt 2',
    detail: 'Claims the same Step under a strictly newer lease and Attempt.',
    facts: ['Lease generation 2', 'Budgets continue, never reset'],
    identity: 'worker=after-restart',
  }),
  node('codec', RECOVERY_POSITIONS.codec, {
    kind: 'pure',
    eyebrow: 'Fail-closed restore',
    title: 'Compatibility gate',
    detail: 'Checks integrity, exact versions, and Workflow IDs before restore.',
    facts: ['Allowlisted context only', 'Tracing API key excluded'],
    identity: 'instance_id + step_id + task_id',
  }),
  node('kernel', RECOVERY_POSITIONS.kernel, {
    kind: 'durable',
    eyebrow: 'Transition authority',
    title: 'Workflow kernel',
    detail: 'Accepts only the current lease result and advances the workflow.',
    facts: ['Stale Attempt 1 rejected', 'One serialized transition'],
    identity: 'instance event sequence',
  }),
  node('result', RECOVERY_POSITIONS.result, {
    kind: 'durable',
    eyebrow: 'Visible outcome',
    title: 'Typed Step result',
    detail: 'The resumed manager completes without repeating approved work.',
    facts: ['Provider-free restart proof', 'Workflow completes once'],
    identity: 'status=completed',
  }),
];

const RECOVERY_EDGES = [
  edge('recovery-1', 'step', 'worker1', 'claim', 'lease Attempt 1', 1),
  edge('recovery-2', 'worker1', 'sdk', 'call', 'run manager', 2),
  edge('recovery-3', 'sdk', 'snapshot', 'commit', 'serialize interruption', 3),
  edge('recovery-4', 'snapshot', 'wait', 'commit', 'block + revoke lease', 4),
  edge('recovery-5', 'signal', 'wait', 'call', 'approve exact wait', 5, {
    sourceHandle: 'left',
    targetHandle: 'right',
  }),
  edge('recovery-6', 'wait', 'step', 'commit', 'release same Step', 6, {
    sourceHandle: 'top',
    targetHandle: 'top',
  }),
  edge('recovery-7', 'step', 'worker2', 'claim', 'lease Attempt 2', 7, {
    sourceHandle: 'bottom',
    targetHandle: 'left',
  }),
  edge('recovery-8', 'worker2', 'snapshot', 'claim', 'load snapshot', 8, {
    sourceHandle: 'top',
    targetHandle: 'bottom',
  }),
  edge('recovery-9', 'snapshot', 'codec', 'call', 'verify + restore', 9, {
    sourceHandle: 'bottom',
    targetHandle: 'top',
  }),
  edge('recovery-10', 'codec', 'worker2', 'return', 'resume state', 10),
  edge('recovery-11', 'worker2', 'kernel', 'return', 'typed result', 11),
  edge('recovery-12', 'kernel', 'result', 'commit', 'fenced completion', 12),
];

const SCALE_NODES = [
  node('client', SCALE_POSITIONS.client, {
    kind: 'external',
    eyebrow: 'Public traffic',
    title: 'Web, SMS, email',
    detail: 'Users submit messages and business commands.',
    facts: ['No raw tasks exposed'],
    identity: 'channel_binding',
  }),
  node('lb', SCALE_POSITIONS.lb, {
    kind: 'interface',
    eyebrow: 'Traffic boundary',
    title: 'Load balancer',
    detail: 'Distributes stateless requests across API replicas.',
    facts: ['Health checks', 'TLS termination'],
  }),
  node('api', SCALE_POSITIONS.api, {
    kind: 'runtime',
    eyebrow: 'Stateless service',
    title: 'API replicas',
    detail: 'Authenticate, authorize, and persist inbound messages.',
    facts: ['Scale horizontally', 'No resident agent sessions'],
  }),
  node('threads', SCALE_POSITIONS.threads, {
    kind: 'durable',
    eyebrow: 'Conversation authority',
    title: 'Threads + Agent Runs',
    detail: 'Own ordered messages, coalescing, frozen context, and turn recovery.',
    facts: ['1 queued/running per Thread', '50 context items', '2 delegations / run'],
    identity: 'thread_id + run_id',
  }),
  node('orchestrator', SCALE_POSITIONS.orchestrator, {
    kind: 'runtime',
    eyebrow: 'Separate capacity pool',
    title: 'Orchestrator workers',
    detail: 'Answer directly, delegate one task, or start a published workflow.',
    facts: ['Disposable processes', 'Serialized per Thread'],
    identity: 'agent_run_lease',
  }),
  node('command', SCALE_POSITIONS.command, {
    kind: 'interface',
    eyebrow: 'Trusted command',
    title: 'Typed workflow start',
    detail: 'Selects a published definition and supplies validated inputs.',
    facts: ['Explicit version', 'Agent cannot invent structure'],
    identity: 'definition_key@version',
  }),
  node('kernel', SCALE_POSITIONS.kernel, {
    kind: 'durable',
    eyebrow: 'Deterministic authority',
    title: 'Workflow kernel',
    detail: 'Owns Instances, dependencies, transitions, waits, and signals.',
    facts: ['Immutable definitions', 'Instance-local serialization'],
    identity: 'instance_id',
  }),
  node('steps', SCALE_POSITIONS.steps, {
    kind: 'durable',
    eyebrow: 'Runnable work',
    title: 'Steps + Attempts',
    detail: 'Separates logical work from each leased execution try.',
    facts: ['AND dependencies', 'Sequential + parallel release'],
    identity: 'step_id + attempt_id',
  }),
  node('dispatcher', SCALE_POSITIONS.dispatcher, {
    kind: 'runtime',
    eyebrow: 'Bounded claims',
    title: 'Dispatcher pool',
    detail: 'Claims only Steps the kernel has already made runnable.',
    facts: ['Tenant-aware admission', 'Global backpressure'],
  }),
  node('executors', SCALE_POSITIONS.executors, {
    kind: 'runtime',
    eyebrow: 'Separate capacity pools',
    title: 'Deterministic + agent',
    detail: 'Runs code or bounded temporary Agents SDK specialists.',
    facts: ['Promote durable children to Steps', 'Temporary DAG stays inside one Step'],
  }),
  node('wait', SCALE_POSITIONS.wait, {
    kind: 'optional',
    eyebrow: 'Durable blocking state',
    title: 'Wait + Signal',
    detail: 'Pauses without occupying a worker and resumes one exact transition.',
    facts: ['Approval or external event', 'No sleeping worker'],
    identity: 'wait_id',
  }),
  node('outbox', SCALE_POSITIONS.outbox, {
    kind: 'durable',
    eyebrow: 'Atomic delivery intent',
    title: 'Transactional outbox',
    detail: 'Records a wake-up beside the authoritative state change.',
    facts: ['Same PostgreSQL transaction', 'Leased relay', 'Separate transport DLQ'],
    identity: 'event_id',
  }),
  node('rabbit', SCALE_POSITIONS.rabbit, {
    kind: 'external',
    eyebrow: 'Wake-up transport',
    title: 'RabbitMQ',
    detail: 'Wakes compatible workers but never owns workflow truth.',
    facts: ['Publisher confirms', 'Prefetch = worker slots', '30s DB safety poll'],
    identity: 'event_id + version + executor_kind',
  }),
  node('delivery', SCALE_POSITIONS.delivery, {
    kind: 'runtime',
    eyebrow: 'Return path',
    title: 'Thread continuation',
    detail: 'Appends an idempotent result to the exact originating Thread.',
    facts: ['Same serialized turn path', 'Visible to UI or SMS'],
    identity: 'thread_id + cause',
  }),
];

const SCALE_EDGES = [
  edge('scale-1', 'client', 'lb', 'call', 'request', 1),
  edge('scale-2', 'lb', 'api', 'call', 'route', 2),
  edge('scale-3', 'api', 'threads', 'commit', 'append message', 3),
  edge('scale-4', 'threads', 'orchestrator', 'claim', 'lease turn', 4),
  edge('scale-5', 'orchestrator', 'command', 'call', 'typed command', 5),
  edge('scale-6', 'command', 'kernel', 'commit', 'start instance', 6),
  edge('scale-7', 'kernel', 'steps', 'commit', 'release steps', 7, {
    sourceHandle: 'bottom',
    targetHandle: 'top',
  }),
  edge('scale-8', 'steps', 'dispatcher', 'claim', 'claim runnable', 8, {
    sourceHandle: 'left',
    targetHandle: 'right',
  }),
  edge('scale-9', 'dispatcher', 'executors', 'call', 'execute step', 9, {
    sourceHandle: 'left',
    targetHandle: 'right',
  }),
  edge('scale-10', 'executors', 'kernel', 'return', 'fenced result', 10, {
    sourceHandle: 'top',
    targetHandle: 'bottom',
  }),
  edge('scale-11', 'kernel', 'wait', 'commit', 'enter wait', 11, {
    sourceHandle: 'bottom',
    targetHandle: 'top',
  }),
  edge('scale-12', 'kernel', 'outbox', 'commit', 'append wake-up', 12, {
    sourceHandle: 'bottom',
    targetHandle: 'top',
  }),
  edge('scale-13', 'outbox', 'rabbit', 'call', 'publish hint', 13, {
    sourceHandle: 'left',
    targetHandle: 'right',
  }),
  edge('scale-14', 'rabbit', 'dispatcher', 'call', 'wake workers', 14, {
    sourceHandle: 'right',
    targetHandle: 'left',
  }),
  edge('scale-15', 'kernel', 'delivery', 'commit', 'result event', 15, {
    sourceHandle: 'bottom',
    targetHandle: 'right',
  }),
  edge('scale-16', 'delivery', 'threads', 'commit', 'append reply', 16, {
    sourceHandle: 'top',
    targetHandle: 'bottom',
  }),
];

const SCENARIOS: Record<ScenarioId, LabScenario> = {
  today: {
    id: 'today',
    label: 'Built today',
    kicker: 'Single-day vertical slice',
    title: 'PostgreSQL is the ledger and the queue',
    summary:
      'Accepted work survives process failure, claims are bounded by tenant and global limits, and stale workers cannot overwrite the winner.',
    proofs: [
      'Crash recovery with leases',
      'Tenant-aware backpressure',
      'Stale-result fencing',
    ],
    nodes: TODAY_NODES,
    edges: TODAY_EDGES,
    steps: [
      {
        title: 'Authenticate the caller',
        narration:
          'The public boundary accepts a message. Tenant and actor identity come from the verified Principal, never from task input.',
        edgeId: 'today-1',
      },
      {
        title: 'Commit before acknowledging',
        narration:
          'The API atomically inserts the task. The row is both the durable ledger and the current queue.',
        edgeId: 'today-2',
      },
      {
        title: 'Claim within capacity',
        narration:
          'Workers use PostgreSQL row locking. Admission stays at 50 outstanding per tenant, 2 active per tenant, and 8 active globally.',
        edgeId: 'today-3',
        attentionNode: 'ledger',
      },
      {
        title: 'Select a trusted executor',
        narration:
          'Executor kind is stored policy. The reasoning model does not choose its own execution authority.',
        edgeId: 'today-4',
      },
      {
        title: 'Run disposable work',
        narration:
          'Synthetic tests prove lifecycle behavior without provider calls. Agent work has the same finite execution envelope.',
        edgeId: 'today-5',
      },
      {
        title: 'Return a typed result',
        narration:
          'The worker returns structured output associated with the exact task lease.',
        edgeId: 'today-6',
      },
      {
        title: 'Fence the final write',
        narration:
          'PostgreSQL accepts completion only from the current owner and lease generation before expiry.',
        edgeId: 'today-7',
      },
      {
        title: 'Recover after process death',
        narration:
          'If a worker dies, its lease expires. A fresh worker gets a higher generation and the stale result is rejected.',
        edgeId: 'today-8',
        attentionNode: 'recovery',
      },
      {
        title: 'Retry without ambiguity',
        narration:
          'The replacement claim keeps the same task identity, increments the attempt, and stops permanently after attempt three.',
        edgeId: 'today-9',
      },
    ],
  },
  workflow: {
    id: 'workflow',
    label: 'Workflow kernel',
    kicker: 'Implemented durable orchestration',
    title: 'Published structure, typed commands, durable transitions',
    summary:
      'The orchestrator chooses approved work, while PostgreSQL owns dependencies, retries, Waits, Signals, and the exact transition that becomes runnable next.',
    proofs: [
      'Immutable versioned definitions',
      'Sequential and parallel Steps',
      'Waits consume zero worker slots',
    ],
    nodes: WORKFLOW_NODES,
    edges: WORKFLOW_EDGES,
    steps: [
      {
        title: 'Accept a business message',
        narration:
          'The public interface authenticates a user message. It never exposes raw task or graph construction.',
        edgeId: 'workflow-1',
      },
      {
        title: 'Persist before acknowledging',
        narration:
          'The API appends the message to a durable Thread, making process restart safe.',
        edgeId: 'workflow-2',
      },
      {
        title: 'Claim one orchestrator turn',
        narration:
          'A disposable worker leases the next Agent Run with a frozen message cutoff.',
        edgeId: 'workflow-3',
      },
      {
        title: 'Submit a typed command',
        narration:
          'The orchestrator selects a published workflow. It cannot invent Steps, dependencies, executor identity, tenant, or actor.',
        edgeId: 'workflow-4',
      },
      {
        title: 'Pin the definition',
        narration:
          'The kernel validates input and creates one Instance pinned to the exact immutable definition version.',
        edgeId: 'workflow-5',
      },
      {
        title: 'Materialize durable structure',
        narration:
          'All Steps and AND dependencies are recorded. Root tasks are queued and dependent tasks remain blocked.',
        edgeId: 'workflow-6',
      },
      {
        title: 'Claim only runnable work',
        narration:
          'Workers use PostgreSQL leases and shared tenant/global capacity. Queue order never controls business order.',
        edgeId: 'workflow-7',
        attentionNode: 'steps',
      },
      {
        title: 'Commit through the kernel',
        narration:
          'The current lease may return one typed result. The Instance lock serializes the short authoritative transition.',
        edgeId: 'workflow-8',
      },
      {
        title: 'Pause without compute',
        narration:
          'A published route can open a Wait. No worker sleeps and the state survives restarts.',
        edgeId: 'workflow-9',
        attentionNode: 'wait',
      },
      {
        title: 'Authenticate the decision',
        narration:
          'A later user turn sends one exact Signal. Tenant, actor, schema, and fresh-user provenance are checked.',
        edgeId: 'workflow-10',
      },
      {
        title: 'Release a predefined route',
        narration:
          'The Signal cannot choose arbitrary work. The pinned definition decides which blocked Step becomes runnable.',
        edgeId: 'workflow-11',
      },
      {
        title: 'Record the winner',
        narration:
          'The result, released Steps, and ordered event commit together. Duplicate or stale completion cannot fork history.',
        edgeId: 'workflow-12',
      },
    ],
  },
  recovery: {
    id: 'recovery',
    label: 'Crash recovery',
    kicker: 'Reviewed issue #28 branch',
    title: 'An approval pause survives a worker restart',
    summary:
      'The first Attempt becomes an immutable snapshot and durable Wait. A valid Signal releases the same logical Step, and only a newer leased Attempt may restore and complete it.',
    proofs: [
      'Real SDK state, provider-free test',
      'Old lease loses authority',
      'Versions and budgets fail closed',
      '174 repository tests green',
    ],
    nodes: RECOVERY_NODES,
    edges: RECOVERY_EDGES,
    steps: [
      {
        title: 'Lease the logical Step',
        narration:
          'Worker 1 claims Attempt 1. Its authority is limited to one owner, one generation, and one expiry.',
        edgeId: 'recovery-1',
      },
      {
        title: 'Run bounded agent coordination',
        narration:
          'The manager uses two specialists as tools. Model requests, child calls, output tokens, and local concurrency all have explicit limits.',
        edgeId: 'recovery-2',
      },
      {
        title: 'Capture the approval interruption',
        narration:
          'The real Agents SDK RunState is serialized with an allowlisted context and no tracing API key. Secret-shaped state fails before persistence.',
        edgeId: 'recovery-3',
      },
      {
        title: 'Pause without holding compute',
        narration:
          'PostgreSQL writes the immutable snapshot, blocks the Step, opens its published Wait, and revokes Attempt 1 in one transaction.',
        edgeId: 'recovery-4',
        attentionNode: 'wait',
      },
      {
        title: 'Authenticate approval',
        narration:
          'The exact approve Signal is the authority. A rejection-shaped or invalid payload leaves the Wait open.',
        edgeId: 'recovery-5',
      },
      {
        title: 'Release the same Step',
        narration:
          'The satisfied Wait makes the existing logical Step runnable again. It does not invent a new route or task.',
        edgeId: 'recovery-6',
      },
      {
        title: 'Create a fresh Attempt',
        narration:
          'Worker 2 claims Attempt 2 with a higher lease generation. Attempt 1 remains permanently unable to commit.',
        edgeId: 'recovery-7',
      },
      {
        title: 'Load only under the new fence',
        narration:
          'The snapshot can be read only while the new lease is current, unexpired, and strictly newer than the suspended Attempt.',
        edgeId: 'recovery-8',
      },
      {
        title: 'Fail closed before restore',
        narration:
          'The codec checks SHA-256, codec version, Agents SDK version, agent-definition version, and exact Workflow IDs.',
        edgeId: 'recovery-9',
      },
      {
        title: 'Resume without resetting cost limits',
        narration:
          'The SDK state is approved and resumed. Persisted model-request and specialist-call usage carry into Attempt 2.',
        edgeId: 'recovery-10',
      },
      {
        title: 'Return one typed result',
        narration:
          'The resumed manager returns the single Step result visible to the deterministic Workflow kernel.',
        edgeId: 'recovery-11',
      },
      {
        title: 'Fence the winning completion',
        narration:
          'Only Attempt 2 can commit. A late result from Attempt 1 is rejected, so the workflow completes exactly once.',
        edgeId: 'recovery-12',
      },
    ],
  },
  scale: {
    id: 'scale',
    label: 'Scale path',
    kicker: 'Ideal architecture',
    title: 'Durable orchestration above bounded execution',
    summary:
      'Stateless APIs, serialized Thread runs, a deterministic workflow kernel, and RabbitMQ wake-ups scale independently while PostgreSQL remains authoritative.',
    proofs: [
      'Outbox removes dual-write loss',
      'RabbitMQ carries hints only',
      'Worker prefetch stays bounded',
      'Modeled idle claims: 960 → 12/min',
    ],
    nodes: SCALE_NODES,
    edges: SCALE_EDGES,
    steps: [
      {
        title: 'Distribute public traffic',
        narration:
          'A load balancer spreads requests across stateless API replicas.',
        edgeId: 'scale-1',
      },
      {
        title: 'Route to a healthy API',
        narration:
          'The API authenticates the channel and resolves the tenant-owned Principal.',
        edgeId: 'scale-2',
      },
      {
        title: 'Persist the message',
        narration:
          'The API appends one idempotent message to its durable Thread before returning.',
        edgeId: 'scale-3',
      },
      {
        title: 'Serialize the conversation',
        narration:
          'One disposable orchestrator claims the coalesced Thread turn at a frozen message cutoff.',
        edgeId: 'scale-4',
      },
      {
        title: 'Choose published work',
        narration:
          'The orchestrator may reply directly, delegate independent work, or select a team-published workflow.',
        edgeId: 'scale-5',
      },
      {
        title: 'Start an immutable workflow',
        narration:
          'A typed Command pins the exact definition version and validated inputs.',
        edgeId: 'scale-6',
      },
      {
        title: 'Release only runnable Steps',
        narration:
          'The kernel checks dependencies. Queue order never decides business order.',
        edgeId: 'scale-7',
      },
      {
        title: 'Claim with backpressure',
        narration:
          'Dispatchers race only for runnable work while shared tenant and global limits remain authoritative.',
        edgeId: 'scale-8',
        attentionNode: 'steps',
      },
      {
        title: 'Use the right capacity pool',
        narration:
          'Deterministic work and bounded reasoning can scale separately without becoming separate state owners.',
        edgeId: 'scale-9',
      },
      {
        title: 'Commit one transition',
        narration:
          'A fenced result returns to the kernel, which releases successors, completes, or enters a durable Wait.',
        edgeId: 'scale-10',
      },
      {
        title: 'Wait without a worker',
        narration:
          'Approvals and external events occupy storage, not compute. One exact Signal resumes a predefined route.',
        edgeId: 'scale-11',
        attentionNode: 'wait',
      },
      {
        title: 'Write the outbox atomically',
        narration:
          'The state transition and wake-up intent commit in one PostgreSQL transaction.',
        edgeId: 'scale-12',
      },
      {
        title: 'Publish a wake-up hint',
        narration:
          'A relay publishes with confirmation. Duplicate hints are safe because claims still happen in PostgreSQL.',
        edgeId: 'scale-13',
      },
      {
        title: 'Wake compatible workers',
        narration:
          'RabbitMQ reduces idle polling. With 8 slots across 4 processes, 4 tasks, and a 30 second safety poll, the deterministic model drops claim calls from 960 to 12 per minute. PostgreSQL still owns every lease.',
        edgeId: 'scale-14',
      },
      {
        title: 'Create a durable continuation',
        narration:
          'Completion produces a result addressed to the exact originating Thread.',
        edgeId: 'scale-15',
      },
      {
        title: 'Deliver through the same Thread',
        narration:
          'The reply is appended idempotently and becomes visible through web, SMS, or another verified channel.',
        edgeId: 'scale-16',
      },
    ],
  },
};

const buttonClass =
  'inline-flex h-9 items-center justify-center gap-2 rounded-lg border border-slate-200 bg-white px-3 text-xs font-semibold text-slate-700 shadow-sm transition hover:border-slate-300 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40';

function cloneNodes(nodes: SystemCanvasNode[]) {
  return nodes.map((item) => ({
    ...item,
    position: { ...item.position },
    data: { ...item.data },
  }));
}

export function OpenPokeSystemLab() {
  const [scenarioId, setScenarioId] = useState<ScenarioId>('today');
  const [stepIndex, setStepIndex] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>('flowing');
  const [traceVisible, setTraceVisible] = useState(true);
  const [locked, setLocked] = useState(true);
  const [burstSize, setBurstSize] = useState(50);
  const [flow, setFlow] =
    useState<ReactFlowInstance<SystemCanvasNode, SystemCanvasEdge> | null>(null);

  const scenario = SCENARIOS[scenarioId];
  const currentStep = scenario.steps[stepIndex];
  const [nodes, setNodes] = useState(() => cloneNodes(scenario.nodes));

  const organize = useCallback(() => {
    setNodes(cloneNodes(SCENARIOS[scenarioId].nodes));
    window.setTimeout(() => flow?.fitView({ padding: 0.16, duration: 450 }), 20);
  }, [flow, scenarioId]);

  useEffect(() => {
    setNodes(cloneNodes(scenario.nodes));
    setStepIndex(0);
    setPlaying(false);
    window.setTimeout(() => flow?.fitView({ padding: 0.16, duration: 450 }), 20);
  }, [flow, scenario]);

  useEffect(() => {
    if (!playing) return;
    const timer = window.setInterval(() => {
      setStepIndex((current) => {
        if (current >= scenario.steps.length - 1) {
          setPlaying(false);
          return current;
        }
        return current + 1;
      });
    }, 1800);
    return () => window.clearInterval(timer);
  }, [playing, scenario.steps.length]);

  useEffect(() => {
    window.setTimeout(() => flow?.fitView({ padding: 0.2, duration: 350 }), 20);
  }, [flow, scenarioId, stepIndex, viewMode]);

  const renderedNodes = useMemo(
    () => {
      const activeEdge = scenario.edges.find(
        (candidate) => candidate.id === currentStep.edgeId,
      );
      const candidates =
        viewMode === 'flowing'
          ? nodes.filter(
              (item) =>
                item.id === activeEdge?.source || item.id === activeEdge?.target,
            )
          : nodes;
      return candidates.map((item, index) => {
        const participates =
          item.id === activeEdge?.source || item.id === activeEdge?.target;
        const state =
          item.id === currentStep.attentionNode
            ? 'attention'
            : participates
              ? 'active'
              : 'idle';
        const data = { ...item.data, state } as SystemNodeData;
        if (item.id === 'ledger') {
          const admitted = Math.min(burstSize, 50);
          data.facts = [
            `${admitted}/${burstSize} admitted`,
            '2 active / tenant',
            '8 active globally',
          ];
        }
        return {
          ...item,
          position:
            viewMode === 'flowing'
              ? { x: index * 360, y: 0 }
              : item.position,
          data,
        };
      });
    },
    [burstSize, currentStep, nodes, scenario.edges, viewMode],
  );

  const renderedEdges = useMemo(() => {
    const candidates =
      viewMode === 'flowing'
        ? scenario.edges.filter((item) => item.id === currentStep.edgeId)
        : scenario.edges;
    return candidates.map((item) => ({
      ...item,
      data: {
        ...item.data!,
        active: item.id === currentStep.edgeId,
      },
    }));
  }, [currentStep.edgeId, scenario.edges, viewMode]);

  const onNodesChange = useCallback(
    (changes: NodeChange<SystemCanvasNode>[]) => {
      setNodes((current) => applyNodeChanges(changes, current));
    },
    [],
  );

  const changeScenario = (next: ScenarioId) => {
    if (next === scenarioId) return;
    setScenarioId(next);
  };

  const previous = () => {
    setPlaying(false);
    setStepIndex((current) => Math.max(0, current - 1));
  };

  const next = () => {
    setPlaying(false);
    setStepIndex((current) =>
      Math.min(scenario.steps.length - 1, current + 1),
    );
  };

  return (
    <main className="min-h-screen bg-[#f4f7fb] text-slate-950">
      <section className="border-b border-slate-200 bg-white px-5 py-5 lg:px-8">
        <div className="mx-auto flex max-w-[1800px] flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <div className="mb-2 flex items-center gap-2 text-[11px] font-bold uppercase tracking-[0.2em] text-blue-700">
              <span className="rounded-full bg-blue-50 px-2 py-1">
                Presentation mode
              </span>
              <span className="text-slate-400">Deterministic scenario data</span>
            </div>
            <p className="text-sm font-semibold text-slate-500">
              {scenario.kicker}
            </p>
            <h1 className="mt-1 text-2xl font-bold tracking-tight sm:text-3xl">
              {scenario.title}
            </h1>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
              {scenario.summary}
            </p>
            <div className="mt-3 flex flex-wrap gap-2">
              {scenario.proofs.map((proof) => (
                <span
                  key={proof}
                  className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-[11px] font-semibold text-emerald-800"
                >
                  {proof}
                </span>
              ))}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            {(Object.values(SCENARIOS) as LabScenario[]).map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() => changeScenario(item.id)}
                className={`${buttonClass} ${
                  item.id === scenarioId
                    ? '!border-blue-600 !bg-blue-600 !text-white hover:!bg-blue-700'
                    : ''
                }`}
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-[1800px] px-4 py-4 lg:px-8">
        <div className="mb-3 flex flex-col gap-3 rounded-xl border border-slate-200 bg-white p-3 shadow-sm xl:flex-row xl:items-center xl:justify-between">
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              className={buttonClass}
              onClick={() => setPlaying((current) => !current)}
            >
              {playing ? <Pause size={14} /> : <Play size={14} />}
              {playing ? 'Pause' : 'Play'}
            </button>
            <button
              type="button"
              aria-label="Previous transition"
              className={buttonClass}
              onClick={previous}
              disabled={stepIndex === 0}
            >
              <ChevronLeft size={14} />
            </button>
            <button
              type="button"
              aria-label="Next transition"
              className={buttonClass}
              onClick={next}
              disabled={stepIndex === scenario.steps.length - 1}
            >
              <ChevronRight size={14} />
            </button>
            <span className="rounded-full bg-slate-100 px-3 py-2 font-mono text-[11px] text-slate-600">
              {stepIndex + 1}/{scenario.steps.length}
            </span>
            <div className="ml-1 flex rounded-lg bg-slate-100 p-1">
              <button
                type="button"
                className={`rounded-md px-3 py-1.5 text-xs font-semibold ${
                  viewMode === 'flowing'
                    ? 'bg-white text-slate-900 shadow-sm'
                    : 'text-slate-500'
                }`}
                onClick={() => setViewMode('flowing')}
              >
                Flowing now
              </button>
              <button
                type="button"
                className={`rounded-md px-3 py-1.5 text-xs font-semibold ${
                  viewMode === 'full'
                    ? 'bg-white text-slate-900 shadow-sm'
                    : 'text-slate-500'
                }`}
                onClick={() => setViewMode('full')}
              >
                Full path
              </button>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            {scenarioId === 'today' ? (
              <label className="flex items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 text-xs font-semibold text-slate-600">
                Burst
                <input
                  aria-label="Burst size"
                  type="range"
                  min="10"
                  max="100"
                  step="10"
                  value={burstSize}
                  onChange={(event) => setBurstSize(Number(event.target.value))}
                  className="w-24 accent-blue-600"
                />
                <span className="w-7 font-mono">{burstSize}</span>
              </label>
            ) : null}
            <button
              type="button"
              className={buttonClass}
              onClick={() => setTraceVisible((current) => !current)}
            >
              {traceVisible ? <EyeOff size={14} /> : <Eye size={14} />}
              Trace
            </button>
            <button
              type="button"
              className={buttonClass}
              onClick={() => setLocked((current) => !current)}
            >
              {locked ? <Lock size={14} /> : <Unlock size={14} />}
              {locked ? 'Locked' : 'Movable'}
            </button>
            <button type="button" className={buttonClass} onClick={organize}>
              <RotateCcw size={14} />
              Organize
            </button>
            <button
              type="button"
              className={buttonClass}
              onClick={() => flow?.fitView({ padding: 0.16, duration: 450 })}
            >
              <Maximize2 size={14} />
              Fit
            </button>
          </div>
        </div>

        <div
          className={`grid min-h-[690px] gap-3 ${
            traceVisible ? 'xl:grid-cols-[minmax(0,1fr)_330px]' : ''
          }`}
        >
          <div className="systems-lab-canvas relative min-h-[690px] overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="pointer-events-none absolute left-4 top-4 z-20 max-w-md rounded-xl border border-slate-200 bg-white/95 p-3 shadow-sm backdrop-blur">
              <div className="flex items-center gap-2">
                <span className="flex size-7 items-center justify-center rounded-full bg-blue-600 text-xs font-bold text-white">
                  {stepIndex + 1}
                </span>
                <p className="text-sm font-bold">{currentStep.title}</p>
              </div>
              <p className="mt-2 text-xs leading-5 text-slate-600">
                {currentStep.narration}
              </p>
            </div>

            <ReactFlow<SystemCanvasNode, SystemCanvasEdge>
              nodes={renderedNodes}
              edges={renderedEdges}
              nodeTypes={systemNodeTypes}
              edgeTypes={systemEdgeTypes}
              onNodesChange={onNodesChange}
              onInit={setFlow}
              nodesDraggable={!locked}
              nodesConnectable={false}
              elementsSelectable={!locked}
              panOnDrag
              zoomOnScroll
              fitView
              fitViewOptions={{ padding: 0.16 }}
              minZoom={0.25}
              maxZoom={1.4}
              proOptions={{ hideAttribution: true }}
            >
              <Background
                variant={BackgroundVariant.Dots}
                gap={22}
                size={1.2}
                color="#d8e0ea"
              />
            </ReactFlow>

            <div className="pointer-events-none absolute bottom-4 left-4 z-20 flex items-center gap-2 rounded-full border border-slate-200 bg-white/95 px-3 py-2 text-[11px] font-semibold text-slate-500 shadow-sm">
              <Focus size={13} />
              Drag to pan, scroll to zoom
            </div>
          </div>

          {traceVisible ? (
            <aside className="max-h-[690px] overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
              <div className="border-b border-slate-200 p-4">
                <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-400">
                  Ordered trace
                </p>
                <h2 className="mt-1 text-lg font-bold">Narrate the system</h2>
                <p className="mt-1 text-xs leading-5 text-slate-500">
                  Select any transition. Only the active edge moves.
                </p>
              </div>
              <ol className="h-[594px] overflow-y-auto p-2">
                {scenario.steps.map((item, index) => (
                  <li key={item.edgeId}>
                    <button
                      type="button"
                      onClick={() => {
                        setPlaying(false);
                        setStepIndex(index);
                      }}
                      className={`flex w-full gap-3 rounded-xl p-3 text-left transition ${
                        index === stepIndex
                          ? 'bg-blue-50 text-blue-950'
                          : 'hover:bg-slate-50'
                      }`}
                    >
                      <span
                        className={`mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-full text-[10px] font-bold ${
                          index === stepIndex
                            ? 'bg-blue-600 text-white'
                            : index < stepIndex
                              ? 'bg-emerald-100 text-emerald-800'
                              : 'bg-slate-100 text-slate-500'
                        }`}
                      >
                        {index + 1}
                      </span>
                      <span>
                        <span className="block text-xs font-bold">
                          {item.title}
                        </span>
                        <span className="mt-1 block text-[11px] leading-4 text-slate-500">
                          {item.narration}
                        </span>
                      </span>
                    </button>
                  </li>
                ))}
              </ol>
            </aside>
          ) : null}
        </div>
      </section>
    </main>
  );
}
