# wombat-go

A ReAct agent harness in Go, with no third-party dependencies.

It is a rewrite of a private OCaml 5 project of mine that is built on algebraic
effects.

The OCaml version's thesis is that every side effect — LLM calls, tool
dispatch, file I/O, logging, time, human input — should be an algebraic effect,
so that the handler stack installed around the agent decides what actually
happens. This version does not translate that. It asks what the same
properties look like when Go writes them.

The answer is: mostly, they were dependency injection.

```go
a, err := wombat.New(
    wombat.WithClient(anthropic.New(anthropic.Config{Model: "claude-opus-5"})),
    wombat.WithTools(builtin.Default(builtin.Deps{})...),
)

ctx, cancel := governor.WithBudget(ctx, governor.Limits{CostUSD: 1.00})
defer cancel()

run := a.Start(ctx, wombat.Ask("what does this repo do?"))
defer run.Close()

for run.Next() {
    switch ev := run.Event().(type) {
    case wombat.TextDelta: os.Stdout.WriteString(ev.Text)
    case wombat.ToolStart: log.Println("→", ev.Name)
    }
}
if err := run.Err(); err != nil { return err }
```

## Try it

```
git clone https://github.com/automanfromm87/wombat-go && cd wombat-go
cp .env.example .env          # base URL, model, key
go build -o bin/ ./cmd/...

scripts/wombat "how many Go files are in this repo? use bash"
```

`scripts/wombat` is `wombat-jsonl | scripts/pretty.py` — the binary speaks
JSONL and nothing else, so the renderer is forty lines of Python and any other
front end is equally cheap. Add `-r` to watch the model think.

Multi-turn, including across process boundaries:

```
scripts/wombat -session /tmp/s.json "remember the number 8317"
scripts/wombat -session /tmp/s.json "what was it times three?"
```

If the first turn stopped on `ask_user`, the second turn's text is the *answer*
to that question rather than a new instruction.

## Three decisions

**A tool takes a context and its arguments. Nothing else.**

```go
type Fn func(ctx context.Context, in json.RawMessage) (string, error)
```

Whatever a tool needs is captured when it is constructed — `ViewFile(fsys)`,
`Bash(runner)`, `HTTPGet(client)`. Eight of the OCaml's thirteen effects exist
only because an OCaml tool handler was `Yojson.t -> result` and had no other
route to the outside world. Give the closure the dependency and they are not
needed. A tool is then an ordinary function: testing one requires no harness.

**A run is a stream of events, not a value plus four callbacks.**

The OCaml harness returns an outcome and emits through `on_log`,
`on_text_delta`, `on_event` and `on_tool_event`; a 473-line sidecar exists to
sew those back into one JSONL stream. Here the stream is the primary API and
the sidecar is a loop and an encoder. Four callbacks were never a design — they
were the shape of not having a stream type.

Diagnostics are separate: they go to `log/slog`. The event stream carries
semantic events a front end renders.

**A budget is a context.**

```go
ctx, cancel := governor.WithBudget(ctx, governor.Limits{CostUSD: 1.00, Wall: 10*time.Minute})
```

Exceeding a limit cancels the context. Every blocking operation in the process
— the HTTP request, `exec.CommandContext`, a channel select — already unwinds
on its own, so aborting from arbitrary depth costs zero new return values.
`context.Cause(ctx)` reports which limit tripped. The OCaml equivalent raises
through the whole stack from inside an effect handler and needs care at every
`try` that might swallow it.

## Talking to a gateway

Corporate deployments put the model behind a proxy, a routing header and a
non-default base URL. `ConfigFromEnv` reads all of it and hands back a plain
`Config`, so anything the environment got wrong is fixable before `New` sees
it:

```go
cfg := openai.ConfigFromEnv()   // OPENAI_BASE_URL, OPENAI_PROXY, OPENAI_EXTRA_HEADERS, …
cfg.Stream = llm.StreamNever    // if this gateway drops usage while streaming
client, err := openai.New(cfg)
```

```
cp .env.example .env      # fill in a base URL and a key
scripts/wombat "what does this repo do?"
```

Two things a real gateway teaches you that a mock does not:

- **Reasoning models stream a second channel.** `reasoning_content` (OpenAI
  compat) and `thinking_delta` (Anthropic) both arrive as
  `llm.Delta{Reasoning: …}` and surface as `ReasoningDelta` events, kept
  separate from the answer. On some models it is most of the generated tokens,
  so a UI that only renders `TextDelta` looks frozen.
- **Streaming and accounting can conflict.** Usage rides on a trailing chunk;
  some servers omit it, and then a streamed call reports zero tokens and cost
  budgeting quietly stops working. `llm.StreamNever` trades deltas for
  accounting when that happens.

## Context has structure, and the structure is the point

Three layers, each structured differently on purpose:

| | shape | why |
|---|---|---|
| conversation | typed blocks: `Text` / `ToolUse` / `ToolResult` / `Thinking` | lets eviction be surgical |
| system prompt | `[]block` at construction, one frozen string afterwards | the cache prefix must be byte-stable, so nothing may rebuild it |
| what the harness knows | `View.Results`, keyed by tool_use id | a strategy that sees only messages can trim by position and nothing else |

That third row is what makes the rest work. Two `ToolResult` blocks have the
same type whether one is nine kilobytes of loaded skill and the other is a
line of grep output. A tool labels its own observation:

```go
tool.Annotate(ctx, skill.Tag, skill.TagPrefix+name)
```

and a strategy acts on the label:

```go
wombat.DropTagged(40, 12, skill.Tag)   // evict skill bodies, keep everything else
```

`DropTagged` removes whole `tool_use`/`tool_result` **pairs**, not a prefix of
the transcript. It reclaims exactly the nine kilobytes and leaves the reasoning
around them intact — only possible because content is a typed block list.

## Skills

Discovery is static, load and unload are dynamic:

```go
skills, _ := skill.LoadDir("./skills", func(err error) { log.Print(err) })
reg := skill.New(skills...)
reg.Gate("git-history", "git_log")     // hidden until the skill is loaded
g := reg.Bind(builtin.Default(deps))

a, _ := wombat.New(
    wombat.WithClient(client),
    wombat.WithToolSet(g.Set),                              // gated visibility
    wombat.WithToolMiddleware(g.Middleware),                // gate enforcement
    wombat.WithSystemBlock("available_skills", g.Index),    // the catalogue
    wombat.WithRunContext(func(ctx context.Context) context.Context {
        return skill.WithState(ctx, skill.NewState())       // per-run activation
    }),
)
```

Live, against a real model: turn 1 offers 12 tools, the model reads the
catalogue and calls `load_skill("git-history")`, turn 2 offers 14.

Four things that are easy to get wrong and are handled:

- **Activation is per run, not per agent.** An `Agent` is immutable and shared
  across goroutines; activation state is born in `WithRunContext` so two
  concurrent runs cannot see each other's skills.
- **Visibility hides; middleware enforces.** Dropping a tool from the list does
  not stop a model that saw the name earlier in the transcript from calling it.
  `Set.Find` therefore stays unfiltered, so the refusal can say *call
  `load_skill("git-history")` first* instead of *unknown tool*.
- **Eviction retires the skill.** A skill's body enters the conversation as a
  tool_result. When a strategy drops it, `tool.Reconciler` tells the set, and
  the gated tools go with it — otherwise the model keeps tools whose knowledge
  left the context.
- **The catalogue is sorted.** It lands in the system prompt, so map iteration
  order would change the bytes between processes and silently kill the cache.

Unloading retires tools, not tokens: the body stays in the transcript until a
strategy evicts it. Those are two mechanisms and it is worth knowing which one
you are reaching for.

## Sub-agents

A delegate tool runs a whole child agent, on the parent's context:

```go
child, _ := wombat.New(wombat.WithClient(client), wombat.WithName("researcher"),
    wombat.WithTools(tool.Filter(all, tool.OnlyCaps(tool.CapReadOnly))...))

parent, _ := wombat.New(...,
    wombat.WithTools(append(all, wombat.DelegateTool(child))...),
    wombat.WithToolTimeoutFallback(0),   // a child's bound is its own iteration cap
)
```

Running on the parent's context is the payoff of the whole design: the budget,
the skill activation set, the tape and the tracer all reach the child by being
values on `ctx`. Nothing forks, nothing is reinstalled. The child's events reach
the parent's stream wrapped in `SubagentEvent`, so a UI can nest them.

The depth cap is the one governor limit that **refuses instead of aborting**.
Every other cap is a resource the run has exhausted; nesting depth is a shape,
and a parent told "not deeper" can still do the job itself — but only if it is
still running.

## Fault tolerance

| layer | what it does |
|---|---|
| `WithRecovery` | innermost: a panicking tool becomes an error the model reads, and trips the breaker exactly like any other failure |
| dispatcher backstop | catches a panic in a *middleware*, which under `WithParallelism` would otherwise die on a worker goroutine |
| loop recover | a panic in a strategy or a client becomes `ErrPanic`, not a dead process |
| `WithTimeout` | a context deadline, so a tool that honours `ctx` is actually stopped — not abandoned |
| `WithRetry` | needs `Idempotent` **and** `Retryable`; a write is never replayed because an error looked transient |
| `WithCircuitBreaker` | per tool, **process-wide** — a dependency that is down is down for every run |
| `WithDedupRepeats` | per **run** — being stuck in a loop is a property of one conversation |
| `WithOverflowRecovery` | the one error the harness can actually fix: shrink the transcript and retry, sticky for the rest of the run |

The model sees a short message; the stack goes to `slog`.

## Recording and replay

```go
tp, _ := tape.Open("run.jsonl", tape.Auto)
defer tp.Close()
client = llm.Chain(client, tp.LLM())
opts = append(opts, wombat.WithToolMiddleware(tp.Tools()))
```

Entries are keyed by a **content hash of the request**, not by position. The
OCaml original keyed by position and died with `tape misalignment` the moment a
run diverged — which, for a differential-testing tool, is exactly when you need
it. A miss in `Auto` is a live call.

Measured on a real run: record took minutes against a gateway, replay took
**0.107 s with the endpoint pointed at a dead address**, and the answer was
byte-identical. Deltas coalesce to one per block on replay, and a replayed call
charges no budget — both documented, neither hidden.

## Tracing

```go
sink, closer, _ := trace.FileSink("run.ndjson")
defer closer.Close()
a, _ := wombat.New(..., wombat.WithTracing(trace.New(sink)))
```

```
run        wombat                 26632ms
  iteration  wombat iteration 1   21375ms
    llm        other               2819ms
    tool       delegate           18540ms
      subagent   delegate         18539ms
        run        researcher     18531ms
          iteration  …               900ms
            llm        subagent      814ms
            tool       git_log        77ms
```

That nesting is free: a goroutine inherits `ctx` and therefore its parent span.
The OCaml needed `Domain.DLS` plus an explicit `Trace.fork`, which does not
exist here.

Attribute keys are OpenTelemetry `gen_ai.*` semconv verbatim, so bridging to a
real `TracerProvider` is about thirty lines in your own module — the dependency
is yours to add, not this library's. `trace.WriteHTML` renders a self-contained
report with no external assets, and neither middleware records message content:
traces get shipped to places transcripts should not go.

## Permission

A tool call can be allowed, denied, or put in front of a person. The gate is a
`tool.Middleware`, so it sees the same call the tool would have seen — the
command string, the path, the arguments — and decides with all of it in hand.

```go
gate := permission.Gate(permission.Workspace("/work/proj"), approver)
```

Three things this got right only after being wrong first.

**A rooted filesystem is not a sandbox.** An early version confined the file
tools to a prefix and left `bash` alone; a model refused `view_file /etc/hosts`
and read the same file with `cat` on the next turn. The flag was renamed to say
what it does, and the honest boundary is `permission`, which has an opinion
about the shell too.

**An allowlist that never fires is not a safety feature.** `SafeCommands` skips
the approval for `go build`, `go test`, `ls`, `cat` and about thirty others so
that nobody is asked twenty times in an afternoon. It originally refused any
command containing a shell metacharacter, which is the safe reading and also
every command a model writes — in a live run an agent could not test the code
it had just written, because it wrote `go test ./... 2>&1 | head -n 300`. A
pipeline is now admitted, with *every stage checked independently*: that is
strictly stronger than a prefix match over the whole string, which never
examined the dangerous half of `go test; rm -rf /` at all.

**A denial has to teach.** When the same agent was refused, the message said
"try an approach the policy allows outright" and it hit the same wall three
times. It now names the commands and the constraint.

## Evaluation

`rl/` turns a run into an `Episode`: a trajectory with a reward. `Rollout` runs
n samples of an `Env` and reports **pass@k** with the unbiased estimator from
Chen et al. rather than "did any of n succeed", which overstates every number
it produces.

`benchmarks/` is eight coding tasks defined as data, in two tiers. The easy
tier is the floor — a live model scores 4/4, and when it does not the harness
is broken rather than the agent. The hard tier discriminates: files the prompt
never names, a bug two hops from its symptom, a genuinely underspecified
request, and a haystack that does not fit in the window.

The interesting part is the anti-cheat. `GoProbe` writes a test into a *private
copy* of the workspace after the agent has stopped, from bytes it never had
access to, and it closes two freebies a naive scorer hands out: `go test ./...`
exits 0 in a module whose packages were all deleted, and a `-run` pattern
matching no test also exits 0.

```
$ wombat-bench -tasks hard -n 4
TASK                N  PASS@1  PASS@K   MEAN    STD  TURNS  TOK/IN  TOK/OUT  COST
refactor-interface  4   0.000   0.000  0.250  0.433   30.0  423.1k    67.3k   n/a

unpriced: some-gateway-model — no rate for the above, so COST reads n/a

solved but did not finish (1): full reward, and the run still failed —
the agent did the work and could not tell it was done. Not counted as a pass.
  refactor-interface#2  reward 1.000  max_iterations
```

That last block exists because the first version of this report said pass@1
0.000 for a task one sample had solved outright, and the only trace of it was a
standard deviation. "This agent cannot do the task" and "this agent cannot tell
when it is done" are different bugs with different fixes, and they rendered
identically.

## Over HTTP

`httpapi/` is the same agent behind Server-Sent Events. `cmd/wombat-serve`
mounts it with a single-file browser client at `/`:

```
cp .env.example .env
go build -o bin/ ./cmd/...
set -a; . ./.env; set +a
bin/wombat-serve -addr :8080 -working-dir /tmp/work -permission workspace
```

Everything below is JSON in and JSON out, and every field name is real —
`cmd/wombat-tsgen` reflects over the Go event registry to generate
`web/events.ts`, so a TypeScript client cannot drift from the server.

### Endpoints

| | |
|---|---|
| `GET /api/health` | `{"status":"ok","version":"3881238e","uptime_sec":12.3}` |
| `GET /api/config` | what this server can do — see below |
| `POST /api/sessions` | create a session **and run its first turn** |
| `GET /api/sessions` | list |
| `GET /api/sessions/{id}` | one session's state |
| `DELETE /api/sessions/{id}` | cancel an in-flight turn and drop it |
| `POST /api/sessions/{id}/messages` | send the next turn |
| `GET /api/sessions/{id}/messages` | the transcript, as `[]llm.Message` |
| `GET /api/sessions/{id}/events` | **SSE**, honours `Last-Event-ID` |
| `GET /api/sessions/{id}/approvals` | what is waiting on a human |
| `POST /api/sessions/{id}/approvals/{use_id}` | `{"allow":true}` → 204 |
| `GET /metrics` | Prometheus text (unless `-metrics=false`) |

Creating a session and running its first turn is **one call**, not two: a
session with no turn in it is a state with no use, and a client that crashed
between the two calls would leave a paid-for slot occupied by nothing.

```jsonc
// POST /api/sessions
{
  "prompt": "what does this repo do?",     // required
  "model": "…",                            // optional, overrides the server default
  "permission": "off|readonly|workspace|ask",  // may only be STRICTER than the server's
  "workspace": "subdir",                   // must resolve inside -working-dir
  "max_iters": 20,                         // may only be LOWER than the server's cap
  "title": "…"
}
// 201 Created, Location: /api/sessions/{id}
{
  "id": "8a6f9a81f18c00d5", "title": "", "state": "running", "turns": 1,
  "events": 1, "created": "…", "updated": "…", "options": {…},
  "spend": {"cost_usd":0,"input_tokens":0,"output_tokens":0,
            "cache_read_tokens":0,"calls":0,"elapsed_sec":0}
}
```

`state` is one of `idle`, `running`, `waiting` (the model called a pause tool
and wants an answer), `approving` (a tool call is parked on a human),
`done`, `error`.

### The event stream

```
GET /api/sessions/{id}/events
Last-Event-ID: 42          # optional; replay resumes at 43
```

```
id: 3
event: reasoning_delta
data: {"type":"reasoning_delta","text":"need the calculator"}

id: 11
event: tool_done
data: {"type":"tool_done","use_id":"call_01…","name":"calculator","ok":true,"output":"42","ms":0}

id: 12
event: turn_ended
data: {"type":"turn_ended","turn":1,"state":"idle","outcome":"answer",
       "answer":"6 × 7 equals 42.","spend":{…}}
```

The `id:` is **session-global, not per turn.** That is the whole reason a UI
can reconnect: a per-turn counter would resume a client that dropped during
turn 3 at the start of turn 4, silently losing everything it missed.

Event `type`s, all generated into `web/events.ts`: `turn_started`,
`iter_start`, `llm_start`, `reasoning_delta`, `text_delta`,
`tool_args_delta`, `tool_start`, `tool_done`, `permission_requested`,
`permission_decided`, `subagent_start`, `subagent_event`, `subagent_end`,
`spend`, `llm_done`, `turn_ended`. A `: ping` comment keeps the connection
open through an idle proxy.

Render `text_delta` **and** `reasoning_delta`. On a reasoning model the
scratchpad is most of the generated tokens, so a UI that only renders the
answer looks frozen for minutes.

### Approvals

Under `-permission workspace`, exec and anything outside the working directory
stop for a person. The turn does not fail — it parks, `state` becomes
`approving`, and a `permission_requested` event names the call:

```jsonc
{"type":"permission_requested","use_id":"call_01…","tool":"bash",
 "input":{"command":"rm -rf build","exec_dir":"/tmp/work"},
 "reason":"bash is an exec tool, which this policy always confirms with a person"}
```

```
POST /api/sessions/{id}/approvals/call_01…
{"allow": false}          → 204 No Content
```

`allow` is a required tri-state: omitting it is a 400 rather than a silent
deny, because "the client forgot the field" and "the human said no" must not
look the same.

### Errors

Always `{"error":{"kind":"…","message":"…"}}`, with `kind` from a closed set a
client can switch on: `bad_request`, `body_too_large`, `not_found`,
`no_such_session`, `no_such_approval`, `busy` (a turn is already running),
`done`, `already_answered`, `too_many_sessions`, `closed`, `internal`.
Never a bare string — a front end that has to pattern-match on prose breaks
the first time a message is reworded.

### Trying it

```bash
SID=$(curl -s -X POST localhost:8080/api/sessions -H 'content-type: application/json' \
  -d '{"prompt":"What is 6*7? Use the calculator, then answer in one sentence."}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -N localhost:8080/api/sessions/$SID/events    # watch it work

curl -s -X POST localhost:8080/api/sessions/$SID/messages \
  -H 'content-type: application/json' -d '{"prompt":"and times three?"}'
```

`-cors` is empty by default, which is right when the bundled UI is served from
this same origin. There is **no authentication**: `-cors '*'` lets any page the
operator visits drive this agent with the operator's network access and file
system. Put it behind something before it leaves localhost.

## What is kept

The middleware chains, which are the best part of the original and translate
exactly:

```go
client := llm.Chain(leaf,
    llm.WithValidation,
    wombat.TrackCost(llm.DefaultPricing),
    llm.WithRetry(llm.DefaultRetryPolicy),   // cost is charged per attempt
    llm.WithLogging(logger),
)
```

Order is semantic — chaos outside retry, tracing outside chaos, observation
outermost so one logical call yields one event pair. Unlike a stack of effect
handlers, position here cannot cause an inner layer to be silently shadowed.

`Pause` and `Terminal` were effects that abandoned their continuation. They are
outcomes:

```go
switch o := run.Outcome().(type) {
case wombat.Answer:    // the model ended its turn
case wombat.Paused:    // it called a CapPause tool; answer and resume
case wombat.Submitted: // it called a CapTerminal tool; the args are the result
}
```

Failure is `run.Err()`, matched with `errors.Is`. One failure channel, not two.

## What is lost

- Exhaustive matching over `Event` and `Outcome`. A linter, not a compiler.
- Illegal transcripts being unrepresentable. `Convo` is an append-only slice
  validated at the boundary, because Go cannot express the OCaml version's
  abstract type without a package per invariant.
- Installing a handler from outside to change an inner leaf's behavior. The
  OCaml documents this and never uses it.
- The effect system itself, which is the original project's reason to exist.

## Layout

```
.                     Agent, Run, Event, Outcome, Convo, Strategy   ← the front door
llm/                  wire vocabulary, Client, middleware, pricing
llm/anthropic/        Messages API client (net/http, no curl subprocess)
llm/openai/           OpenAI-compatible client
tool/                 Def, Set, Typed, dispatch, middleware, fault attribution
tool/builtin/         the twelve built-in tools
skill/                progressive disclosure: tools that appear when a skill loads
permission/           per-call authorization, with a human in the loop
governor/             budgets as context cancellation
tape/                 content-hash record and replay
trace/                OTel-shaped spans, NDJSON and a self-contained HTML report
metric/               Prometheus text exposition, hand-written
httpapi/              resumable multi-turn sessions over SSE
rl/                   episodes, verifiers, pass@k
benchmarks/           eight coding tasks as data
web/                  TypeScript event types, generated from the Go registry
cmd/                  wombat-jsonl, -serve, -bench, -trace-html, -tsgen
```

Standard library only — `go.mod` has no `require` block, and there is nothing
to vendor.
