// Command wombat-jsonl runs one agent turn and writes a JSON-Lines event
// stream on stdout.
//
// It is the sidecar a UI front end spawns. The interesting thing about it is
// how little there is: the harness already exposes a stream of events, so the
// adapter is a loop and an encoder.
//
//	for run.Next() { enc.Encode(run.Event()) }
//
// In the OCaml original the harness returned a value and emitted four separate
// observer callbacks, and 473 lines of sidecar existed to sew them back into
// one stream. Making the stream the primary API deletes that job rather than
// reimplementing it.
//
// Contract:
//   - stdout is JSONL and nothing else, one object per line, each with a
//     "type" discriminator. A front end can read it with a line reader and no
//     state machine.
//   - stderr carries diagnostics (slog).
//   - exit 0 on a completed turn (including a pause), 1 on failure,
//     2 on bad usage.
//
// That contract is also why -permission behaves the way it does here. A policy
// can allow and it can deny, but it cannot ask: there is no console to prompt
// on, and stdout belongs to the protocol. An Ask is therefore refused, with an
// explanation the model can read. See the flag's help.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	wombat "github.com/automanfromm87/wombat-go"
	"github.com/automanfromm87/wombat-go/governor"
	"github.com/automanfromm87/wombat-go/llm"
	"github.com/automanfromm87/wombat-go/llm/anthropic"
	"github.com/automanfromm87/wombat-go/llm/openai"
	"github.com/automanfromm87/wombat-go/metric"
	"github.com/automanfromm87/wombat-go/permission"
	"github.com/automanfromm87/wombat-go/skill"
	"github.com/automanfromm87/wombat-go/tape"
	"github.com/automanfromm87/wombat-go/tool"
	"github.com/automanfromm87/wombat-go/tool/builtin"
	"github.com/automanfromm87/wombat-go/trace"
)

const (
	exitOK    = 0
	exitFail  = 1
	exitUsage = 2
)

func main() {
	// All work happens in run so that deferred cleanup executes before exit.
	os.Exit(run())
}

type options struct {
	provider    string
	stream      string
	model       string
	system      string
	workdir     string
	fsRoot      string
	sandbox     string // deprecated alias for fsRoot
	permission  string
	resumePath  string
	sessionPath string
	skillsDir   string
	tapePath    string
	tapeMode    string
	tracePath   string
	traceHTML   string
	metricsFile string
	delegate    bool
	maxIters    int
	maxTokens   int
	budgetUSD   float64
	wall        time.Duration
	parallel    int
	readOnly    bool
	verbose     bool
	logLevel    string
}

func run() int {
	var o options
	flag.StringVar(&o.provider, "provider", envOr("WOMBAT_PROVIDER", "anthropic"), "anthropic | openai")
	flag.StringVar(&o.stream, "stream", "", "auto | always | never (default: the provider's own env setting)")
	flag.StringVar(&o.model, "model", "", "model id (empty uses the provider default)")
	flag.StringVar(&o.system, "system", "", "system prompt override")
	flag.StringVar(&o.workdir, "working-dir", ".", "working directory for shell and git tools")
	flag.StringVar(&o.fsRoot, "fs-root", "",
		"confine the FILE tools (view_file, write_file, str_replace, save_tool_result) to this prefix.\n"+
			"It does NOT cover bash or grep_search, which shell out and can read and write anywhere\n"+
			"this process can; nor is it a security boundary even for the file tools (symlinks are\n"+
			"not resolved). Use -permission for a rule that also has an opinion about the shell.")
	flag.StringVar(&o.sandbox, "sandbox", "", "deprecated alias for -fs-root; the name oversold what it does")
	flag.StringVar(&o.permission, "permission", "off",
		"off | readonly | workspace | ask — the policy every tool call is checked against.\n"+
			"off: no gate, today's behaviour. readonly: only read-only tools run. workspace: reads\n"+
			"are free, writes inside -working-dir are free, exec and anything outside it must be\n"+
			"approved. ask: nothing runs without approval.\n"+
			"This front end CANNOT collect an approval — stdout is a JSONL protocol and there is no\n"+
			"console to prompt on — so an Ask is denied and the model is told why. -permission ask\n"+
			"here therefore means \"deny anything not explicitly allowed\", which is a useful mode and\n"+
			"not a broken one. Use wombat-serve when you want to answer.")
	flag.StringVar(&o.resumePath, "resume", "", "JSON file holding a prior transcript to continue (read only)")
	flag.StringVar(&o.sessionPath, "session", "",
		"JSON file holding the conversation: read at start if it exists, rewritten when the\n"+
			"turn ends. This is what makes -resume usable — nothing else in this binary ever\n"+
			"wrote a transcript, so the flag that read one could only be fed by a front end\n"+
			"that reconstructed the conversation itself. With -session, multi-turn is:\n"+
			"  wombat-jsonl -session s.json 'start the task'\n"+
			"  wombat-jsonl -session s.json 'now do the next part'\n"+
			"and if the first turn stopped on ask_user, the second turn's text is the ANSWER\n"+
			"to that question rather than a new instruction.")
	flag.StringVar(&o.skillsDir, "skills-dir", "", "directory of <name>/SKILL.md to offer as loadable skills")
	flag.StringVar(&o.tapePath, "tape", "", "record/replay JSONL tape")
	flag.StringVar(&o.tapeMode, "tape-mode", "auto", "auto | record | replay")
	flag.StringVar(&o.tracePath, "trace", "", "write an NDJSON trace here")
	flag.StringVar(&o.traceHTML, "trace-html", "", "also render the trace as a self-contained HTML report")
	flag.StringVar(&o.metricsFile, "metrics-file", "",
		"write one Prometheus exposition dump to this file as the process exits.\n"+
			"A file rather than an endpoint because this command is one-shot: it lives for the\n"+
			"length of a single turn and there is nothing for a scraper to poll — by the time a\n"+
			"scrape interval came round the process would be gone. Point node_exporter's textfile\n"+
			"collector at the directory, or have CI read the file, and one turn's token spend and\n"+
			"tool failures land in the same dashboard as the long-lived services.\n"+
			"The dump is written even when the turn fails, since that is when it matters most.")
	flag.BoolVar(&o.delegate, "delegate", false, "offer a read-only sub-agent through the delegate tool")
	flag.IntVar(&o.maxIters, "max-iters", wombat.DefaultMaxIters, "ReAct iteration cap")
	flag.IntVar(&o.maxTokens, "max-tokens", wombat.DefaultMaxTokens, "reply token cap")
	flag.Float64Var(&o.budgetUSD, "budget", 0, "cost cap in USD (0 = uncapped)")
	flag.DurationVar(&o.wall, "wall", 0, "wall-clock cap (0 = uncapped)")
	flag.IntVar(&o.parallel, "tool-parallelism", 1, "concurrent tool calls per batch")
	flag.BoolVar(&o.readOnly, "read-only", false, "hide mutating and exec tools")
	flag.BoolVar(&o.verbose, "v", false, "debug logging on stderr (same as -log debug)")
	flag.StringVar(&o.logLevel, "log", "info", "stderr log level: debug | info | warn | error")
	flag.Parse()

	query := strings.TrimSpace(strings.Join(flag.Args(), " "))
	if query == "" && o.resumePath == "" && o.sessionPath == "" {
		fmt.Fprintln(os.Stderr, "usage: wombat-jsonl [flags] <query>")
		flag.PrintDefaults()
		return exitUsage
	}
	// Both would mean "read from here, write to there", and the silent version
	// of that discards whatever -session already held without saying so.
	// resolveFSRoot took the same view of -sandbox and -fs-root.
	if o.resumePath != "" && o.sessionPath != "" {
		fmt.Fprintln(os.Stderr,
			"wombat-jsonl: -resume and -session both read a transcript; give one.\n"+
				"  -resume  reads and never writes (fork a conversation)\n"+
				"  -session reads and rewrites (carry one on)")
		return exitUsage
	}
	if err := checkSessionWritable(o.sessionPath); err != nil {
		fmt.Fprintln(os.Stderr, "wombat-jsonl:", err)
		return exitUsage
	}

	// Diagnostics are for whoever is operating the process, and an interactive
	// reader is not that person: at Info this prints a line per model call
	// straight through the transcript they are trying to read. -log warn is
	// what the Makefile's `ask` target uses for exactly that reason.
	level := slog.LevelInfo
	switch strings.ToLower(o.logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	if o.verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	resolveFSRoot(&o, logger)

	out := newEmitter(os.Stdout)

	// The tape and the trace sink both outlive the run and both report real
	// failures only on Close, so they are opened here and not inside build.
	tp, err := openTape(o)
	if err != nil {
		out.emit(failed{Type: "failed", Reason: err.Error()})
		return exitFail
	}
	if tp != nil {
		defer func() {
			if err := tp.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "wombat-jsonl: tape: %v\n", err)
			}
		}()
	}

	tracer, traceDone, err := openTrace(o, logger)
	if err != nil {
		out.emit(failed{Type: "failed", Reason: err.Error()})
		return exitFail
	}
	defer traceDone()

	// The registry outlives the agent by exactly one deferred call. Registered
	// here rather than inside build for the same reason the tape and the trace
	// are: build assembles a chain, and something has to hold the object after
	// the chain is gone in order to write it out.
	var m *metric.Metrics
	if o.metricsFile != "" {
		reg := metric.NewRegistry()
		m = metric.New(reg)
		defer dumpMetrics(reg, o.metricsFile)
	}

	a, err := build(o, logger, tp, tracer, m)
	if err != nil {
		out.emit(failed{Type: "failed", Reason: err.Error()})
		return exitFail
	}

	// Ctrl-C and SIGTERM cancel the context, which unwinds the in-flight HTTP
	// request and any running subprocess on its own.
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	ctx, cancelBudget := governor.WithBudget(ctx, governor.Limits{
		CostUSD: o.budgetUSD,
		Wall:    o.wall,
	})
	defer cancelBudget()

	in, err := input(o, query, tool.NewSet(a.Tools(ctx)...))
	if err != nil {
		out.emit(failed{Type: "failed", Reason: err.Error()})
		return exitFail
	}

	out.emit(sessionStarted{
		Type:      "session_started",
		Model:     o.model,
		Provider:  o.provider,
		Query:     query,
		MaxIters:  o.maxIters,
		BudgetUSD: o.budgetUSD,
	})

	r := a.Start(ctx, in)
	defer r.Close()

	// Deferred, and registered before the drain, so the transcript is written
	// however this turn ends — answered, paused, out of budget, interrupted.
	// The paused and out-of-budget cases are the ones worth resuming.
	defer func() { saveSession(o.sessionPath, r.Messages()) }()

	for r.Next() {
		out.emit(r.Event())
	}

	if err := r.Err(); err != nil {
		out.emit(failed{Type: "failed", Reason: err.Error(), Class: classOf(err)})
		return exitFail
	}

	switch res := r.Outcome().(type) {
	case wombat.Answer:
		out.emit(done{Type: "done", Answer: res.Text})
	case wombat.Paused:
		out.emit(waiting{
			Type:      "agent_waiting",
			ToolUseID: string(res.ToolUseID),
			Question:  res.Schema.Question,
			Schema:    res.Schema.Schema,
		})
	case wombat.Submitted:
		out.emit(submitted{Type: "submitted", Tool: res.Tool, Payload: res.Payload})
	default:
		out.emit(failed{Type: "failed", Reason: "run produced no outcome"})
		return exitFail
	}
	return exitOK
}

// resolveFSRoot folds the deprecated -sandbox into -fs-root.
//
// The old name was renamed rather than kept because it was measurably
// misleading: a run with -sandbox set refused view_file on /etc/hosts and then
// read the same file with bash, in the same turn. A flag that names a boundary
// it does not enforce is worse than no flag. The alias keeps existing callers
// working; the warning tells them the name changed and why.
func resolveFSRoot(o *options, logger *slog.Logger) {
	if o.sandbox == "" {
		return
	}
	if o.fsRoot != "" {
		logger.Warn("-sandbox is a deprecated alias for -fs-root; both were given, using -fs-root",
			"fs_root", o.fsRoot, "sandbox", o.sandbox)
		return
	}
	logger.Warn("-sandbox is deprecated, use -fs-root: it confines the file tools and NOT bash",
		"fs_root", o.sandbox)
	o.fsRoot = o.sandbox
}

// dumpMetrics writes the registry out, once, on the way to exit.
//
// Deferred rather than written after the run, so a bad -permission, an
// unreadable transcript or a panic-free early return still produces whatever
// was measured before things went wrong. Written to a temporary file and
// renamed, because the consumer is a textfile collector that reads the
// directory on its own schedule and would otherwise eventually read a
// half-written exposition and treat the missing half as series that no longer
// exist.
//
// Failures go to stderr and change nothing else. stdout is a JSONL protocol
// and the run's exit status is about the turn, not about telemetry: a full
// disk must not turn a completed turn into a failed one.
func dumpMetrics(reg *metric.Registry, path string) {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: metrics: %v\n", err)
		return
	}
	defer os.Remove(tmp.Name()) // a no-op once the rename below has succeeded

	if err := reg.WriteText(tmp); err != nil {
		tmp.Close()
		fmt.Fprintf(os.Stderr, "wombat-jsonl: metrics: %v\n", err)
		return
	}
	// CreateTemp makes the file 0600, and the reader is a collector daemon
	// running as somebody else. A dump nobody can open is the same as no dump,
	// and it fails silently on the far side where nobody is looking.
	if err := tmp.Chmod(0o644); err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: metrics: %v\n", err)
	}
	if err := tmp.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: metrics: %v\n", err)
		return
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: metrics: %v\n", err)
	}
}

// outcomeClass is the vocabulary of the "outcome" label, and the reason
// [metric.WithErrorClass] exists.
//
// The metric package cannot write this function: saying "denied" means knowing
// about permission.ErrDenied, and metric importing permission would make it
// un-importable FROM permission. So the host supplies the words.
//
// The split that earns its keep is denied versus error. Under -permission
// anything but off this front end refuses every Ask by design (see
// [errNoConsole]), so a headless run produces refusals as a matter of routine
// — counting them as failures would make every dashboard panel red and teach
// whoever reads it to ignore the colour. Cancelled is the same argument for a
// SIGTERM from CI.
//
// Timeout is checked BEFORE cancellation deliberately: a tool that outran
// tool.WithTimeout is a cancelled context underneath and would otherwise match
// the broader arm, and it is the one of the two that means something is wrong.
//
// Five fixed words, so this adds at most five series per tool — see the
// cardinality section of the metric package doc for why that promise matters.
func outcomeClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, permission.ErrDenied):
		return "denied"
	case errors.Is(err, tool.ErrTimeout):
		return "timeout"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "cancelled"
	default:
		return "error"
	}
}

// build assembles the agent. Every dependency is supplied here, at
// construction: the client, the tools, and the tools' own dependencies.
//
// m is nil unless -metrics-file was given, and every use of it is guarded
// rather than replaced with a no-op middleware: a layer that measures nothing
// still costs a closure per call and still appears in a trace as something
// that ran.
func build(
	o options,
	logger *slog.Logger,
	tp *tape.Tape,
	tracer *trace.Tracer,
	m *metric.Metrics,
) (*wombat.Agent, error) {
	var client llm.Client
	var err error

	// ConfigFromEnv supplies the deployment's defaults; the flags override
	// them. The library itself never reads the environment behind the
	// caller's back, so this is the one place configuration is implicit — and
	// it is a main package, which is where implicit configuration belongs.
	switch o.provider {
	case "anthropic", "claude":
		cfg := anthropic.ConfigFromEnv()
		if o.model != "" {
			cfg.Model = o.model
		}
		if o.stream != "" {
			if cfg.Stream, err = parseStream(o.stream); err != nil {
				return nil, err
			}
		}
		client, err = anthropic.New(cfg)
	case "openai":
		cfg := openai.ConfigFromEnv()
		if o.model != "" {
			cfg.Model = o.model
		}
		if o.stream != "" {
			if cfg.Stream, err = parseStream(o.stream); err != nil {
				return nil, err
			}
		}
		client, err = openai.New(cfg)
	default:
		return nil, fmt.Errorf("unknown provider %q (want anthropic or openai)", o.provider)
	}
	if err != nil {
		return nil, err
	}

	// Order is semantic. Cost is charged per attempt, so it sits inside retry.
	// Overflow recovery sits outside retry, so a transient blip is retried at
	// the current transcript size instead of escalating. The tape is
	// outermost of the transport concerns, so a replayed call skips all of it.
	client = llm.Chain(client,
		llm.WithValidation,
		wombat.TrackCost(llm.DefaultPricing),
		llm.WithRetry(llm.DefaultRetryPolicy),
		wombat.WithOverflowRecovery(),
		llm.WithLogging(logger),
	)
	if tp != nil {
		client = llm.Chain(client, tp.LLM())
	}
	if m != nil {
		// Outermost, in a chain of its own so it stays outermost however the
		// list above grows: a call retried three times is one call, which is
		// the boundary a latency histogram has to draw to mean anything.
		// Outside the tape too, so a replayed turn still reports the tokens it
		// would have spent — which is the number a CI budget check wants.
	}

	// Resolve the working directory before anything sees it. The file and
	// shell tools all demand absolute paths, and a model handed a relative
	// one has no way to expand it — observed behavior was a run that spent
	// its entire iteration budget shelling around the filesystem looking for
	// the project it had been pointed at as ".".
	workdir, err := filepath.Abs(o.workdir)
	if err != nil {
		return nil, fmt.Errorf("resolving working directory %q: %w", o.workdir, err)
	}

	// Everything below assembles the tool surface, and the ORDER is the whole
	// point. Three earlier attempts got it wrong in the same way: each layer
	// was appended as it was thought of, and wombat.WithTools resets the tool
	// SET, so registering the delegate tool last silently discarded the
	// skill-gated set built before it. A run then looked correct and had no
	// gating at all.
	//
	// So: decide the tool list completely, THEN gate it, THEN install it once.

	var shared []wombat.Option
	if tp != nil {
		shared = append(shared, wombat.WithToolMiddleware(tp.Tools()))
	}
	if tracer != nil {
		shared = append(shared, wombat.WithTracing(tracer))
	}

	// The gate goes in `shared`, so the sub-agent is governed by the same
	// policy as its parent. A child that could run what its parent had to ask
	// about would make the whole thing decorative — the same argument that
	// puts the skill registry on both.
	//
	// Position in the chain is load-bearing twice over, and both are satisfied
	// by WithToolMiddleware rather than by anything written here: every
	// caller-supplied middleware sits OUTSIDE tool.WithTimeout, so a blocked
	// approval is not guillotined by the per-call deadline, and outside
	// tool.WithRetry, so an approved call is not re-asked on the second
	// attempt.
	gate, err := permissionGate(o.permission, workdir)
	if err != nil {
		return nil, err
	}
	if gate != nil {
		shared = append(shared, wombat.WithToolMiddleware(gate))
	}

	// measure is applied LAST at both call sites below, after gateOptions, and
	// that ordering is load-bearing. WithToolMiddleware appends and a later
	// entry wraps an earlier one, so installed any sooner this would sit
	// INSIDE the permission gate and inside the skill gate and would never see
	// a call either of them refused. The denied/error split that outcomeClass
	// exists for would read zero denials for ever, which looks exactly like a
	// healthy run.
	//
	// Being last also puts it outside retry, the circuit breaker and the
	// per-call timeout, so one dispatch is one observation with the attempts
	// already collapsed. Both agents share it on purpose: a sub-agent's tool
	// calls cost the same money and break in the same ways as its parent's.
	var measure []wombat.Option
	// One switch rather than three hand-wired pieces: WithMetrics installs the
	// LLM middleware, the tool middleware and the run counters together, and
	// applies them in New AFTER every other option — so the tool middleware
	// lands outside the permission gate and a refused call is actually counted.
	if m != nil {
		measure = append(measure, wombat.WithMetrics(m, metric.WithErrorClass(outcomeClass)))
	}

	tools := builtin.Default(builtin.Deps{
		FS:   builtin.OSFS(o.fsRoot),
		Exec: builtin.OSRunner(workdir),
		HTTP: &http.Client{Timeout: 30 * time.Second},
		Now:  time.Now,
	})
	if o.readOnly {
		tools = tool.Filter(tools, tool.OnlyCaps(tool.CapReadOnly|tool.CapNetwork|tool.CapPause))
	}

	// 1. Skills are discovered once, here. The catalogue goes into the system
	// prompt and is therefore part of the prompt-cache prefix, so it has to be
	// fixed before the first call; loading and unloading are what happen
	// during a run.
	var reg *skill.Registry
	if o.skillsDir != "" {
		skills, err := skill.LoadDir(o.skillsDir, func(err error) {
			logger.Warn("skipping skill", "err", err)
		})
		if err != nil {
			return nil, fmt.Errorf("loading skills: %w", err)
		}
		if len(skills) > 0 {
			reg = skill.New(skills...)
			// Demonstration gate: history archaeology is a distinct mode, and
			// its two tools are noise in the surface until the model says it
			// is doing that. Real deployments register gates from a manifest.
			for _, t := range []string{"git_log", "git_show"} {
				if _, ok := reg.GateFor(t); !ok {
					reg.Gate("git-history", t)
				}
			}
			logger.Info("skills loaded", "count", len(skills), "dir", o.skillsDir)
		}
	}

	// 2. The sub-agent, if asked for. Same client and middleware, read-only
	// tools, a short leash, and no delegate tool of its own so recursion stops
	// here. It is gated by the SAME registry: a child that could reach tools
	// its parent has to unlock would make the gate decorative.
	if o.delegate {
		childTools := tool.Filter(tools, tool.OnlyCaps(tool.CapReadOnly|tool.CapNetwork))
		childOpts := append([]wombat.Option{
			wombat.WithName("researcher"),
			wombat.WithClient(client),
			wombat.WithModel(o.model),
			wombat.WithPurpose(llm.PurposeSubagent),
			wombat.WithMaxIters(12),
			wombat.WithLogger(logger),
		}, shared...)
		childOpts = append(childOpts, gateOptions(reg, childTools)...)
		childOpts = append(childOpts, measure...)

		child, err := wombat.New(childOpts...)
		if err != nil {
			return nil, fmt.Errorf("building the sub-agent: %w", err)
		}
		tools = append(tools, wombat.DelegateTool(child))
	}

	// 3. Now the list is final, install it — gated if there is a registry.
	opts := append([]wombat.Option{
		wombat.WithName("wombat"),
		wombat.WithClient(client),
		wombat.WithModel(o.model),
		wombat.WithMaxIters(o.maxIters),
		wombat.WithMaxTokens(o.maxTokens),
		wombat.WithToolParallelism(o.parallel),
		wombat.WithLogger(logger),
	}, shared...)
	opts = append(opts, gateOptions(reg, tools)...)
	opts = append(opts, measure...)

	// Grants are installed on the PARENT only. A sub-agent runs on a context
	// derived from its parent's, so it inherits this set and an approval given
	// once is not asked again on the other side of a delegation; giving the
	// child its own decorator would replace the inherited set with an empty
	// one and undo exactly that.
	if gate != nil {
		opts = append(opts, wombat.WithRunContext(func(ctx context.Context) context.Context {
			return permission.WithGrants(ctx, permission.NewGrants())
		}))
	}

	if o.delegate {
		// A delegated task runs a whole child agent; 120 seconds of real work
		// is nothing. The child's own iteration cap and the run budget bound
		// it instead.
		opts = append(opts, wombat.WithToolTimeoutFallback(0))
	}
	if o.system != "" {
		opts = append(opts, wombat.WithSystemPrompt(o.system))
	}

	// Say what the directory is FOR, not just what it is. A bare path in the
	// system prompt does not connect to the bash tool's exec_dir parameter in
	// the model's head; naming the parameter does.
	opts = append(opts, wombat.WithEnvBlock("working_directory",
		workdir+"\n\nThis is the project you are working on. Pass it as exec_dir "+
			"to the bash tool and use it as the base for any relative path, "+
			"unless the user names a different location."))

	return wombat.New(opts...)
}

// gateOptions installs a tool list, gated by reg when there is one. Returning
// the options rather than applying them keeps the one rule that matters
// visible at both call sites: the tool set is installed exactly once, after
// the list is final.
func gateOptions(reg *skill.Registry, tools []tool.Def) []wombat.Option {
	if reg == nil {
		return []wombat.Option{wombat.WithTools(tools...)}
	}
	g := reg.Bind(tools)
	return []wombat.Option{
		wombat.WithToolSet(g.Set),
		wombat.WithToolMiddleware(g.Middleware),
		wombat.WithSystemBlock("available_skills", g.Index),
		wombat.WithRunContext(func(ctx context.Context) context.Context {
			return skill.WithState(ctx, skill.NewState())
		}),
	}
}

// permissionGate turns -permission into tool middleware, or nil for "off".
//
// Nil rather than an always-allow gate: "off" has to be exactly today's
// behaviour, and an installed middleware that decides nothing still costs a
// context lookup and still shows up in a trace as a layer that ran.
func permissionGate(mode, root string) (tool.Middleware, error) {
	var p permission.Policy
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "off", "":
		return nil, nil
	case "readonly", "read-only":
		p = permission.ReadOnly()
	case "workspace":
		p = permission.Workspace(root)
	case "ask":
		p = permission.AskEverything()
	default:
		return nil, fmt.Errorf("bad -permission %q (want off, readonly, workspace or ask)", mode)
	}
	return permission.Gate(p, permission.ApproverFunc(refuseToAsk)), nil
}

// errNoConsole is what an Ask becomes in a headless front end.
//
// It is an error rather than a bare false so the model reads a sentence
// instead of a verdict: it can then choose a different approach — read the
// file with view_file instead of cat, say — rather than retrying the same
// call and burning the iteration budget on a question nobody is going to
// answer.
var errNoConsole = errors.New(
	"this operation needs a human approval, and wombat-jsonl cannot collect one: " +
		"its stdout is a JSONL protocol and it has no console to prompt on. " +
		"Shell commands run without approval only when the WHOLE command is built " +
		"from these, with no substitution, no redirect to a file, no ~ and no ..: " +
		strings.Join(permission.DefaultSafeCommands, ", ") + ". " +
		"A pipeline of them is fine and so is 2>&1, but `; && || $() >file` are not — " +
		"reach for view_file and grep_search instead of cat and grep where you can. " +
		"Otherwise try an approach the policy allows outright, or tell the user to re-run " +
		"with a front end that can ask")

// refuseToAsk is the headless approver: it denies, with an explanation.
//
// The signature still takes a context it never waits on, because that is the
// [permission.Approver] contract — an approver that CAN ask blocks on it.
func refuseToAsk(context.Context, permission.Request) (bool, error) {
	return false, errNoConsole
}

func openTape(o options) (*tape.Tape, error) {
	if o.tapePath == "" {
		return nil, nil
	}
	var mode tape.Mode
	switch strings.ToLower(o.tapeMode) {
	case "auto", "":
		mode = tape.Auto
	case "record":
		mode = tape.Record
	case "replay":
		mode = tape.Replay
	default:
		return nil, fmt.Errorf("bad -tape-mode %q (want auto, record or replay)", o.tapeMode)
	}
	return tape.Open(o.tapePath, mode)
}

// openTrace returns a tracer and a function that flushes it, rendering the
// HTML report if one was asked for.
func openTrace(o options, logger *slog.Logger) (*trace.Tracer, func(), error) {
	if o.tracePath == "" {
		return nil, func() {}, nil
	}
	sink, closer, err := trace.FileSink(o.tracePath)
	if err != nil {
		return nil, func() {}, fmt.Errorf("opening the trace: %w", err)
	}
	return trace.New(sink), func() {
		_ = closer.Close()
		if o.traceHTML == "" {
			return
		}
		if err := renderTrace(o.tracePath, o.traceHTML); err != nil {
			logger.Error("rendering the trace report", "err", err)
		}
	}, nil
}

func renderTrace(in, out string) error {
	spans, err := trace.ReadFile(in)
	if err != nil {
		return err
	}
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer f.Close()
	return trace.WriteHTML(f, spans)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseStream(s string) (llm.StreamMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "auto", "":
		return llm.StreamAuto, nil
	case "always", "on", "true":
		return llm.StreamAlways, nil
	case "never", "off", "false":
		return llm.StreamNever, nil
	default:
		return 0, fmt.Errorf("bad -stream %q (want auto, always or never)", s)
	}
}

// input decides what this invocation is continuing from.
//
// -resume reads and never writes; -session reads if the file is there and is
// rewritten when the turn ends. A -session file that does not exist yet is a
// fresh conversation, not an error, because that is what the first turn of a
// new session looks like and making the user create an empty file first would
// be a worse interface than the one this replaces.
func input(o options, query string, set tool.Set) (wombat.Input, error) {
	path := o.resumePath
	if path == "" {
		path = o.sessionPath
	}
	if path == "" {
		return wombat.Ask(query), nil
	}

	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist) && path == o.sessionPath:
		return fresh(query)
	case err != nil:
		return wombat.Input{}, fmt.Errorf("reading transcript: %w", err)
	}

	var prior []llm.Message
	if err := json.Unmarshal(b, &prior); err != nil {
		return wombat.Input{}, fmt.Errorf("parsing transcript: %w", err)
	}
	if len(prior) == 0 {
		return fresh(query)
	}
	// wombat.Resume, not wombat.Then: a transcript suspended on ask_user must
	// receive the query as the ANSWER to the question the model asked, not as a
	// new instruction preceded by "(cancelled)".
	return wombat.Resume(prior, query, set), nil
}

// fresh starts a new conversation, refusing an empty one.
//
// The usage check accepts -session with no query, because resuming a saved
// conversation without adding anything is a real thing to want. It is not a
// real thing to want when there is nothing to resume: wombat.Ask("") builds a
// user turn holding an empty text block, Convo.Validate is happy with it (the
// slice is non-empty and starts with a user turn), and the provider answers
// "text content blocks must be non-empty" after the request has already gone
// out. Saying so here costs nothing and names the actual problem.
func fresh(query string) (wombat.Input, error) {
	if query == "" {
		return wombat.Input{}, errors.New(
			"nothing to do: there is no saved conversation to continue and no query was given")
	}
	return wombat.Ask(query), nil
}

// checkSessionWritable fails before the model is called, rather than after.
//
// saveSession runs on the way out and can only report to stderr — stdout is a
// JSONL protocol and the exit status is about the turn, so a save that fails
// there is invisible to a front end reading stdout, which sees a clean `done`
// and reasonably concludes the conversation persisted. Every subsequent
// invocation then starts from zero. The tokens are already spent by that point,
// which is what makes it worth one stat call up front.
func checkSessionWritable(path string) error {
	if path == "" {
		return nil
	}
	if st, err := os.Stat(path); err == nil {
		if st.IsDir() {
			return fmt.Errorf("-session %s is a directory", path)
		}
		return nil
	}
	// Absent is fine — that is a new session — but its directory has to exist
	// and take a file, which is what saveSession will need at the end.
	//
	// The directory is created rather than demanded. builtin's WriteFile takes
	// the same view for the same reason: the caller asked for a path, not for a
	// lecture about mkdir -p, and refusing costs them a round trip to learn
	// something the harness could just do. A typo then creates one empty
	// directory, which is a much smaller problem than the alternative.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("-session %s: %w", path, err)
	}
	probe, err := os.CreateTemp(dir, "."+filepath.Base(path)+".probe*")
	if err != nil {
		return fmt.Errorf("-session %s is not writable: %w", path, err)
	}
	name := probe.Name()
	probe.Close()
	return os.Remove(name)
}

// saveSession writes the transcript so the next invocation can pick it up.
//
// Written on every exit path, including failure, and that is the case it exists
// for: a run that died on a budget cap or stopped to ask a question is exactly
// the one worth resuming, and a save that only happened on success would drop
// it. The caller defers this.
//
// Written to a temporary file and renamed, so a reader — the next invocation,
// or a front end watching the file — never sees a half-written transcript, and
// a crash mid-write leaves the previous turn's file intact rather than a
// truncated one. A failure is reported on stderr and changes nothing else:
// stdout is a JSONL protocol and the exit status is about the turn.
func saveSession(path string, msgs []llm.Message) {
	if path == "" || len(msgs) == 0 {
		return
	}
	b, err := json.MarshalIndent(msgs, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: encoding the session: %v\n", err)
		return
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: writing the session: %v\n", err)
		return
	}
	name := tmp.Name()
	defer os.Remove(name) // no-op once the rename has succeeded

	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		fmt.Fprintf(os.Stderr, "wombat-jsonl: writing the session: %v\n", err)
		return
	}
	// Sync before the rename, or the rename can be durable while the data
	// blocks are not — a power loss then leaves a zero-length session file and
	// the PREVIOUS turn's transcript is gone too, which is worse than the
	// truncated file the temp-and-rename dance was protecting against.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		fmt.Fprintf(os.Stderr, "wombat-jsonl: writing the session: %v\n", err)
		return
	}
	if err := tmp.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: writing the session: %v\n", err)
		return
	}
	// os.CreateTemp makes the file 0600. Renaming it over the session would
	// silently strip whatever the user or a front end running as another
	// account had set, on every single turn.
	mode := os.FileMode(0o600)
	if st, err := os.Stat(path); err == nil {
		mode = st.Mode().Perm()
	}
	if err := os.Chmod(name, mode); err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: writing the session: %v\n", err)
		return
	}
	if err := os.Rename(name, path); err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: writing the session: %v\n", err)
	}
}

// classOf gives the front end a stable machine-readable failure class, so it
// can render "out of budget" differently from "the model refused" without
// pattern-matching on prose.
func classOf(err error) string {
	switch {
	case errors.Is(err, governor.ErrBudgetExhausted):
		return "budget_exhausted"
	case errors.Is(err, governor.ErrWallClock):
		return "wall_clock"
	case errors.Is(err, governor.ErrStepLimit), errors.Is(err, wombat.ErrMaxIterations):
		return "max_iterations"
	case errors.Is(err, wombat.ErrMaxTokens):
		return "max_tokens"
	case errors.Is(err, wombat.ErrRefused):
		return "refused"
	case errors.Is(err, llm.ErrContextWindow):
		return "context_window"
	case errors.Is(err, llm.ErrAuth):
		return "auth"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		return "error"
	}
}

// ===== stdout =====

// emitter serializes writes.
//
// A mutex rather than "just write from one goroutine", because tool
// observations can be emitted from a parallel batch. One interleaved write
// corrupts a line, and the reader on the other side is a line reader.
type emitter struct {
	mu  sync.Mutex
	enc *json.Encoder
	f   *os.File
}

func newEmitter(f *os.File) *emitter {
	enc := json.NewEncoder(f)
	// Model output routinely contains <, > and &. HTML escaping would corrupt
	// it for every consumer that is not a browser.
	enc.SetEscapeHTML(false)
	return &emitter{enc: enc, f: f}
}

func (e *emitter) emit(v any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.enc.Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "wombat-jsonl: encode: %v\n", err)
		return
	}
	_ = e.f.Sync()
}

type sessionStarted struct {
	Type      string  `json:"type"`
	Model     string  `json:"model,omitempty"`
	Provider  string  `json:"provider"`
	Query     string  `json:"query,omitempty"`
	MaxIters  int     `json:"max_iters"`
	BudgetUSD float64 `json:"budget_usd,omitempty"`
}

type done struct {
	Type   string `json:"type"`
	Answer string `json:"answer"`
}

type waiting struct {
	Type      string          `json:"type"`
	ToolUseID string          `json:"tool_use_id"`
	Question  string          `json:"question,omitempty"`
	Schema    json.RawMessage `json:"schema,omitempty"`
}

type submitted struct {
	Type    string          `json:"type"`
	Tool    string          `json:"tool"`
	Payload json.RawMessage `json:"payload"`
}

type failed struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
	Class  string `json:"class,omitempty"`
}
