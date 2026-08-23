package skill

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/automanfromm87/wombat-go/tool"
)

// The two meta tools and the annotation vocabulary they emit.
const (
	// LoadToolName pulls a skill body into the transcript and activates it.
	LoadToolName = "load_skill"

	// UnloadToolName retires a skill, hiding its gated tools again.
	UnloadToolName = "unload_skill"

	// Tag labels every load_skill observation. It is the contract with
	// wombat.DropTagged: the harness cannot tell nine kilobytes of skill
	// body from nine kilobytes of grep output by looking at the tool_result,
	// so the handler says which it is via [tool.Annotate], the dispatcher
	// records it on Result.Tags, and the strategy evicts on meaning instead of
	// position.
	Tag = "skill"

	// TagPrefix + the skill's name is emitted alongside [Tag], so a strategy
	// can target one skill ("skill:pdf-forms") rather than all of them.
	TagPrefix = "skill:"
)

// loadTool builds the load_skill tool over r.
func (r *Registry) loadTool() tool.Def {
	desc := "Load a skill's full body into the conversation. Use this when the current task falls " +
		"into the skill's domain (see the skill index in the system prompt). Loading a skill also " +
		"unlocks the tools gated behind it, which are not in your tool list until then. Do not " +
		"pre-load skills speculatively — only when you are about to act in that domain. Subsequent " +
		"calls for the same skill in one run return a short stub, because the body is already in " +
		"your conversation history."

	schemaDesc := "Exact skill name. Available: " + r.nameList() + "."
	if len(r.skills) == 0 {
		schemaDesc = "Exact skill name (no skills are registered — this tool has nothing to do)."
	}

	// annotateFirst wraps the finished Def so the tag is attached before
	// tool.Typed's decoder runs.
	//
	// Putting tool.Annotate at the top of the handler is not early enough: a
	// malformed argument fails inside Typed and never reaches the handler at
	// all, so the resulting is_error tool_result went out untagged and
	// survived every DropTagged sweep. Small leak, but a permanent one, and it
	// only shows up in a transcript that has been running long enough for
	// eviction to matter.
	annotateFirst := func(d tool.Def) tool.Def {
		inner := d.Fn
		d.Fn = func(ctx context.Context, in json.RawMessage) (string, error) {
			tool.Annotate(ctx, Tag)
			return inner(ctx, in)
		}
		return d
	}

	return annotateFirst(tool.Typed(tool.Def{
		Name:        LoadToolName,
		Description: desc,
		InputSchema: nameSchema(r.names(), schemaDesc),

		// Read-only because loading changes no external state, and Meta
		// because it changes the agent's own surface — a planner filtered to
		// CapReadOnly|CapMeta can still pull domain knowledge before deciding
		// how to decompose the work.
		Caps: tool.CapReadOnly | tool.CapMeta,

		// Idempotent: the second call is a stub, so a retry cannot double-load.
		Idempotent: true,
		Category:   "meta",

		// A map lookup and a string copy. A second of wall clock is already
		// three orders of magnitude of headroom; anything longer means the
		// process is wedged, not that the tool is slow.
		Timeout: time.Second,
	}, func(ctx context.Context, in nameInput) (string, error) {
		name := strings.TrimSpace(in.Name)

		// Annotate FIRST and unconditionally, before any early return. The tag
		// is what lets DropTagged find this tool_result later; a stub that
		// went untagged would survive eviction forever, and an error result
		// that went untagged would too. Tagging costs nothing and the model
		// never sees it.
		tool.Annotate(ctx, Tag, TagPrefix+name)

		s, ok := r.byName[name]
		if !ok {
			return "", fmt.Errorf("%w %q; available: %s", ErrUnknownSkill, name, r.nameList())
		}

		st := StateFrom(ctx)

		if st.IsActive(name) {
			// Per-run memo. The body is already in the transcript verbatim;
			// returning it again would pay for it a second time and, in a
			// plan/act pipeline where planner, executor and recovery each call
			// load_skill for the same domain, a third and fourth. That is how
			// a context window dies. The stub keeps the activation truthful —
			// the gated tools stay exposed — and points at the copy already
			// present.
			return fmt.Sprintf(
				"[skill %q was already loaded earlier in this run. Its full body is still in your "+
					"conversation history above — consult it there. The skill remains active and its "+
					"tools stay available. Active skills: %s]",
				name, join(st.Active())), nil
		}

		// Record WHICH tool_use carries the body. Reconcile matches on this id
		// to detect that the body has been trimmed out from under the
		// activation; without it there is nothing to match, because the skill
		// name appears nowhere in the transcript's structure.
		st.Activate(name, tool.InfoFrom(ctx).UseID)

		// Footer, not header: the body is the payload, and a model reading a
		// long markdown blob should hit the content first. It is mostly for
		// trace clarity and to tell the model its surface just changed.
		return s.Body + fmt.Sprintf("\n\n---\n[skill %q activated; its gated tools are now available. Active skills: %s]\n",
			name, join(st.Active())), nil
	}))
}

// unloadTool builds the unload_skill tool over r.
func (r *Registry) unloadTool() tool.Def {
	desc := "Deactivate a previously loaded skill. Its gated tools are retired from your tool list " +
		"starting next turn — call this when you are DONE with the skill's domain, to keep your tool " +
		"surface focused. Underlying state is NOT touched: sandbox containers, browser tabs and any " +
		"other resources the skill's tools created stay alive, so clean those up with their own tools " +
		"BEFORE unloading. Idempotent: unloading a skill that is not active does nothing."

	schemaDesc := "Exact skill name to unload. Known: " + r.nameList() + "."
	if len(r.skills) == 0 {
		schemaDesc = "Exact skill name to unload (no skills are registered — this tool has nothing to do)."
	}

	return tool.Typed(tool.Def{
		Name:        UnloadToolName,
		Description: desc,
		InputSchema: nameSchema(r.names(), schemaDesc),
		Caps:        tool.CapReadOnly | tool.CapMeta,
		Idempotent:  true,
		Category:    "meta",
		Timeout:     time.Second,
	}, func(ctx context.Context, in nameInput) (string, error) {
		name := strings.TrimSpace(in.Name)

		// Unknown names are still an error here even though unloading is
		// idempotent: "unload finance-tools" when the skill is "finance" is a
		// typo the model should see, not a no-op it will believe worked.
		if _, ok := r.byName[name]; !ok {
			return "", fmt.Errorf("%w %q; known: %s", ErrUnknownSkill, name, r.nameList())
		}

		st := StateFrom(ctx)
		if !st.IsActive(name) {
			// A no-op, reported as an ordinary observation rather than an
			// is_error. Nothing is wrong: the desired end state — skill not
			// active — already holds, and an error card here would push the
			// model into pointless recovery.
			return fmt.Sprintf("[skill %q was not active — nothing to do. Active skills: %s]",
				name, join(st.Active())), nil
		}

		st.Deactivate(name)
		return fmt.Sprintf("[skill %q unloaded; its gated tools are retired from your tool list. "+
			"Active skills: %s]", name, join(st.Active())), nil
	})
}

// join renders an active-skill list for a model-facing message.
func join(names []string) string {
	if len(names) == 0 {
		return "(none)"
	}
	return strings.Join(names, ", ")
}
