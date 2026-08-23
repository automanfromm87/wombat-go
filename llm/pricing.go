package llm

import "strings"

// Pricing turns token usage into dollars.
type Pricing interface {
	CostUSD(model string, u Usage) float64
}

// Pricer is an optional extension a Pricing may implement to admit that it
// does not know a model.
//
// Without it, an unpriced model and a free one are indistinguishable: both
// report 0.00, and a cost column reading $0.0000 after a real run looks like
// good news rather than a missing table entry. That is not hypothetical — a
// benchmark against a gateway whose model was not in DefaultPricing reported
// zero for every episode, and the number was believed for a while.
//
// A Pricing that does not implement Pricer is taken at its word.
type Pricer interface {
	Pricing
	Priced(model string) bool
}

// Priced reports whether p can price model. True when p is not a [Pricer],
// since a plain Pricing makes no claim either way.
func Priced(p Pricing, model string) bool {
	if pr, ok := p.(Pricer); ok {
		return pr.Priced(model)
	}
	return true
}

// Rate is a model's price in USD per million tokens.
type Rate struct {
	In         float64
	Out        float64
	CacheWrite float64
	CacheRead  float64
}

// Table prices by longest matching model-id prefix, so a dated id such as
// "claude-haiku-4-5-20251001" is priced by its "claude-haiku-4-5" entry
// without the table needing a row per release.
type Table map[string]Rate

// CostUSD implements Pricing. An unpriced model costs 0 — a budget that
// silently stops counting is better than a run that dies because the table is
// stale. Use [Table.Priced] to tell that zero apart from a model that is
// genuinely free.
func (t Table) CostUSD(model string, u Usage) float64 {
	rate, ok := t.lookup(model)
	if !ok {
		return 0
	}
	const perMillion = 1_000_000.0
	return float64(u.InputTokens)*rate.In/perMillion +
		float64(u.OutputTokens)*rate.Out/perMillion +
		float64(u.CacheWriteTokens)*rate.CacheWrite/perMillion +
		float64(u.CacheReadTokens)*rate.CacheRead/perMillion
}

// Priced implements [Pricer].
func (t Table) Priced(model string) bool {
	_, ok := t.lookup(model)
	return ok
}

func (t Table) lookup(model string) (Rate, bool) {
	if r, ok := t[model]; ok {
		return r, true
	}
	best, bestLen, found := Rate{}, -1, false
	for prefix, r := range t {
		if len(prefix) > bestLen && strings.HasPrefix(model, prefix) {
			best, bestLen, found = r, len(prefix), true
		}
	}
	return best, found
}

// DefaultPricing is a starting table. Rates move; treat it as a default to
// override with a Config-supplied table rather than as a source of truth.
//
// Keys are id prefixes, so a dated release is covered by its family. Cache
// write is 1.25x input and cache read 0.1x, which is the published ratio
// across the Claude family.
//
// A model with no entry costs 0 — notably any self-hosted or gateway-internal
// model, which is usually correct (nobody is billing per token) but does mean
// a cost budget silently never trips for those. Supply a table if that matters.
var DefaultPricing Pricing = Table{
	"claude-opus-5":    {In: 15, Out: 75, CacheWrite: 18.75, CacheRead: 1.50},
	"claude-sonnet-5":  {In: 3, Out: 15, CacheWrite: 3.75, CacheRead: 0.30},
	"claude-fable-5":   {In: 3, Out: 15, CacheWrite: 3.75, CacheRead: 0.30},
	"claude-haiku-4-5": {In: 1, Out: 5, CacheWrite: 1.25, CacheRead: 0.10},

	"claude-opus-4":     {In: 15, Out: 75, CacheWrite: 18.75, CacheRead: 1.50},
	"claude-sonnet-4":   {In: 3, Out: 15, CacheWrite: 3.75, CacheRead: 0.30},
	"claude-3-7-sonnet": {In: 3, Out: 15, CacheWrite: 3.75, CacheRead: 0.30},
	"claude-3-5-haiku":  {In: 0.80, Out: 4, CacheWrite: 1.00, CacheRead: 0.08},
}

// FreePricing prices everything at zero. Useful for local models and tests.
var FreePricing Pricing = Table{}
