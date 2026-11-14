package langfuse

// Sampling is where an agent's model reads its sampling settings on each call.
//
// kagent's OpenAI path ignores the request's generation config and sends the
// values of its ModelConfig, held by pointer, so a variant's settings reach the
// model only by writing through those pointers. A nil pointer is a setting the
// ModelConfig leaves unset, which the model then omits from the request, so a
// variant cannot set it.
type Sampling struct {
	Temperature *float64
	TopP        *float64
	MaxTokens   *int64
}

type baseline struct {
	live Sampling
	temp float64
	topP float64
	max  int64
}

func newBaseline(live Sampling) baseline {
	b := baseline{live: live}
	if live.Temperature != nil {
		b.temp = *live.Temperature
	}
	if live.TopP != nil {
		b.topP = *live.TopP
	}
	if live.MaxTokens != nil {
		b.max = *live.MaxTokens
	}
	return b
}

// apply sets each tunable setting to the variant's value, or back to the
// ModelConfig's when the variant leaves it out. A Substrate worker multiplexes
// many actors, but each actor is its own gVisor sandbox with its own runtime
// process, so the pointers are never shared between conversations. Inside an
// actor the variant follows from the prompt name and the actor's name, so
// concurrent calls of one agent write the same values, and the agents of one
// conversation take turns through transfer_to_agent.
func (b baseline) apply(settings *ModelSettings) {
	if settings == nil {
		settings = &ModelSettings{}
	}
	if b.live.Temperature != nil {
		*b.live.Temperature = valueOr(settings.Temperature, b.temp)
	}
	if b.live.TopP != nil {
		*b.live.TopP = valueOr(settings.TopP, b.topP)
	}
	if b.live.MaxTokens != nil {
		*b.live.MaxTokens = valueOr(settings.MaxTokens, b.max)
	}
}

func valueOr[T any](value *T, fallback T) T {
	if value == nil {
		return fallback
	}
	return *value
}
