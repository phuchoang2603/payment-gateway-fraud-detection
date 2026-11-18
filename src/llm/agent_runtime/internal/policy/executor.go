package policy

import (
	"context"
	"errors"
	"iter"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

type (
	boundaryKey    struct{}
	reviewBoundary struct{ final string }
)

type Executor struct {
	a2asrv.AgentExecutor
	Policy *Policy
}

func (e *Executor) Execute(ctx context.Context, request *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		run, err := e.Policy.selectProfile(ctx)
		if err != nil {
			yield(nil, err)
			return
		}
		// NOTE: the A2A client forwards the caller's tenant to every remote skill, and the target Agent URL rejects it.
		ctx = a2a.AttachTenant(ctx, "")
		if run.profile.Input != nil {
			var text strings.Builder
			if request.Message != nil {
				for _, part := range request.Message.Parts {
					text.WriteString(part.Text())
				}
			}
			if rule := contractRule("input", run.profile.Input.validate(text.String())); rule != "" {
				_ = e.Policy.deny(ctx, run.name, rule, "review_decline")
				e.reply(request, `{"error":"review input contract refused"}`, yield)
				return
			}
		}
		if run.profile.Output == nil {
			e.AgentExecutor.Execute(ctx, request)(yield)
			return
		}
		boundary := &reviewBoundary{}
		ctx = context.WithValue(ctx, boundaryKey{}, boundary)
		failed := false
		// NOTE: only the validated root result may cross the merchant boundary, including for streaming requests.
		for _, err := range e.AgentExecutor.Execute(ctx, request) {
			if err != nil {
				failed = true
			}
		}
		text := boundary.final
		rule := "output_contract_upstream_failed"
		if !failed {
			rule = contractRule("output", run.profile.Output.validate(text))
		}
		if rule != "" {
			_ = e.Policy.deny(ctx, run.name, rule, "review_decline")
			text = `{"error":"review output contract refused"}`
		}
		e.reply(request, text, yield)
	}
}

func (e *Executor) reply(request *a2asrv.ExecutorContext, text string, yield func(a2a.Event, error) bool) {
	reply := a2a.NewMessageForTask(a2a.MessageRoleAgent, request, a2a.NewTextPart(text))
	yield(a2a.NewStatusUpdateEvent(request, a2a.TaskStateCompleted, reply), nil)
}

func contractRule(side string, err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errContractJSON):
		return side + "_contract_not_json"
	case errors.Is(err, errContractSchema):
		return side + "_contract_schema"
	default:
		return side + "_contract_forbidden_field"
	}
}

func (e *Executor) Cleanup(ctx context.Context, request *a2asrv.ExecutorContext, result a2a.SendMessageResult, cause error) {
	if cleaner, ok := e.AgentExecutor.(a2asrv.AgentExecutionCleaner); ok {
		cleaner.Cleanup(ctx, request, result, cause)
	}
}
