package cycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type ackHarness struct {
	store    *journal.MemoryStore
	recorder *journal.Recorder
	gates    *gate.Controller
	commands *gate.Commands
	cycles   *Coordinator
	plan     model.ConvoyPlan
	ticks    atomic.Int64
}

func newAckHarness(t *testing.T) *ackHarness {
	t.Helper()
	const chamber = "chamber-1"
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	h := &ackHarness{store: journal.NewMemoryStore()}
	now := func() time.Time { return base.Add(time.Duration(h.ticks.Add(1)) * time.Second) }
	h.recorder = journal.NewRecorder(h.store, now)
	state := gate.NewState(chamber)
	h.commands = gate.NewCommands()
	h.gates = gate.NewController(state, h.commands, h.recorder, now)
	ramp := hydraulics.NewRamp(time.Millisecond, now)
	hydraulicsController := hydraulics.NewController(h.commands, h.recorder, ramp, now)
	h.cycles = NewCoordinator(h.gates, hydraulicsController, h.recorder, now)
	vessel, err := model.NewVessel("MV Ranger", "WYC1", 120)
	if err != nil {
		t.Fatal(err)
	}
	h.plan, err = model.NewConvoyPlan(chamber, model.DirectionUpstream, []model.Vessel{vessel}, now())
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *ackHarness) startCycle(t *testing.T) model.TransitCycle {
	t.Helper()
	cycleValue, err := h.cycles.Start(context.Background(), h.plan)
	if err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	return cycleValue
}

func (h *ackHarness) requestGate(t *testing.T, cycleValue model.TransitCycle, open bool) model.DeviceCommand {
	t.Helper()
	command, err := h.gates.Request(context.Background(), cycleValue, model.DirectionUpstream, open)
	if err != nil {
		t.Fatalf("request gate: %v", err)
	}
	return command
}

func ackFor(command model.DeviceCommand) model.DeviceAck {
	return model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: command.UpdatedAt,
	}
}

func (h *ackHarness) eventCount(t *testing.T) int {
	t.Helper()
	events, err := h.store.Events(context.Background(), h.plan.ChamberID, 0)
	if err != nil {
		t.Fatal(err)
	}
	return len(events)
}

func (h *ackHarness) eventTypes(t *testing.T) []model.EventType {
	t.Helper()
	events, err := h.store.Events(context.Background(), h.plan.ChamberID, 0)
	if err != nil {
		t.Fatal(err)
	}
	types := make([]model.EventType, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	return types
}

// A legitimate acknowledgement settles the command, observes the final gate
// position and advances the cycle one stage.
func TestApplyGateAck_LegitimateAckAdvancesCycle(t *testing.T) {
	h := newAckHarness(t)
	cycleValue := h.startCycle(t)
	command := h.requestGate(t, cycleValue, true)
	baseline := h.eventCount(t)

	if err := h.cycles.ApplyGateAck(context.Background(), ackFor(command)); err != nil {
		t.Fatalf("legitimate ack rejected: %v", err)
	}

	current, ok := h.cycles.Current(h.plan.ChamberID)
	if !ok || current.Stage != model.StageAdmitting {
		t.Fatalf("cycle stage = %v, want admitting", current.Stage)
	}
	if settled, _ := h.commands.Get(command.ID); settled.State != model.CommandSettled {
		t.Fatalf("command state = %q, want settled", settled.State)
	}
	if position := h.gates.State().Snapshot(h.plan.ChamberID)[model.DirectionUpstream]; position != model.GateOpen {
		t.Fatalf("gate position = %q, want open", position)
	}
	if h.eventCount(t) != baseline+2 {
		t.Fatalf("event count = %d, want %d (command.settled + cycle.advanced)", h.eventCount(t), baseline+2)
	}
	last := h.eventTypes(t)[h.eventCount(t)-2:]
	if last[0] != model.EventCommandSettled || last[1] != model.EventCycleAdvanced {
		t.Fatalf("last events = %v, want [command.settled cycle.advanced]", last)
	}
}

// The reported defect: after the transit is cancelled the late field ack must
// be rejected without mutating command state, gate position or the journal.
func TestApplyGateAck_StaleAckAfterCancellationChangesNothing(t *testing.T) {
	h := newAckHarness(t)
	cycleValue := h.startCycle(t)
	command := h.requestGate(t, cycleValue, true)
	if _, err := h.cycles.Cancel(context.Background(), h.plan.ChamberID, "operator cancelled"); err != nil {
		t.Fatalf("cancel cycle: %v", err)
	}

	stored, _ := h.commands.Get(command.ID)
	baselineEvents := h.eventCount(t)
	baselinePosition := h.gates.State().Snapshot(h.plan.ChamberID)

	err := h.cycles.ApplyGateAck(context.Background(), ackFor(command))
	if err == nil || err.Error() != "device acknowledgement belongs to an inactive transit generation" {
		t.Fatalf("stale ack error = %v, want inactive transit generation", err)
	}

	if after, _ := h.commands.Get(command.ID); after.State != stored.State {
		t.Fatalf("command state changed from %q to %q by rejected ack", stored.State, after.State)
	}
	if after := h.gates.State().Snapshot(h.plan.ChamberID); after[model.DirectionUpstream] != baselinePosition[model.DirectionUpstream] {
		t.Fatalf("gate position changed from %q to %q by rejected ack", baselinePosition[model.DirectionUpstream], after[model.DirectionUpstream])
	}
	if h.eventCount(t) != baselineEvents {
		t.Fatalf("rejected stale ack appended events: count %d -> %d", baselineEvents, h.eventCount(t))
	}
	current, _ := h.cycles.Current(h.plan.ChamberID)
	if current.Stage != model.StageCancelled {
		t.Fatalf("cancelled cycle stage = %q", current.Stage)
	}
}

// A command cancelled directly (emergency stop) is also rejected as stale
// with no side effects.
func TestApplyGateAck_CancelledCommandRejectedAsStale(t *testing.T) {
	h := newAckHarness(t)
	cycleValue := h.startCycle(t)
	command := h.requestGate(t, cycleValue, true)
	if _, err := h.cycles.Cancel(context.Background(), h.plan.ChamberID, "emergency"); err != nil {
		t.Fatalf("cancel cycle: %v", err)
	}
	h.commands.CancelChamber(h.plan.ChamberID, time.Now())
	baseline := h.eventCount(t)

	err := h.cycles.ApplyGateAck(context.Background(), ackFor(command))
	if !errors.Is(err, gate.ErrStaleAck) {
		t.Fatalf("error = %v, want gate.ErrStaleAck", err)
	}
	if after, _ := h.commands.Get(command.ID); after.State != model.CommandCancelled {
		t.Fatalf("command state = %q, want cancelled", after.State)
	}
	if position := h.gates.State().Snapshot(h.plan.ChamberID)[model.DirectionUpstream]; position != model.GateOpening {
		t.Fatalf("gate position = %q, want opening (unchanged)", position)
	}
	if h.eventCount(t) != baseline {
		t.Fatalf("rejected ack appended events: count %d -> %d", baseline, h.eventCount(t))
	}
}

// An ack carrying an older generation after a new transit started belongs to
// a superseded generation and must not settle anything.
func TestApplyGateAck_OldGenerationAfterRestartChangesNothing(t *testing.T) {
	h := newAckHarness(t)
	firstCycle := h.startCycle(t)
	oldCommand := h.requestGate(t, firstCycle, true)
	if _, err := h.cycles.Cancel(context.Background(), h.plan.ChamberID, "superseded"); err != nil {
		t.Fatalf("cancel cycle: %v", err)
	}
	secondCycle := h.startCycle(t)
	newCommand := h.requestGate(t, secondCycle, true)
	baseline := h.eventCount(t)

	err := h.cycles.ApplyGateAck(context.Background(), ackFor(oldCommand))
	if err == nil || err.Error() != "device acknowledgement belongs to an inactive transit generation" {
		t.Fatalf("old generation ack error = %v", err)
	}
	if after, _ := h.commands.Get(oldCommand.ID); after.State == model.CommandSettled {
		t.Fatal("old generation command was settled")
	}
	if h.eventCount(t) != baseline {
		t.Fatalf("rejected old-generation ack appended events")
	}

	// The current generation's ack still advances normally.
	if err := h.cycles.ApplyGateAck(context.Background(), ackFor(newCommand)); err != nil {
		t.Fatalf("current generation ack rejected: %v", err)
	}
	current, _ := h.cycles.Current(h.plan.ChamberID)
	if current.Generation != 2 || current.Stage != model.StageAdmitting {
		t.Fatalf("current cycle = gen %d stage %q, want gen 2 admitting", current.Generation, current.Stage)
	}
}

// Re-sending an already accepted acknowledgement is a no-op: no state change
// and no additional events.
func TestApplyGateAck_DuplicateAckIsIdempotent(t *testing.T) {
	h := newAckHarness(t)
	cycleValue := h.startCycle(t)
	command := h.requestGate(t, cycleValue, true)
	ack := ackFor(command)
	if err := h.cycles.ApplyGateAck(context.Background(), ack); err != nil {
		t.Fatalf("first ack: %v", err)
	}
	baseline := h.eventCount(t)

	err := h.cycles.ApplyGateAck(context.Background(), ack)
	if !errors.Is(err, gate.ErrDuplicateAck) {
		t.Fatalf("duplicate ack error = %v, want gate.ErrDuplicateAck", err)
	}
	if after, _ := h.commands.Get(command.ID); after.State != model.CommandSettled {
		t.Fatalf("command state = %q, want settled", after.State)
	}
	if position := h.gates.State().Snapshot(h.plan.ChamberID)[model.DirectionUpstream]; position != model.GateOpen {
		t.Fatalf("gate position = %q, want open", position)
	}
	if h.eventCount(t) != baseline {
		t.Fatalf("duplicate ack appended events: count %d -> %d", baseline, h.eventCount(t))
	}
	current, _ := h.cycles.Current(h.plan.ChamberID)
	if current.Stage != model.StageAdmitting {
		t.Fatalf("cycle stage = %q, want admitting", current.Stage)
	}
}

// An ack whose identity fields do not match the stored command is rejected
// before any commit.
func TestApplyGateAck_MismatchedIdentityChangesNothing(t *testing.T) {
	h := newAckHarness(t)
	cycleValue := h.startCycle(t)
	command := h.requestGate(t, cycleValue, true)
	baseline := h.eventCount(t)

	ack := ackFor(command)
	ack.Generation++
	if err := h.cycles.ApplyGateAck(context.Background(), ack); err == nil {
		t.Fatal("ack with wrong generation was accepted")
	}
	if after, _ := h.commands.Get(command.ID); after.State != model.CommandQueued {
		t.Fatalf("command state = %q, want queued", after.State)
	}
	if position := h.gates.State().Snapshot(h.plan.ChamberID)[model.DirectionUpstream]; position != model.GateOpening {
		t.Fatalf("gate position = %q, want opening", position)
	}
	if h.eventCount(t) != baseline {
		t.Fatalf("rejected ack appended events")
	}
}

// Gate acks are not valid in every stage: once the transit is releasing, a
// late ack must not settle its command or append events.
func TestApplyGateAck_InvalidStageChangesNothing(t *testing.T) {
	h := newAckHarness(t)
	cycleValue := h.startCycle(t)

	var lastCommand model.DeviceCommand
	// planned -> admitting -> sealed -> leveling -> releasing
	for i := 0; i < 4; i++ {
		lastCommand = h.requestGate(t, cycleValue, i%2 == 0)
		if err := h.cycles.ApplyGateAck(context.Background(), ackFor(lastCommand)); err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
		current, _ := h.cycles.Current(h.plan.ChamberID)
		cycleValue = current
	}
	current, _ := h.cycles.Current(h.plan.ChamberID)
	if current.Stage != model.StageReleasing {
		t.Fatalf("stage = %q, want releasing", current.Stage)
	}

	extra := h.requestGate(t, cycleValue, true)
	baseline := h.eventCount(t)
	if err := h.cycles.ApplyGateAck(context.Background(), ackFor(extra)); err == nil {
		t.Fatal("ack in releasing stage was accepted")
	}
	if after, _ := h.commands.Get(extra.ID); after.State != model.CommandQueued {
		t.Fatalf("command state = %q, want queued", after.State)
	}
	if position := h.gates.State().Snapshot(h.plan.ChamberID)[model.DirectionUpstream]; position != model.GateOpening {
		t.Fatalf("gate position = %q, want opening", position)
	}
	if h.eventCount(t) != baseline {
		t.Fatalf("rejected stage-invalid ack appended events")
	}
	if stage, _ := h.cycles.Current(h.plan.ChamberID); stage.Stage != model.StageReleasing {
		t.Fatalf("stage changed to %q", stage.Stage)
	}
}

// Regardless of how cancel and ack interleave, a rejected ack must never leave
// behind a settled command, a final gate position or a settled event. If the
// ack legitimately won the race, its commits must be fully present.
func TestApplyGateAck_CancelRaceInvariant(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		h := newAckHarness(t)
		cycleValue := h.startCycle(t)
		command := h.requestGate(t, cycleValue, true)
		ack := ackFor(command)

		var wg sync.WaitGroup
		wg.Add(2)
		var ackErr error
		go func() {
			defer wg.Done()
			ackErr = h.cycles.ApplyGateAck(context.Background(), ack)
		}()
		go func() {
			defer wg.Done()
			_, _ = h.cycles.Cancel(context.Background(), h.plan.ChamberID, "race cancel")
		}()
		wg.Wait()

		stored, _ := h.commands.Get(command.ID)
		position := h.gates.State().Snapshot(h.plan.ChamberID)[model.DirectionUpstream]
		settledEvents := 0
		for _, kind := range h.eventTypes(t) {
			if kind == model.EventCommandSettled {
				settledEvents++
			}
		}

		if ackErr != nil {
			// Ack lost the race: its rejection must have changed nothing.
			if stored.State != model.CommandQueued {
				t.Fatalf("iteration %d: rejected ack left command state %q", iteration, stored.State)
			}
			if position != model.GateOpening {
				t.Fatalf("iteration %d: rejected ack left gate position %q", iteration, position)
			}
			if settledEvents != 0 {
				t.Fatalf("iteration %d: rejected ack left %d command.settled events", iteration, settledEvents)
			}
			current, _ := h.cycles.Current(h.plan.ChamberID)
			if current.Stage != model.StageCancelled {
				t.Fatalf("iteration %d: stage after lost ack = %q, want cancelled", iteration, current.Stage)
			}
			continue
		}

		// Ack won: all three commits must be present. Cancel afterwards may
		// still terminate the cycle legitimately.
		if stored.State != model.CommandSettled {
			t.Fatalf("iteration %d: accepted ack left command state %q", iteration, stored.State)
		}
		if position != model.GateOpen {
			t.Fatalf("iteration %d: accepted ack left gate position %q", iteration, position)
		}
		if settledEvents != 1 {
			t.Fatalf("iteration %d: accepted ack left %d command.settled events, want 1", iteration, settledEvents)
		}
	}
}
