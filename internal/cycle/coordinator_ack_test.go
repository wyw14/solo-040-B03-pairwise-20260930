package cycle

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type ackFixture struct {
	coordinator *Coordinator
	gates       *gate.Controller
	store       *journal.MemoryStore
	chamberID   string
}

func newAckFixture(t *testing.T) *ackFixture {
	t.Helper()
	now := time.Now
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, now)
	gateController := gate.NewController(gate.NewState("chamber-1"), gate.NewCommands(), recorder, now)
	ramp := hydraulics.NewRamp(time.Millisecond, now)
	hydraulicsController := hydraulics.NewController(gateController.Commands(), recorder, ramp, now)
	coordinator := NewCoordinator(gateController, hydraulicsController, recorder, now)
	return &ackFixture{
		coordinator: coordinator, gates: gateController, store: store,
		chamberID: "chamber-1",
	}
}

func (f *ackFixture) startCycle(t *testing.T) (model.TransitCycle, model.ConvoyPlan) {
	t.Helper()
	plan, err := model.NewConvoyPlan(f.chamberID, model.DirectionUpstream, []model.Vessel{
		{ID: [16]byte{1}},
	}, time.Now())
	if err != nil {
		t.Fatalf("new plan: %v", err)
	}
	cycleValue, err := f.coordinator.Start(context.Background(), plan)
	if err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	return cycleValue, plan
}

func (f *ackFixture) requestOpen(t *testing.T, cycleValue model.TransitCycle) model.DeviceCommand {
	t.Helper()
	command, err := f.gates.Request(context.Background(), cycleValue, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request open: %v", err)
	}
	return command
}

func ackFor(command model.DeviceCommand, settled bool) model.DeviceAck {
	return model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: settled, ObservedAt: time.Now().UTC(),
	}
}

func (f *ackFixture) settledEventCount(t *testing.T) int {
	t.Helper()
	events, err := f.store.Events(context.Background(), f.chamberID, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	count := 0
	for _, event := range events {
		if event.Type == model.EventCommandSettled {
			count++
		}
	}
	return count
}

func (f *ackFixture) upstreamPosition() model.GatePosition {
	return f.gates.State().Position(f.chamberID, model.DirectionUpstream).Position
}

// A receipt that arrives after the transit was cancelled must be rejected and
// must not settle the command, move the gate to open or append any event.
func TestApplyGateAck_TerminatedCycleLeavesStateUntouched(t *testing.T) {
	f := newAckFixture(t)
	ctx := context.Background()
	cycleValue, _ := f.startCycle(t)
	command := f.requestOpen(t, cycleValue)

	if _, err := f.coordinator.Cancel(ctx, f.chamberID, "operator cancellation"); err != nil {
		t.Fatalf("cancel cycle: %v", err)
	}

	eventsBefore := f.settledEventCount(t)
	err := f.coordinator.ApplyGateAck(ctx, ackFor(command, true))
	if err == nil {
		t.Fatal("expected error for receipt on terminated transit, got nil")
	}

	stored, ok := f.gates.Commands().Get(command.ID)
	if !ok {
		t.Fatal("command disappeared")
	}
	if stored.State == model.CommandSettled {
		t.Fatalf("command was marked settled by rejected receipt: %s", stored.State)
	}
	if pos := f.upstreamPosition(); pos != model.GateOpening {
		t.Fatalf("gate position changed on rejected receipt: want opening, got %s", pos)
	}
	if count := f.settledEventCount(t); count != eventsBefore {
		t.Fatalf("settlement events changed on rejection: before=%d after=%d", eventsBefore, count)
	}
}

// An emergency stop cancels the in-flight command. Its late receipt must be a
// read-only rejection.
func TestApplyGateAck_CancelledCommandLeavesStateUntouched(t *testing.T) {
	f := newAckFixture(t)
	ctx := context.Background()
	cycleValue, _ := f.startCycle(t)
	command := f.requestOpen(t, cycleValue)

	if _, err := f.coordinator.Cancel(ctx, f.chamberID, "emergency"); err != nil {
		t.Fatalf("cancel cycle: %v", err)
	}
	f.gates.Commands().CancelChamber(f.chamberID, time.Now())

	err := f.coordinator.ApplyGateAck(ctx, ackFor(command, true))
	if err == nil {
		t.Fatal("expected error for receipt on cancelled command, got nil")
	}

	stored, _ := f.gates.Commands().Get(command.ID)
	if stored.State != model.CommandCancelled {
		t.Fatalf("command state changed: want cancelled, got %s", stored.State)
	}
	if pos := f.upstreamPosition(); pos != model.GateOpening {
		t.Fatalf("gate position changed on rejected receipt: want opening, got %s", pos)
	}
	if count := f.settledEventCount(t); count != 0 {
		t.Fatalf("expected no settlement events, got %d", count)
	}
}

// A receipt belonging to a previous generation must not advance the current
// transit or mutate the old command.
func TestApplyGateAck_StaleGenerationRejected(t *testing.T) {
	f := newAckFixture(t)
	ctx := context.Background()
	firstCycle, _ := f.startCycle(t)
	staleCommand := f.requestOpen(t, firstCycle)

	if _, err := f.coordinator.Cancel(ctx, f.chamberID, "operator cancellation"); err != nil {
		t.Fatalf("cancel first cycle: %v", err)
	}
	currentCycle, _ := f.startCycle(t)

	err := f.coordinator.ApplyGateAck(ctx, ackFor(staleCommand, true))
	if err == nil {
		t.Fatal("expected error for receipt from stale generation, got nil")
	}

	stored, _ := f.gates.Commands().Get(staleCommand.ID)
	if stored.State == model.CommandSettled {
		t.Fatalf("stale command was marked settled: %s", stored.State)
	}
	if pos := f.upstreamPosition(); pos != model.GateOpening {
		t.Fatalf("gate position changed by stale receipt: want opening, got %s", pos)
	}
	if current, ok := f.coordinator.Current(f.chamberID); !ok ||
		current.Generation != currentCycle.Generation || current.Stage != model.StagePlanned {
		t.Fatalf("current transit mutated by stale receipt: %+v", current)
	}
}

// A valid receipt settles the command, observes the open gate and advances the
// transit; a second copy of the same receipt is then a duplicate no-op.
func TestApplyGateAck_ValidReceiptAdvancesAndDuplicateIsRejected(t *testing.T) {
	f := newAckFixture(t)
	ctx := context.Background()
	cycleValue, _ := f.startCycle(t)
	command := f.requestOpen(t, cycleValue)

	if err := f.coordinator.ApplyGateAck(ctx, ackFor(command, true)); err != nil {
		t.Fatalf("valid receipt rejected: %v", err)
	}

	stored, _ := f.gates.Commands().Get(command.ID)
	if stored.State != model.CommandSettled {
		t.Fatalf("command not settled: %s", stored.State)
	}
	if pos := f.upstreamPosition(); pos != model.GateOpen {
		t.Fatalf("gate not observed open after valid receipt: %s", pos)
	}
	if current, ok := f.coordinator.Current(f.chamberID); !ok || current.Stage != model.StageAdmitting {
		t.Fatalf("transit did not advance to admitting: %+v", current)
	}
	if count := f.settledEventCount(t); count != 1 {
		t.Fatalf("expected exactly one settlement event, got %d", count)
	}

	// Duplicate receipt: everything must stay exactly as it is.
	if err := f.coordinator.ApplyGateAck(ctx, ackFor(command, true)); err == nil {
		t.Fatal("expected error for duplicate receipt, got nil")
	}
	stored, _ = f.gates.Commands().Get(command.ID)
	if stored.State != model.CommandSettled {
		t.Fatalf("settled command changed after duplicate: %s", stored.State)
	}
	if pos := f.upstreamPosition(); pos != model.GateOpen {
		t.Fatalf("gate position changed after duplicate receipt: %s", pos)
	}
	if current, ok := f.coordinator.Current(f.chamberID); !ok || current.Stage != model.StageAdmitting {
		t.Fatalf("transit stage changed after duplicate: %+v", current)
	}
	if count := f.settledEventCount(t); count != 1 {
		t.Fatalf("duplicate receipt appended a settlement event: %d", count)
	}
}

// Cancelling a transit and delivering its receipt must interleave safely:
// whichever outcome wins, the command ends in exactly one terminal state and
// the gate is never observed open on a cancelled command.
func TestApplyGateAck_ConcurrentCancelRace(t *testing.T) {
	f := newAckFixture(t)
	ctx := context.Background()
	cycleValue, _ := f.startCycle(t)
	command := f.requestOpen(t, cycleValue)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = f.coordinator.Cancel(ctx, f.chamberID, "emergency")
		f.gates.Commands().CancelChamber(f.chamberID, time.Now())
	}()
	go func() {
		defer wg.Done()
		_ = f.coordinator.ApplyGateAck(ctx, ackFor(command, true))
	}()
	wg.Wait()

	stored, _ := f.gates.Commands().Get(command.ID)
	position := f.upstreamPosition()
	switch stored.State {
	case model.CommandSettled:
		// A settled command must be paired with the observed open gate; the
		// transit may legitimately be cancelled only *after* settlement.
		if position != model.GateOpen {
			t.Fatalf("settled command but gate not open: %s", position)
		}
	case model.CommandCancelled:
		// The rejected receipt must never have moved the gate.
		if position == model.GateOpen {
			t.Fatal("gate observed open on cancelled command")
		}
	default:
		t.Fatalf("command not terminal after race: %s", stored.State)
	}
}

// A receipt that reports the device as not settled must never commit.
func TestApplyGateAck_UnsettledReceiptRejected(t *testing.T) {
	f := newAckFixture(t)
	ctx := context.Background()
	cycleValue, _ := f.startCycle(t)
	command := f.requestOpen(t, cycleValue)

	if err := f.coordinator.ApplyGateAck(ctx, ackFor(command, false)); err == nil {
		t.Fatal("expected error for unsettled receipt, got nil")
	}
	stored, _ := f.gates.Commands().Get(command.ID)
	if stored.State != model.CommandQueued {
		t.Fatalf("command changed after unsettled receipt: want queued, got %s", stored.State)
	}
	if pos := f.upstreamPosition(); pos != model.GateOpening {
		t.Fatalf("gate position changed after unsettled receipt: want opening, got %s", pos)
	}
	if count := f.settledEventCount(t); count != 0 {
		t.Fatalf("expected no settlement events, got %d", count)
	}
}
