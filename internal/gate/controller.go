package gate

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Controller links physical observations, command lifecycle and the journal.
type Controller struct {
	state    *State
	commands *Commands
	recorder *journal.Recorder
	now      func() time.Time
}

func NewController(state *State, commands *Commands, recorder *journal.Recorder, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{state: state, commands: commands, recorder: recorder, now: now}
}

func (c *Controller) Request(ctx context.Context, cycle model.TransitCycle, direction model.Direction, open bool) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	device := model.DeviceUpstreamGate
	if direction == model.DirectionDownstream {
		device = model.DeviceDownstreamGate
	}
	action := "close"
	position := model.GateClosing
	if open {
		action = "open"
		position = model.GateOpening
	}
	command, err := c.commands.Issue(cycle, device, action, 0, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := c.state.Observe(cycle.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	_, err = c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command)
	return command, err
}

// CommitGateAck is the committing step for an acknowledgement. The caller must
// already have validated the receipt (Commands.Resolve) and confirmed it
// belongs to the live transit generation. Only then does this mark the command
// settled, observe the terminal gate position and append the settlement event.
// If the command became terminal in the meantime, nothing is mutated.
func (c *Controller) CommitGateAck(ctx context.Context, commandID uuid.UUID) (model.DeviceCommand, error) {
	now := c.now()
	command, err := c.commands.MarkSettled(commandID, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	direction := model.DirectionUpstream
	if command.Device == model.DeviceDownstreamGate {
		direction = model.DirectionDownstream
	}
	position := model.GateClosed
	if command.Action == "open" {
		position = model.GateOpen
	}
	if err := c.state.Observe(command.ChamberID, direction, position, now); err != nil {
		return model.DeviceCommand{}, err
	}
	if _, err := c.recorder.Record(ctx, command.ChamberID, model.EventCommandSettled, command.CycleID, command.Generation, command); err != nil {
		return model.DeviceCommand{}, err
	}
	return command, nil
}

func (c *Controller) Observe(chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	return c.state.Observe(chamberID, direction, position, c.now())
}

func (c *Controller) State() *State       { return c.state }
func (c *Controller) Commands() *Commands { return c.commands }
