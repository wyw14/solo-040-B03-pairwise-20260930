package gate

import (
	"context"
	"errors"
	"time"

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

// ValidateAck classifies a field acknowledgement without mutating command
// state, gate positions or the journal. Callers must finish all generation and
// transit-stage checks against the returned command before calling CommitAck.
func (c *Controller) ValidateAck(ack model.DeviceAck) (model.DeviceCommand, error) {
	return c.commands.ValidateAck(ack)
}

// AckCommit captures the pre-ack snapshots so a failure in a later commit
// step can restore command state and gate position.
type AckCommit struct {
	command          model.DeviceCommand
	previousCommand  model.DeviceCommand
	direction        model.Direction
	previousPosition PositionRecord
}

// CommitAck applies a previously validated acknowledgement: command state,
// gate observation and command-settled event are committed in that order.
// Nothing is changed when validation fails, and a journal failure rolls the
// in-memory commits back to their pre-ack snapshots.
func (c *Controller) CommitAck(ctx context.Context, ack model.DeviceAck) (AckCommit, error) {
	command, previousCommand, err := c.commands.CommitSettle(ack, c.now())
	if err != nil {
		return AckCommit{}, err
	}
	direction := model.DirectionUpstream
	if command.Device == model.DeviceDownstreamGate {
		direction = model.DirectionDownstream
	}
	position := model.GateClosed
	if command.Action == "open" {
		position = model.GateOpen
	}
	previousPosition := c.state.Position(command.ChamberID, direction)
	commit := AckCommit{
		command: command, previousCommand: previousCommand,
		direction: direction, previousPosition: previousPosition,
	}
	if err := c.state.Observe(command.ChamberID, direction, position, c.now()); err != nil {
		_ = c.commands.Revert(command, previousCommand)
		return AckCommit{}, err
	}
	if _, err := c.recorder.Record(ctx, command.ChamberID, model.EventCommandSettled, command.CycleID, command.Generation, command); err != nil {
		_ = c.state.Observe(command.ChamberID, direction, previousPosition.Position, previousPosition.UpdatedAt)
		_ = c.commands.Revert(command, previousCommand)
		return AckCommit{}, err
	}
	return commit, nil
}

// RollbackAck restores the command and gate snapshots captured before the
// commit. The append-only command-settled event cannot be withdrawn; command
// events are audit-only and replay rebuilds cycles from cycle events, so an
// orphaned entry never resurrects state.
func (c *Controller) RollbackAck(commit AckCommit) error {
	if err := c.state.Observe(commit.command.ChamberID, commit.direction, commit.previousPosition.Position, commit.previousPosition.UpdatedAt); err != nil {
		return err
	}
	return c.commands.Revert(commit.command, commit.previousCommand)
}

func (c *Controller) Observe(chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	return c.state.Observe(chamberID, direction, position, c.now())
}

func (c *Controller) State() *State       { return c.state }
func (c *Controller) Commands() *Commands { return c.commands }
