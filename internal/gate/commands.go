package gate

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// ErrStaleAck rejects acknowledgements whose command belongs to a cancelled or
// superseded transit generation.
var ErrStaleAck = errors.New("device acknowledgement belongs to an inactive transit generation")

// ErrDuplicateAck rejects repeated acknowledgements of an already settled command.
var ErrDuplicateAck = errors.New("duplicate device acknowledgement for settled command")

// Commands owns command identity, lifecycle and late-ack classification.
type Commands struct {
	mu        sync.RWMutex
	byID      map[uuid.UUID]model.DeviceCommand
	byChamber map[string][]uuid.UUID
	nextEpoch map[string]uint64
}

func NewCommands() *Commands {
	return &Commands{
		byID:      make(map[uuid.UUID]model.DeviceCommand),
		byChamber: make(map[string][]uuid.UUID),
		nextEpoch: make(map[string]uint64),
	}
}

func epochKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

func (c *Commands) Issue(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := epochKey(cycle.ChamberID, device)
	c.nextEpoch[key]++
	command, err := model.NewDeviceCommand(cycle, c.nextEpoch[key], device, action, target, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	c.byChamber[cycle.ChamberID] = append(c.byChamber[cycle.ChamberID], command.ID)
	return command, nil
}

func (c *Commands) Accept(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandAccepted, now)
}

func (c *Commands) Execute(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandExecuting, now)
}

func (c *Commands) transition(commandID uuid.UUID, state model.CommandState, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return fmt.Errorf("unknown command %s", commandID)
	}
	if err := command.SetState(state, now); err != nil {
		return err
	}
	c.byID[commandID] = command
	return nil
}

// ValidateAck verifies an acknowledgement without changing command state.
func (c *Commands) ValidateAck(ack model.DeviceAck) (model.DeviceCommand, error) {
	c.mu.RLock()
	command, ok := c.byID[ack.CommandID]
	c.mu.RUnlock()
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", ack.CommandID)
	}
	if err := validateAck(command, ack); err != nil {
		return model.DeviceCommand{}, err
	}
	return command, nil
}

func validateAck(command model.DeviceCommand, ack model.DeviceAck) error {
	if !ack.Matches(command) {
		return errors.New("ack identity does not match command generation")
	}
	if !ack.Settled {
		return errors.New("device has not settled")
	}
	switch command.State {
	case model.CommandSettled:
		return ErrDuplicateAck
	case model.CommandCancelled, model.CommandFailed:
		return ErrStaleAck
	}
	return nil
}

// CommitSettle transitions a validated command to settled and returns both
// the committed command and its pre-commit snapshot. Validation is repeated
// under the lock so a cancellation racing the caller's pre-check can still
// reject the acknowledgement.
func (c *Commands) CommitSettle(ack model.DeviceAck, now time.Time) (model.DeviceCommand, model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[ack.CommandID]
	if !ok {
		return model.DeviceCommand{}, model.DeviceCommand{}, fmt.Errorf("unknown command %s", ack.CommandID)
	}
	previous := command
	if err := validateAck(command, ack); err != nil {
		return model.DeviceCommand{}, model.DeviceCommand{}, err
	}
	if err := command.SetState(model.CommandSettled, now); err != nil {
		return model.DeviceCommand{}, model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	return command, previous, nil
}

// Revert restores the pre-commit command snapshot after a downstream step in
// the same acknowledgement transaction failed. It refuses to overwrite a
// command that changed since the settle commit (e.g. a concurrent cancel).
func (c *Commands) Revert(committed, previous model.DeviceCommand) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.byID[committed.ID]
	if !ok || current.State != model.CommandSettled || !current.UpdatedAt.Equal(committed.UpdatedAt) {
		return errors.New("command changed since settle commit")
	}
	c.byID[committed.ID] = previous
	return nil
}

func (c *Commands) CancelChamber(chamberID string, now time.Time) []model.DeviceCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cancelled []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if command.State.Terminal() {
			continue
		}
		if command.SetState(model.CommandCancelled, now) == nil {
			c.byID[commandID] = command
			cancelled = append(cancelled, command)
		}
	}
	return cancelled
}

func (c *Commands) Pending(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var pending []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if !command.State.Terminal() {
			pending = append(pending, command)
		}
	}
	return pending
}

func (c *Commands) Get(commandID uuid.UUID) (model.DeviceCommand, bool) {
	c.mu.RLock()
	command, ok := c.byID[commandID]
	c.mu.RUnlock()
	return command, ok
}
