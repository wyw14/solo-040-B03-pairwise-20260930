package gate

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

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

// Resolve validates an acknowledgement without touching command state. It lets
// the caller decide whether the receipt belongs to the live transit before any
// command, gate or event mutation happens. A receipt for a cancelled, failed or
// already-settled command is rejected, which also makes duplicate receipts a
// read-only no-op.
func (c *Commands) Resolve(ack model.DeviceAck) (model.DeviceCommand, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	command, ok := c.byID[ack.CommandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", ack.CommandID)
	}
	if !ack.Matches(command) {
		return model.DeviceCommand{}, errors.New("ack identity does not match command generation")
	}
	if !ack.Settled {
		return model.DeviceCommand{}, errors.New("device has not settled")
	}
	switch command.State {
	case model.CommandCancelled:
		return model.DeviceCommand{}, errors.New("command was cancelled before receipt arrived")
	case model.CommandFailed:
		return model.DeviceCommand{}, errors.New("command failed before receipt arrived")
	case model.CommandSettled:
		return model.DeviceCommand{}, errors.New("duplicate settlement for command")
	}
	return command, nil
}

// MarkSettled is the committing half of Resolve. It is reached only after the
// caller has verified the receipt and advanced its own state, so it flips the
// command exactly once. A command that became terminal in the meantime (for
// example cancelled by an emergency stop) is left untouched.
func (c *Commands) MarkSettled(commandID uuid.UUID, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	if command.State.Terminal() {
		return model.DeviceCommand{}, fmt.Errorf("command %s is already terminal", commandID)
	}
	if err := command.SetState(model.CommandSettled, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	return command, nil
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
