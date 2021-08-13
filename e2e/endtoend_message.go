package e2e

import "time"

type EndToEndMessage struct {
	MinionID  string `json:"minionID"`     // unique for each running kminion instance
	MessageID string `json:"messageID"`    // unique for each message
	Timestamp int64  `json:"createdUtcNs"` // when the message was created, unix nanoseconds

	// The following properties are only used within the message tracker
	partition       int
	hasArrived      bool
	failedToProduce bool
}

func (m *EndToEndMessage) creationTime() time.Time {
	return time.Unix(0, m.Timestamp)
}
