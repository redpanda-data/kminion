package minion

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestService_metadataRequest(t *testing.T) {
	tt := []struct {
		name          string
		topicsEnabled bool
		wantAllTopics bool
	}{
		{
			name:          "topics enabled asks for every topic",
			topicsEnabled: true,
			wantAllTopics: true,
		},
		{
			name:          "topics disabled asks for no topics",
			topicsEnabled: false,
			wantAllTopics: false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{Cfg: Config{Topics: TopicConfig{Enabled: tc.topicsEnabled}}}

			req := s.metadataRequest()

			// A nil topic list asks the broker for every topic, an empty but
			// non-nil list asks for none. That distinction is the whole point.
			if tc.wantAllTopics {
				assert.Nil(t, req.Topics)
			} else {
				assert.NotNil(t, req.Topics)
				assert.Empty(t, req.Topics)
			}
		})
	}
}
