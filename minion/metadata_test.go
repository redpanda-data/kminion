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
		wantEncodedV1 []byte
	}{
		{
			name:          "topics enabled asks for every topic",
			topicsEnabled: true,
			wantAllTopics: true,
			wantEncodedV1: []byte{0xff, 0xff, 0xff, 0xff}, // null array
		},
		{
			name:          "topics disabled asks for no topics",
			topicsEnabled: false,
			wantAllTopics: false,
			wantEncodedV1: []byte{0x00, 0x00, 0x00, 0x00}, // empty array
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

			// The topic array is nullable from v1 on, so the distinction
			// survives encoding: the body is a length of -1 or of 0.
			req.Version = 1
			assert.Equal(t, tc.wantEncodedV1, req.AppendTo(nil))
		})
	}
}
