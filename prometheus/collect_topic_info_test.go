package prometheus

import "testing"

func TestFormatTopicID(t *testing.T) {
	if got := formatTopicID([16]byte{}); got != "N/A" {
		t.Errorf("zero topic ID: got %q, want %q", got, "N/A")
	}

	// Kafka encodes topic IDs as URL-safe base64 without padding, so '-' and '_' must appear instead of '+' and '/'.
	id := [16]byte{0xfb, 0xff, 0xbf}
	if got, want := formatTopicID(id), "-_-_AAAAAAAAAAAAAAAAAA"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
