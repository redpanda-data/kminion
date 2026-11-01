package minion

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileRegex(t *testing.T) {
	tests := []struct {
		name      string
		expr      string
		matches   []string
		noMatches []string
	}{
		{name: "single slash is a literal", expr: "/", matches: []string{"/"}, noMatches: []string{"a"}},
		{name: "slashes mark a regex", expr: "/a.*/", matches: []string{"abc"}, noMatches: []string{"bcd"}},
		{name: "plain value matches the whole name", expr: "abc", matches: []string{"abc"}, noMatches: []string{"abcd"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			regex, err := compileRegex(tt.expr)
			require.NoError(t, err)
			for _, s := range tt.matches {
				require.True(t, regex.MatchString(s), s)
			}
			for _, s := range tt.noMatches {
				require.False(t, regex.MatchString(s), s)
			}
		})
	}
}
