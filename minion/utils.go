package minion

import (
	"fmt"
	"regexp"
	"strings"
)

func (s *Service) IsGroupAllowed(groupName string) bool {
	isAllowed := false
	for _, regex := range s.AllowedGroupIDsExpr {
		if regex.MatchString(groupName) {
			isAllowed = true
			break
		}
	}

	for _, regex := range s.IgnoredGroupIDsExpr {
		if regex.MatchString(groupName) {
			isAllowed = false
			break
		}
	}
	return isAllowed
}

func (s *Service) IsTopicAllowed(topicName string) bool {
	isAllowed := false
	for _, regex := range s.AllowedTopicsExpr {
		if regex.MatchString(topicName) {
			isAllowed = true
			break
		}
	}

	for _, regex := range s.IgnoredTopicsExpr {
		if regex.MatchString(topicName) {
			isAllowed = false
			break
		}
	}
	return isAllowed
}

func compileRegex(expr string) (*regexp.Regexp, error) {
	if strings.HasPrefix(expr, "/") && strings.HasSuffix(expr, "/") {
		substr := expr[1 : len(expr)-1]
		regex, err := regexp.Compile(substr)
		if err != nil {
			return nil, err
		}

		return regex, nil
	}

	// If this is no regex input (which is marked by the slashes around it) then we escape it so that it's a literal
	regex, err := regexp.Compile("^" + expr + "$")
	if err != nil {
		return nil, err
	}
	return regex, nil
}

func compileRegexes(expr []string) ([]*regexp.Regexp, error) {
	compiledExpressions := make([]*regexp.Regexp, len(expr))
	for i, exprStr := range expr {
		expr, err := compileRegex(exprStr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile expression string '%v': %w", exprStr, err)
		}
		compiledExpressions[i] = expr
	}

	return compiledExpressions, nil
}
