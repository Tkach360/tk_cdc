package mapper

import "errors"

var (
	ErrMissingOpeningBrace       = errors.New("missing opening brace '{'")
	ErrMissingClosingBrace       = errors.New("missing closing brace '}'")
	ErrClosingBraceBeforeOpening = errors.New("closing brace appears before opening brace")
	ErrEmptyColumnName           = errors.New("empty column name between braces")
)
