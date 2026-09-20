package stream

import "strconv"

// Generation numbers a stream generation. Zero is invalid; the first is 1.
type Generation int

func (g Generation) Valid() bool    { return g >= 1 }
func (g Generation) String() string { return strconv.Itoa(int(g)) }
