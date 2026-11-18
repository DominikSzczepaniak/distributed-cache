//go:build race

package harness

// raceEnabled makes the harness build the subprocess binaries with -race when
// the tests themselves run under -race.
const raceEnabled = true
