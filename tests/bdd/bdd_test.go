// Package bdd runs the Gherkin feature files under ./features through
// godog, in Strict mode so an undefined or pending step fails the build.
package bdd

import (
	"testing"

	"github.com/cucumber/godog"
)

func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: func(sc *godog.ScenarioContext) {
			registerDeliverySteps(sc)
			registerConfigSteps(sc)
			registerOperatorSteps(sc)
		},
		Options: &godog.Options{Format: "pretty", Paths: []string{"features"}, Strict: true, TestingT: t},
	}
	if suite.Run() != 0 {
		t.Fatal("feature scenarios failed")
	}
}
