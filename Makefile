GO          ?= go
GATED       := ./internal/config/... ./internal/stream/... ./internal/protocol/... ./internal/provision/... ./internal/delivery/... ./internal/control/... ./internal/activity/... ./internal/adapters/operator/... ./internal/adapters/httpdelivery/... ./internal/adapters/debezium/...
COVER_MIN   ?= 90
MUTATE_MIN  ?= 90
FUZZTIME    ?= 30s
GREMLINS    := $(GO) run github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
comma       := ,
empty       :=
space       := $(empty) $(empty)

.PHONY: lint test unit bdd contract regression architecture fuzz cover mutation integration quality

lint:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	$(GO) vet ./...
	$(GO) vet -tags=integration ./tests/integration

test:
	$(GO) test -count=1 -race ./...

unit:
	$(GO) test -count=1 ./tests/unit/... ./internal/... ./cmd/...

bdd:
	$(GO) test -count=1 ./tests/bdd

contract:
	$(GO) test -count=1 ./tests/contract

architecture:
	$(GO) test -count=1 ./tests/architecture

regression:
	$(GO) test -count=1 ./tests/regression

fuzz:
	$(GO) test -run='^$$' -fuzz=FuzzDecodeAcknowledgement -fuzztime=$(FUZZTIME) ./tests/unit/protocol
	$(GO) test -run='^$$' -fuzz=FuzzMatchesFilter -fuzztime=$(FUZZTIME) ./tests/unit/protocol
	$(GO) test -run='^$$' -fuzz=FuzzDecode -fuzztime=$(FUZZTIME) ./tests/unit/adapters/debezium

# Unit, BDD, contract and regression tests all count toward the gated packages.
cover:
	$(GO) test -count=1 -race -coverpkg=$(subst $(space),$(comma),$(GATED)) -coverprofile=coverage.out ./...
	@$(GO) tool cover -func=coverage.out | awk -v min=$(COVER_MIN) '/^total:/ { sub("%","",$$3); \
		printf "coverage %.1f%% (floor %d%%)\n", $$3, min; if ($$3+0 < min) exit 1 }'

# Gremlins derives each mutant's timeout from its own baseline test run. A warm
# test cache makes that baseline near zero, every mutant then times out, and
# gremlins exits 0 with nothing killed. So: cold cache, a generous coefficient,
# and a run that killed nothing fails instead of passing. Gremlins v0.6.0 also
# exits 0 below --threshold-efficacy, so the floor is enforced here.
# Black-box unit tests live in tests/unit, outside the mutated package, so each
# mutant runs the whole offline suite (--integration) and coverage is attributed
# across packages (--coverpkg).
mutation:
	@$(GO) clean -testcache
	@for p in $(GATED:/...=); do \
		out=$$($(GREMLINS) unleash --integration --coverpkg=$$p/... --timeout-coefficient 10 $$p 2>&1) || { echo "$$out"; exit 1; }; \
		echo "$$out"; \
		if echo "$$out" | grep -q 'Killed: 0,'; then echo "mutation: nothing was killed in $$p, so this run proves nothing"; exit 1; fi; \
		echo "$$out" | awk -v min=$(MUTATE_MIN) -v p=$$p '/^Test efficacy:/ { sub("%","",$$3); seen=1; \
			if ($$3+0 < min) { printf "mutation: %s efficacy %.2f%% is below the %d%% floor\n", p, $$3, min; exit 1 } } \
			END { if (!seen) { print "mutation: no efficacy reported for " p; exit 1 } }' || exit 1; \
	done

integration:
	$(GO) test -tags=integration -count=1 -p 1 -timeout 25m ./tests/integration

quality: lint cover mutation
