Feature: Operator control API
  docs/specification.md:141-142

  Background:
    Given an operator server with token "secret-token" and subscriptions "orders", "invoices"

  Scenario: Health endpoints need no token
    When I GET "/health/live" with no token
    Then the response status is 200
    When I GET "/health/ready" with no token
    Then the response status is 200

  Scenario: Status requires the right token
    When I GET "/v1/status" with no token
    Then the response status is 401
    When I GET "/v1/status" with token "wrong-token"
    Then the response status is 401
    When I GET "/v1/status" with token "secret-token"
    Then the response status is 200

  Scenario: Pausing a subscription is visible in the list and resume restores it
    When I POST "/v1/subscriptions/orders/pause" with token "secret-token"
    Then the response status is 200
    And the subscription list shows "orders" as "paused"
    And the subscription list shows "invoices" as "running"
    When I POST "/v1/subscriptions/orders/resume" with token "secret-token"
    Then the response status is 200
    And the subscription list shows "orders" as "running"

  Scenario: An unknown subscription is not found
    When I POST "/v1/subscriptions/missing/pause" with token "secret-token"
    Then the response status is 404

  Scenario: Recent engine activity requires authentication
    When I GET "/v1/logs" with no token
    Then the response status is 401
    When I GET "/v1/logs" with token "secret-token"
    Then the response status is 200
    When I GET "/v1/logs?after=broken" with token "secret-token"
    Then the response status is 400
