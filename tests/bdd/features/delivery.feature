Feature: HTTP delivery contract
  docs/specification.md:75-98 and :109

  Background:
    Given a destination "projection" with Basic credentials "user" and "pass"
    And a source "events"

  Scenario: A success acknowledgement delivers the record
    Given the receiver answers 200 with body:
      """
      {"result":{"success":{}}}
      """
    When the engine delivers the payload '{"event_name":"Created"}'
    Then the outcome is "delivered" after 1 attempt
    And the request was a Basic-authenticated JSON POST
    And the request body matches the delivery contract envelope around that payload

  Scenario Outline: Anything short of a terminal acknowledgement is retried
    Given the receiver first answers <status> with body '<body>'
    And the receiver then answers 200 with a success acknowledgement
    When the engine delivers the payload '{"event_name":"Created"}'
    Then the outcome is "delivered" after 2 attempts

    Examples:
      | status | body                                                                     |
      | 500    |                                                                          |
      | 401    | {"result":{"success":{}}}                                                |
      | 200    |                                                                          |
      | 200    | {not json                                                                |
      | 200    | {"result":{}}                                                            |
      | 200    | {"result":{"success":null}}                                              |
      | 200    | {"result":{"error":{"policy":"must_retry","class":"c","description":"d"}}} |
      | 200    | {"result":{"error":{"policy":"keep_going"}}}                             |

  Scenario: keep_going is a terminal skip
    Given the receiver answers 200 with body:
      """
      {"result":{"error":{"policy":"keep_going","class":"c","description":"d"}}}
      """
    When the engine delivers the payload '{"event_name":"Created"}'
    Then the outcome is "skipped" after 1 attempt

  Scenario: A response larger than 64 KiB is retried
    Given the receiver first answers 200 with a body larger than 64 KiB
    And the receiver then answers 200 with a success acknowledgement
    When the engine delivers the payload '{"event_name":"Created"}'
    Then the outcome is "delivered" after 2 attempts

  Scenario: Every retry wait is between 1 and 60 seconds
    Given the receiver fails 12 times then answers 200 with a success acknowledgement
    When the engine delivers the payload '{"event_name":"Created"}'
    Then the outcome is "delivered" after 13 attempts
    And every recorded sleep was between 1 and 60 seconds

  Scenario: Cancelling stops the retry loop without an outcome
    Given the receiver always answers 500
    And the next retry sleep cancels the delivery
    When the engine delivers the payload '{"event_name":"Created"}'
    Then the delivery failed with a cancellation error
    And the outcome is the zero value
    And the receiver request count is 1

  Scenario: Diagnostic headers identify the record
    Given a record with topic "events", partition 3, offset 42, event id "evt-1", generation 2, replay true
    And the receiver answers 200 with a success acknowledgement
    When the engine delivers the record
    Then the outcome is "delivered" after 1 attempt
    And the request carried the diagnostic headers for that record
