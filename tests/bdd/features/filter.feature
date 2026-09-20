Feature: Filter delivery contract
  docs/specification.md:96

  Background:
    Given a destination "projection" with Basic credentials "user" and "pass"
    And a source "events"
    And the receiver answers 200 with a success acknowledgement

  Scenario Outline: Filter evaluation follows the delivery contract
    Given the destination filter is on column "<column>" with values "<values>"
    When the engine delivers the payload '<payload>'
    Then the outcome is "<outcome>"
    And the receiver request count is <requests>

    Examples:
      | column     | values  | payload                  | outcome   | requests |
      | event_name | Created | {"event_name":"Created"} | delivered | 1        |
      | event_name | Created | {"event_name":"Deleted"} | filtered  | 0        |
      | event_name | Created | {"other":1}               | delivered | 1        |
      | event_name | Created | {"event_name":3}          | delivered | 1        |
      | event_name | Created | "just text"               | delivered | 1        |
      |            |         | {"event_name":"Deleted"} | delivered | 1        |
