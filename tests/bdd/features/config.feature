Feature: Application configuration validation
  docs/specification.md:64-72

  Scenario Outline: Fixture configs are accepted or rejected
    Given SOURCE_PASSWORD and DEST_PASSWORD are set
    When I load the application config "<file>"
    Then it is <result>

    Examples:
      | file                                       | result                                                              |
      | valid-full.yaml                            | accepted                                                            |
      | valid-filter.yaml                          | accepted                                                            |
      | valid-no-filter.yaml                       | accepted                                                            |
      | invalid-empty-filter-values.yaml           | rejected with "filter column and values are required"               |
      | invalid-duplicate-source.yaml              | rejected with "duplicate source id"                                 |
      | invalid-duplicate-destination.yaml         | rejected with "duplicate destination id"                            |
      | invalid-unknown-source.yaml                | rejected with "unknown source"                                      |
      | invalid-unsupported-source-type.yaml       | rejected with "unsupported type"                                    |
      | invalid-unsupported-destination-type.yaml  | rejected with "unsupported type"                                    |
      | invalid-columns-omit-partitioning.yaml     | rejected with "columns must include serialColumn and partitioningColumn" |

  Scenario: A missing environment variable fails the load
    Given DEST_PASSWORD is set
    And SOURCE_PASSWORD is not set
    When I load the application config "valid-full.yaml"
    Then it is rejected with "environment variable SOURCE_PASSWORD is not set"

  Scenario: Redacted configuration never shows a password
    Given SOURCE_PASSWORD and DEST_PASSWORD are set
    When I load the application config "valid-full.yaml"
    Then it is accepted
    And the redacted config does not contain "src-pass"
    And the redacted config does not contain "dest-pass"
