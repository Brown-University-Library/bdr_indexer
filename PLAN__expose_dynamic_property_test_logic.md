# Plan: Expose DWC `dynamicProperties` Alt-Text Parsing Expectations

## Goal

Explore ways another app could reuse the `bdr_indexer` Darwin Core `dynamicProperties` parsing expectations at submission time, so users can be warned before metadata reaches OCFL and later indexing.

This is planning only.

## Approach 1: Data-Driven Fixtures Plus A Small Validator API

Keep parser examples in a machine-readable fixture file, such as YAML:

```yaml
version: 1
cases:
  - id: legacy_valid_pairs
    input: "iucnStatus=vulnerable; distribution=Neuquen, Argentina"
    expected:
      dwc_dynamic_properties_ssi: "iucnStatus=vulnerable; distribution=Neuquen, Argentina"
      image_accessibility_alt_text_ssi: null
      warnings: []

  - id: legacy_alt_text
    input: "iucnStatus=vulnerable; distribution=Neuquen, Argentina; image_accessibility_alt_text= The image description."
    expected:
      dwc_dynamic_properties_ssi: "iucnStatus=vulnerable; distribution=Neuquen, Argentina"
      image_accessibility_alt_text_ssi: "The image description."
      warnings: []

  - id: legacy_ambiguous_alt_text
    input: "iucnStatus=vulnerable; distribution=Neuquen, Argentina; image_accessibility_alt_text= The beginning; the end."
    expected:
      dwc_dynamic_properties_ssi: null
      image_accessibility_alt_text_ssi: null
      warnings:
        - code: invalid_delimiter_splitting
```

Then expose a small `bdr_indexer` function that both tests and external apps can call, for example:

```python
result = parse_dynamic_properties_for_validation(dynamic_properties_text)
```

The result should be structured, not log-driven:

```json
{
  "fields": {
    "dwc_dynamic_properties_ssi": "iucnStatus=vulnerable; distribution=Neuquen, Argentina",
    "image_accessibility_alt_text_ssi": "The image description."
  },
  "warnings": []
}
```

Benefits:

- Unit tests and submission-time validation use the same examples.
- Warning codes become stable and user-interface friendly.
- The indexer can still log warnings internally, but callers do not need to scrape logs.

Tradeoff:

- Requires separating pure parsing from Solr indexing side effects. That is probably worthwhile if another app will depend on this behavior.

## Approach 2: Contract File Only, With Independent Implementations

Publish the expected behavior as JSON/YAML contract data and let each app implement its own validator against that contract.

Example JSON shape:

```json
{
  "version": 1,
  "field": "dwc:dynamicProperties",
  "canonical_alt_text_field": "image_accessibility_alt_text_ssi",
  "cases": [
    {
      "id": "legacy_invalid_bare_string",
      "input": "foo",
      "expected_fields": {},
      "expected_warnings": [
        {"code": "invalid_key_pair"}
      ]
    }
  ]
}
```

Benefits:

- Avoids importing `bdr_indexer` into a submission app if dependency weight, Python version, or environment setup is a concern.
- The contract is easy to review across repositories.
- The submission app can generate user-facing messages in its own style.

Tradeoff:

- Parser implementations can drift. To reduce that risk, run the same contract file in both repos' CI and version the contract.

## Approach 3: Thin HTTP Or CLI Validation Endpoint

If importing `bdr_indexer` directly is awkward, expose a small validation command or service around the shared parser:

```bash
python -m bdr_solrizer.validate_dynamic_properties --format json
```

Input:

```json
{
  "dynamic_properties": "iucnStatus=vulnerable; distribution=Neuquen, Argentina; image_accessibility_alt_text= The image description."
}
```

Output:

```json
{
  "valid_for_indexing": true,
  "fields": {
    "dwc_dynamic_properties_ssi": "iucnStatus=vulnerable; distribution=Neuquen, Argentina",
    "image_accessibility_alt_text_ssi": "The image description."
  },
  "warnings": []
}
```

Benefits:

- The submission app does not need to link directly against indexer internals.
- The parser remains owned by `bdr_indexer`.

Tradeoff:

- Adds deployment/runtime coordination if implemented as HTTP.
- A CLI avoids a service but still requires packaging and environment agreement.

## Recommendation

Prefer Approach 1 if the other app can safely import a small, dependency-light module from `bdr_indexer`: extract the parsing logic into a pure function, return structured warnings, and drive both unit tests and submission validation from shared YAML/JSON fixtures.

Prefer Approach 2 if cross-repo dependency management is likely to be painful: keep a versioned contract file and run it in both apps' tests.

In either case, avoid making the submission app call `unittest` tests directly. Tests are good evidence of behavior, but a validation-facing API should return stable fields and warning codes that can be shown to users.

## Original prompt

question -- if I wanted to use these dynamic-property / alt-text indexer-parsing tests for validation in another app -- to alert a user on submission -- what might be a way to encode these in json or yaml?

I'm thinking that another app could import bdr-indexer at, for example, submission time -- not to index, but to call an endpoint to get the tests to perform a version of the tests on the incoming material.

Don't change any code, just think of a couple of approaches to this.

Save the writeup to `bdr_indexer/PLAN__expose_dynamic_property_test_logic.md` .

---
