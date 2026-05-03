# Plan: `bdr_indexer` Image Accessibility Alt Text Implementation

_Created: 2026-05-01; scope: planning only, no code changes._

## Purpose

Update `bdr_indexer` so BDR indexing emits the canonical Solr field:

```text
image_accessibility_alt_text_ssi
```

for the new `image_accessibility_alt_text` data element. This repo is responsible for turning OCFL-backed object metadata into Solr add/update payloads. It should normalize alt text from supported source metadata into one source-agnostic Solr field, and it should prevent the MODS typed note from leaking into generic public note handling.

This plan was developed in part from the following files in the `bdr_ecosystem_project`:

- `bdr_ecosystem_project/bdr_analyses/z_temp_impage_a11y_prompt.md`
- `bdr_ecosystem_project/bdr_analyses/image_a11y_description_plan.md`
- `bdr_ecosystem_project/bdr_repos/bdr_uploader_hub_project/AGENTS.md`
- `bdr_ecosystem_project/AGENT_info/bdr_indexer__AGENT_INDEX.yaml`

## Coding Directives To Carry Forward

- Be aware this project uses Python-3.8.9. 
- Add type hints for new or changed functions.
- If supported by this old version of python -- prefer builtin generics and `str | None`.
- Keep functions explicit, avoid nested function definitions, and use focused tests.

Run tests, locally or on dev (not production) from the indexer project root (containing the `.git` directory) via:

```shell
source ../env/bin/activate
python run ./run_tests.py
```

If granular tests are useful during implementation, use standard `unittest` module paths, but finish with the full test command above.

## Current Code Landmarks

Primary implementation files:

- `bdr_indexer/bdr_solrizer/solrdocbuilder.py`
- `bdr_indexer/bdr_solrizer/indexers/modsindexer.py`
- `bdr_indexer/bdr_solrizer/indexers/dwcindexer.py`
- `bdr_indexer/bdr_solrizer/indexers/teiindexer.py`

Primary tests:

- `bdr_indexer/tests/unit/test_modsindexer.py`
- `bdr_indexer/tests/unit/test_dwcindexer.py`
- `bdr_indexer/tests/unit/test_solrizer.py`
- `bdr_indexer/tests/unit/test_teiindexer.py`

Important current behavior:

- `SolrDocBuilder.descriptive_data()` reads `MODS`, `DWC`, and `TEI` metadata from the object or eligible ancestors, then merges all indexer output into one descriptive dict.
- `ModsIndexer.index_notes()` currently appends every MODS note to the generic `note` field and creates `mods_note_<type>_ssim` / `mods_note_<displayLabel>_ssim` dynamic fields.
- `SimpleDarwinRecordIndexer.index_data()` currently serializes every Darwin Core field to a `dwc_*_ssi` field, including `dwc_dynamic_properties_ssi`, but does not parse JSON from `dynamicProperties`.
- `TEIIndexer` exists, but the ecosystem plan treats TEI alt-text extraction as deferred pending stakeholder approval of a TEI representation.
- Solr config already supports `image_accessibility_alt_text_ssi` through the existing `*_ssi` dynamic field. No `bdr_solr_conf` schema change is expected.

## Target Behavior

1. If MODS contains:

   ```xml
   <mods:note type="image_accessibility_alt_text">The image-accessibility description.</mods:note>
   ```

   index:

   ```python
   {"image_accessibility_alt_text_ssi": "The image-accessibility description."}
   ```

2. Do not append that MODS note to the generic `note` field.

3. Do not create `mods_note_image_accessibility_alt_text_ssim` for that note type.

4. If DWC contains JSON in `dwc:dynamicProperties`, for example:

   ```xml
   <dwc:dynamicProperties>{"image_accessibility_alt_text":"The image-accessibility description."}</dwc:dynamicProperties>
   ```

   parse it and index the same canonical Solr field.

5. If API work stores direct alt text in OCFL as `image_accessibility_alt_text.json`, teach `SolrDocBuilder` to read it and index the same canonical field.

6. Preserve existing metadata indexing behavior for all unrelated MODS notes, DWC fields, and TEI fields.

7. Do not copy alt text to general `text`, do not add `image_accessibility_alt_text_tesim`, and do not make it searchable as broad full text in this implementation.

## Recommended Implementation Steps

### 1. Add Shared Constants

Add module-level constants near the relevant implementation site. A small dedicated helper module is probably unnecessary; keep this scoped unless duplication grows.

Recommended names:

```python
IMAGE_ACCESSIBILITY_ALT_TEXT_KEY = "image_accessibility_alt_text"
IMAGE_ACCESSIBILITY_ALT_TEXT_SOLR_FIELD = "image_accessibility_alt_text_ssi"
IMAGE_ACCESSIBILITY_ALT_TEXT_JSON_DS_ID = "image_accessibility_alt_text.json"
```

If constants are needed by both `modsindexer.py` and `dwcindexer.py`, either duplicate two obvious local constants or add them to `bdr_solrizer/indexers/common.py`. Prefer the smallest clear option during implementation.

### 2. Update MODS Note Indexing

File:

- `bdr_indexer/bdr_solrizer/indexers/modsindexer.py`

Change `ModsIndexer.index_notes()` so it special-cases notes where:

```python
note.type == "image_accessibility_alt_text"
```

Recommended behavior:

- Strip and normalize the note text using existing `CommonIndexer.append_field()` behavior.
- Set or append `image_accessibility_alt_text_ssi`.
- Skip the rest of the generic-note logic for this note.

Implementation detail:

- Because `image_accessibility_alt_text_ssi` is single-valued in Solr, use `set_field()` or assign one normalized string, not a multi-value list.
- Multiple MODS alt-text notes should be rejected upstream, not by `bdr_indexer`. If the indexer nevertheless sees multiple alt-text notes, log a warning and use the first non-empty value. This matches the single-valued target contract and avoids silently joining multiple descriptions.
- If `note.text` is empty, skip it.

Tests to add in `test_modsindexer.py`:

- A MODS alt-text note indexes `image_accessibility_alt_text_ssi`.
- That note is absent from `note`.
- `mods_note_image_accessibility_alt_text_ssim` is not created.
- Existing generic notes and typed notes still behave as before.
- Multiple alt-text notes use the first non-empty value and log a warning.

### 3. Parse DWC `dynamicProperties`

File:

- `bdr_indexer/bdr_solrizer/indexers/dwcindexer.py`

Add JSON-aware handling for `self.dwc.dynamic_properties` after the normal DWC field loop. Preserve the current behavior for non-JSON `dynamicProperties`, but handle valid JSON specially.

Recommended behavior:

- If `dynamicProperties` is not valid JSON, keep the existing behavior exactly as it is today: index the whole raw string to `dwc_dynamic_properties_ssi`.
- If `dynamicProperties` is valid JSON, parse it as an object/dict.
- If that JSON object contains a non-empty string value at `image_accessibility_alt_text`, add that value to `image_accessibility_alt_text_ssi`.
- Do not include the `image_accessibility_alt_text` JSON key-value pair in `dwc_dynamic_properties_ssi`.
- For any other key-value pairs in the JSON object, add their content to `dwc_dynamic_properties_ssi`.
- If the JSON object contains only `image_accessibility_alt_text`, do not emit `dwc_dynamic_properties_ssi` from that JSON content.
- Use a deterministic serialization for JSON-derived non-alt-text dynamic properties. Recommended format: store a JSON string containing all non-alt-text key-value pairs, with sorted keys. For example:

  ```xml
  <dwc:dynamicProperties>{"image_accessibility_alt_text":"The image-accessibility description.","iucnStatus":"vulnerable","distribution":"Neuquen, Argentina"}</dwc:dynamicProperties>
  ```

  should emit:

  ```python
  {
      "image_accessibility_alt_text_ssi": "The image-accessibility description.",
      "dwc_dynamic_properties_ssi": "{\"distribution\":\"Neuquen, Argentina\",\"iucnStatus\":\"vulnerable\"}"
  }
  ```

- Strip surrounding whitespace from the alt-text value through local normalization or common helper behavior.
- If JSON is invalid, log a warning, do not fail the whole object, and use the legacy raw-string behavior by indexing the whole value to `dwc_dynamic_properties_ssi`.
- If JSON is valid but malformed for the expected object/dict shape, or contains a non-string `image_accessibility_alt_text` value, do not fail the whole object. Log a warning if a logger is readily available; otherwise skip the unsupported JSON-specific part and cover the non-failing behavior with a test.
- Important existing-test context: `test_dwcindexer.py` already includes a fixture with non-JSON `dynamicProperties`, currently:

  ```xml
  <dwc:dynamicProperties>iucnStatus=vulnerable; distribution=Neuquen, Argentina</dwc:dynamicProperties>
  ```

  That existing behavior should continue. Adding JSON handling for alt text must not make older or non-JSON DWC records fail indexing; those records should continue to emit the ordinary raw `dwc_dynamic_properties_ssi` field and simply not emit `image_accessibility_alt_text_ssi`.

Tests to add in `test_dwcindexer.py`:

- JSON `dynamicProperties` emits `image_accessibility_alt_text_ssi`.
- JSON `dynamicProperties` does not include the `image_accessibility_alt_text` key-value pair in `dwc_dynamic_properties_ssi`.
- JSON `dynamicProperties` does include other JSON key-value pairs in `dwc_dynamic_properties_ssi`.
- JSON `dynamicProperties` containing only `image_accessibility_alt_text` does not emit `dwc_dynamic_properties_ssi`.
- Existing non-JSON `dynamicProperties` still indexes as `dwc_dynamic_properties_ssi`, does not raise, and logs a warning when JSON parsing fails.
- Empty or non-string values do not emit the canonical field.

### 4. Index API-Managed JSON Datastream

File:

- `bdr_indexer/bdr_solrizer/solrdocbuilder.py`

Add a small helper on `SolrDocBuilder`, for example:

```python
def _get_image_accessibility_alt_text_json_index_data(self) -> dict[str, str]:
    ...
```

Recommended behavior:

- If `image_accessibility_alt_text.json` is active on the storage object, read it with `storage_object.get_file_contents(...)`.
- Parse JSON as UTF-8.
- Accept the direct logical contract:

  ```json
  {"image_accessibility_alt_text": "The image-accessibility description."}
  ```

- Return `{}` for missing, invalid, empty, or unsupported values, while logging a warning for invalid JSON or unsupported shapes.
- Merge this data in `descriptive_data()`.

Recommended precedence:

1. MODS typed note
2. DWC `dynamicProperties`
3. TEI later, after approval
4. API-managed `image_accessibility_alt_text.json`

Rationale: the metadata file for the image should be the source of truth. The API-managed JSON datastream is useful as a source-agnostic fallback, but it should not override alt text provided by MODS, DWC, or a future approved TEI representation.

Implementation detail:

- Current `descriptive_data()` merges MODS, then DWC, then TEI with `dict.update()`, meaning later sources overwrite earlier fields when they emit the same key. Do not let that accidental update order control `image_accessibility_alt_text_ssi`.
- Recommended approach: collect each source's descriptive dict, merge ordinary fields as before, and resolve only `image_accessibility_alt_text_ssi` through an explicit helper that chooses the first non-empty value in the precedence order above.
- If a future implementation keeps the current update-style flow, then lower-priority sources must only set `image_accessibility_alt_text_ssi` when it is not already present. This is more fragile than an explicit precedence helper, but it preserves the required source-of-truth behavior.

Tests to add in `test_solrizer.py`:

- An OCFL object with `image_accessibility_alt_text.json` includes `image_accessibility_alt_text_ssi` in the Solr add doc.
- Invalid JSON does not break indexing.
- If MODS and JSON are present, MODS wins.
- If MODS and DWC are present, MODS wins.
- If DWC and JSON are present, DWC wins.

### 5. Defer TEI Extraction Unless Scope Changes

File:

- `bdr_indexer/bdr_solrizer/indexers/teiindexer.py`

Do not implement TEI alt-text extraction in the first bdr_indexer slice unless a stakeholder-approved TEI representation is included in the implementation ticket. The overview plan lists possible TEI patterns:

- `figDesc[@ana="#image_accessibility_alt_text"]`
- `graphic/desc[@type="image_accessibility_alt_text"]`

If TEI is later added, implement it in `TEIIndexer.index_data()` as a dedicated method and add tests to `test_teiindexer.py`.

### 6. Do Not Add Validation In `bdr_indexer`

`bdr_indexer` should index available metadata. It should not reject objects lacking alt text. Presence and 250-character enforcement belong in ingestion/API code before metadata reaches OCFL. Indexer tests should therefore check extraction and non-fatal behavior, not hard validation failures.

## Suggested Patch Order

1. Add MODS test coverage, then update `ModsIndexer.index_notes()`.
2. Add DWC test coverage, then update `SimpleDarwinRecordIndexer`.
3. Add Solr document builder tests for `image_accessibility_alt_text.json`, then update `SolrDocBuilder.descriptive_data()`.
4. Run `python run ./run_tests.py` from `bdr_indexer`.
5. If tests fail because this repo still targets older syntax in CI, adapt type hints conservatively while respecting the current AGENTS guidance as much as possible.

## Reindexing And Rollout

After deployment:

- Reindex image objects whose MODS already contains `note type="image_accessibility_alt_text"`.
- Reindex DWC objects after any DWC `dynamicProperties` alt-text submissions are in OCFL.
- Reindex objects with API-managed `image_accessibility_alt_text.json` after the API side starts storing that datastream.

Useful Solr audit queries after deployment:

```text
object_type:image AND image_accessibility_alt_text_ssi:*
object_type:image AND -image_accessibility_alt_text_ssi:*
```

The second query is useful for identifying image records that still need metadata or reindexing. It is not proof that ingestion validation failed, because legacy image objects may predate the requirement.

## Context For A Future Session

- The central ecosystem plan recommends `image_accessibility_alt_text_ssi` as the canonical field and explicitly says downstream code should not read `note` or `mods_note_image_accessibility_alt_text_ssim`.
- The existing Solr schema already has `*_ssi`; no Solr schema edit should be necessary for this indexer change.
- The existing `note` field is copied to Solr `text` in `bdr_solr_conf/managed-schema`, so keeping the MODS alt-text note out of generic `note` also prevents accidental broad text search behavior.
- The existing DWC test fixture already exercises `dynamicProperties`, but with a non-JSON value. Future JSON handling for alt text must preserve that older/non-JSON behavior: index the raw value to `dwc_dynamic_properties_ssi`, do not raise, and do not emit the canonical alt-text field. Valid JSON `dynamicProperties` should be split: `image_accessibility_alt_text` goes to `image_accessibility_alt_text_ssi`, while all other JSON key-value pairs continue into `dwc_dynamic_properties_ssi`.
- The initial implementation should avoid broad refactors. The relevant behavior is concentrated in `ModsIndexer`, `SimpleDarwinRecordIndexer`, and `SolrDocBuilder`.

## Open Decisions

- Confirm exact stored shape of API-managed `image_accessibility_alt_text.json` once `bdr_apis_project` work exists. The plan assumes the logical single-string JSON contract.

## Addendum: DWC Legacy `dynamicProperties` Semicolon Parsing

Add a follow-up DWC implementation slice for legacy, non-JSON `dwc:dynamicProperties` values. The conceptual intent is to support the existing semicolon-delimited `key=value` style conservatively: parse only the unambiguous leading `key=value` segments, extract `image_accessibility_alt_text` when it appears as a valid segment, preserve the safe non-alt-text prefix in `dwc_dynamic_properties_ssi`, and stop with a warning once delimiter splitting becomes ambiguous or invalid.

Add a brief module docstring to both:

- `bdr_indexer/bdr_solrizer/indexers/dwcindexer.py`
- `bdr_indexer/tests/unit/test_dwcindexer.py`

The docstrings should describe the conceptual intent above in one or two sentences, so future maintainers understand why legacy semicolon parsing is intentionally conservative and why warnings are expected for ambiguous delimiter splitting.

NOTE: where functionality conflicts with previous implementation, prioritize the addendum implementation below.

Additional test requirements for `tests/unit/test_dwcindexer.py`:

1. Legacy `dynamicProperties` without alt text and with valid semicolon-delimited key-value pairs:

   ```xml
   <dwc:dynamicProperties>iucnStatus=vulnerable; distribution=Neuquen, Argentina</dwc:dynamicProperties>
   ```

   Expected:

   ```python
   dwc_dynamic_properties_ssi == "iucnStatus=vulnerable; distribution=Neuquen, Argentina"
   image_accessibility_alt_text_ssi is not emitted
   no warning is logged
   ```

2. Legacy `dynamicProperties` containing an invalid bare string:

   ```xml
   <dwc:dynamicProperties>foo</dwc:dynamicProperties>
   ```

   Expected:

   ```python
   dwc_dynamic_properties_ssi is not emitted
   image_accessibility_alt_text_ssi is not emitted
   warning is logged about invalid key-pair
   ```

3. Legacy `dynamicProperties` with a valid alt-text key-value segment:

   ```xml
   <dwc:dynamicProperties>iucnStatus=vulnerable; distribution=Neuquen, Argentina; image_accessibility_alt_text= The image description.</dwc:dynamicProperties>
   ```

   Expected:

   ```python
   dwc_dynamic_properties_ssi == "iucnStatus=vulnerable; distribution=Neuquen, Argentina"
   image_accessibility_alt_text_ssi == "The image description."
   no warning is logged
   ```

4. Legacy `dynamicProperties` where the alt-text value itself appears to contain the semicolon delimiter:

   ```xml
   <dwc:dynamicProperties>iucnStatus=vulnerable; distribution=Neuquen, Argentina; image_accessibility_alt_text= The beginning; the end.</dwc:dynamicProperties>
   ```

   Expected:

   ```python
   dwc_dynamic_properties_ssi is not emitted
   image_accessibility_alt_text_ssi is not emitted
   warning is logged about invalid delimiter splitting
   ```

   Rationale: the legacy format cannot distinguish a semicolon inside the alt-text sentence from a semicolon delimiter. Don't try to guess the intended structure.

5. Legacy `dynamicProperties` with valid leading key-value segments, then an invalid segment, then another valid-looking segment:

   ```xml
   <dwc:dynamicProperties>iucnStatus=vulnerable; distribution=Neuquen, Argentina; box 123; foo=bar</dwc:dynamicProperties>
   ```

   Expected:

   ```python
   dwc_dynamic_properties_ssi is not emitted
   image_accessibility_alt_text_ssi is not emitted
   warning is logged about invalid delimiter splitting
   ```

   Rationale: Don't try to guess the intended structure.
