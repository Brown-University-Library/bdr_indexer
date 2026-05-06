# Plan: Prevent Ancestor-Derived Image Accessibility Alt Text

_Created: 2026-05-06; scope: planning only, no code changes._

## Purpose

Ensure `image_accessibility_alt_text_ssi` is derived only from metadata directly associated with the object being indexed. Preserve the existing ancestor fallback behavior for all other metadata fields.

The issue is narrow: `SolrDocBuilder.descriptive_data()` currently gets `MODS`, `DWC`, and `TEI` via `get_metadata_bytes_to_index()`. That method checks the current object's active files first, then checks eligible related objects through `StorageObject.ancestors`. This existed before the image-accessibility work and should remain in place for normal descriptive indexing.

However, after the recent alt-text implementation, inherited MODS or DWC can emit `image_accessibility_alt_text_ssi`. That means a child image without direct MODS/DWC alt text can receive alt text parsed from a parent or original object's metadata. The new requirement is to prevent that for the canonical alt-text field only.

## Directives Reviewed

Reviewed:

- `PLAN__a11y_bdr_indexer_implementation.md`
- `AGENTS.toml`

Applicable directives for this older project:

- Preserve the existing older architecture.
- Use standard-library `unittest`.
- Keep changes focused.
- Add type hints for new or changed functions where compatible with this project.
- Use existing `ruff.toml` style: single quotes, 125-character line length, Python 3.8 target.
- Ignore newer-version `AGENTS.toml` directions that conflict with this repo's current older setup, especially Python 3.12-only assumptions, `pyproject.toml`, and `uv`-only execution.

Relevant existing plan intent:

- `image_accessibility_alt_text_ssi` is the canonical Solr field.
- MODS alt-text notes should not leak into generic public note handling.
- DWC `dynamicProperties` may extract alt text.
- `image_accessibility_alt_text.json` is a direct object file fallback.
- Existing metadata indexing behavior should be preserved for unrelated MODS, DWC, and TEI fields.

## Current Behavior

`StorageObject.get_metadata_bytes_to_index(ds_id)` behaves like this:

1. If the current object has the requested datastream, return the current object's file contents.
2. Otherwise, iterate through `self.ancestors`.
3. If an ancestor has the requested datastream, return the ancestor's file contents.

`SolrDocBuilder.descriptive_data()` currently calls that method for:

- `MODS`
- `DWC`
- `TEI`

The recent image-accessibility implementation then lets these source dictionaries emit `image_accessibility_alt_text_ssi`:

- `ModsIndexer.index_notes()` emits the field from `<mods:note type="image_accessibility_alt_text">...`.
- `SimpleDarwinRecordIndexer` emits the field from JSON or valid legacy `dwc:dynamicProperties`.
- `SolrDocBuilder._get_image_accessibility_alt_text_json_index_data()` emits the field from direct `image_accessibility_alt_text.json`.

The JSON source already checks only `self.storage_object.active_file_names`, so it is already direct-object-only.

## Target Behavior

For the object currently being indexed:

- Direct-object MODS may produce `image_accessibility_alt_text_ssi`.
- Direct-object DWC may produce `image_accessibility_alt_text_ssi`.
- Direct-object `image_accessibility_alt_text.json` may produce `image_accessibility_alt_text_ssi`.
- Future direct-object TEI alt-text handling, if implemented, may produce `image_accessibility_alt_text_ssi`.
- Ancestor MODS, DWC, or TEI must not produce `image_accessibility_alt_text_ssi`.
- Ancestor MODS, DWC, or TEI should still contribute all other existing descriptive fields exactly as before.

Example:

- Image child has no `MODS`.
- Parent has `MODS` with `primary_title` and `<mods:note type="image_accessibility_alt_text">Parent description.</mods:note>`.
- Child indexing should still inherit the parent's `primary_title`.
- Child indexing should not receive `image_accessibility_alt_text_ssi` from the parent's MODS note.

## Recommended Implementation

### 1. Add a helper to identify direct metadata

Add a small helper on `StorageObject` or `SolrDocBuilder` to make direct-file checks explicit.

Recommended minimal option on `SolrDocBuilder`:

```python
def _has_direct_metadata(self, ds_id: str) -> bool:
    return ds_id in self.storage_object.active_file_names
```

This keeps `get_metadata_bytes_to_index()` untouched and makes the special alt-text rule local to Solr document assembly.

### 2. Strip ancestor-derived alt text after each metadata source is indexed

Keep using `get_metadata_bytes_to_index()` for `MODS`, `DWC`, and `TEI`, so ordinary metadata inheritance stays intact.

Immediately after indexing each source, remove `image_accessibility_alt_text_ssi` if the source file was not directly present on the current object.

Suggested helper:

```python
def _remove_indirect_image_accessibility_alt_text(self, data: dict, ds_id: str) -> None:
    if ds_id not in self.storage_object.active_file_names:
        data.pop(IMAGE_ACCESSIBILITY_ALT_TEXT_SOLR_FIELD, None)
```

Usage in `descriptive_data()`:

```python
mods_bytes = self.storage_object.get_metadata_bytes_to_index('MODS')
if mods_bytes:
    mods_index = self._get_mods_index_data(mods_bytes)
    self._remove_indirect_image_accessibility_alt_text(mods_index, 'MODS')
```

Apply the same pattern to DWC and TEI.

This approach is intentionally conservative:

- It does not change metadata lookup.
- It does not require changing `ModsIndexer` or `SimpleDarwinRecordIndexer`.
- It does not require source indexers to know whether bytes came from the current object or an ancestor.
- It preserves all non-alt-text fields from inherited metadata.

### 3. Leave `image_accessibility_alt_text.json` direct-only

No behavior change is needed for `_get_image_accessibility_alt_text_json_index_data()`. It already checks only the current object's active files and reads via `self.storage_object.get_file_contents(...)`.

Keep it that way. Do not switch it to `get_metadata_bytes_to_index()`.

### 4. Preserve explicit alt-text precedence

Keep `_resolve_image_accessibility_alt_text()` and the existing precedence order:

1. MODS
2. DWC
3. TEI
4. `image_accessibility_alt_text.json`

The difference is that each source dictionary should already have had ancestor-derived alt text removed before precedence resolution.

This means:

- Direct MODS alt text still wins over direct DWC and JSON.
- Direct DWC alt text still wins over JSON.
- JSON still serves as a direct-object fallback.
- Ancestor MODS or DWC alt text is invisible to the resolver.

## Tests To Add

Add focused tests in `tests/unit/test_solrizer.py`, because the behavior depends on object-vs-ancestor metadata resolution in `SolrDocBuilder`.

### 1. Parent MODS alt text is not inherited

Create:

- Parent object with `MODS` containing:
  - a normal title
  - `<mods:note type="image_accessibility_alt_text">Parent image description.</mods:note>`
- Child image object with `RELS-EXT` `isPartOf` parent and no direct `MODS`.

Expected:

- Child Solr doc still gets inherited normal MODS fields, such as `primary_title`.
- Child Solr doc does not contain `image_accessibility_alt_text_ssi`.

This test should be modeled on the existing `test_solrize_child_object_with_parent_metadata()`.

### 2. Parent DWC alt text is not inherited

Create:

- Parent object with `DWC` containing `dwc:dynamicProperties` with `image_accessibility_alt_text`.
- Child object related to parent and no direct `DWC`.

Expected:

- Child Solr doc still gets ordinary inherited DWC fields, such as `dwc_catalog_number_ssi` or other safe fixture fields.
- Child Solr doc does not contain `image_accessibility_alt_text_ssi`.

### 3. Direct MODS alt text still indexes

Either keep the existing unit coverage or add/adjust a Solr document test to confirm that an object with direct MODS containing the alt-text note still emits `image_accessibility_alt_text_ssi`.

Existing `test_mods_image_accessibility_alt_text_takes_precedence_over_json()` may already cover direct MODS behavior.

### 4. Direct DWC alt text still indexes

Either keep the existing unit coverage or add/adjust a Solr document test to confirm that an object with direct DWC `dynamicProperties` alt text still emits `image_accessibility_alt_text_ssi`.

Existing `test_dwc_image_accessibility_alt_text_takes_precedence_over_json()` may already cover direct DWC behavior.

### 5. Direct JSON fallback still indexes

Keep the existing `test_solrize_image_accessibility_alt_text_json()` behavior.

## Possible Test Adjustments

Review existing tests that combine parent metadata and alt text once the change is implemented. The current tests for parent metadata do not appear to assert alt text, so the new behavior should be additive.

The existing precedence tests should remain valid because they use direct files on the object being indexed.

## Decision Points

### Decision 1: Should direct-object alt text override inherited non-alt text?

Recommended answer: yes, but only for the `image_accessibility_alt_text_ssi` field according to the existing direct-source precedence. This is already how the resolver works once ancestor-derived alt text is removed from source dictionaries.

No user decision needed unless a different precedence order is desired.

### Decision 2: Should inherited MODS alt-text notes remain in generic inherited `note` fields?

Recommended answer: no change should be needed. Because `ModsIndexer.index_notes()` already suppresses the special alt-text note from generic `note` handling, an inherited MODS alt-text note should neither emit `image_accessibility_alt_text_ssi` after stripping nor appear as a generic note.

No user decision needed unless the parent object's alt-text note should be preserved somewhere else for child records, which would conflict with the current goal.

### Decision 3: Should the implementation avoid parsing alt text from ancestors, or parse then strip it?

Recommended answer: parse then strip in `SolrDocBuilder`.

Reasoning:

- It is the smallest scoped change.
- It avoids changing the indexer classes' public behavior when used directly in tests or elsewhere.
- It keeps all ancestor metadata behavior intact.
- It makes the direct-only rule visible where inherited metadata is assembled.

No user decision needed unless avoiding even temporary parsing from ancestor bytes is important for logging/noise reasons.

### Decision 4: Should future TEI alt text follow the same direct-only rule?

Recommended answer: yes. Even though TEI alt-text extraction is currently deferred, the stripping helper should be applied to `tei_index` now so future TEI alt text cannot accidentally inherit from ancestors.

No user decision needed unless TEI gets a different source-of-truth rule later.

## Patch Order For A Future Implementation

1. Add Solrizer test: parent MODS alt text is not inherited, while normal parent MODS metadata still is.
2. Add Solrizer test: parent DWC alt text is not inherited, while normal parent DWC metadata still is.
3. Add the small `SolrDocBuilder` helper that removes `image_accessibility_alt_text_ssi` from source data when the source datastream is not direct.
4. Apply the helper after MODS, DWC, and TEI source indexing in `descriptive_data()`.
5. Run the focused Solrizer tests.
6. Run the full test suite from the project root:

```shell
source ../env/bin/activate
python ./run_tests.py
```

## Non-Goals

- Do not change `StorageObject.get_metadata_bytes_to_index()`.
- Do not remove ancestor fallback for MODS, DWC, or TEI.
- Do not change DWC `dynamicProperties` parsing rules.
- Do not change MODS note handling except as needed through Solr document assembly.
- Do not change the standalone `image_accessibility_alt_text.json` direct-only behavior.
- Do not add TEI alt-text extraction in this change.
- Do not add ingestion validation for missing alt text.

## Original prompt

Goal: ensure `image_accessibility_alt_text` is only derived from the metadata-file directly associated with an image, not from an ancestor.

Context:

- I recently asked about this note:

```
"""One follow-up worth considering: `_get_image_accessibility_alt_text_json_index_data()` only checks the
  current object's active files, while MODS, DWC, and TEI can be inherited from ancestors through `get_metadata_bytes_to_index()`. If standalone alt-text
  JSON is intended to behave like descriptive metadata, ancestor lookup should be made explicit either way."""
```

...and was told that `get_metadata_bytes_to_index()` now -- and previously before my "image_accessibility_alt_text" work -- inspects an ancestor metadata file if one directly associated with a given file (let's say image in this context) is not available.

- I want to keep all metadata processing as it was and is -- regarding ancestors -- EXCEPT that I do _NOT_ want "image_accessibility_alt_text" to be parsed from an ancestor object -- I want it to only be parsed from it's direct-object.

Tasks:

- Review `bdr_indexer/PLAN__a11y_bdr_indexer_implementation.md` to understand the intent of the "image_accessibility_alt_text" recent work that has been implemented.

- Review `bdr_indexer/AGENTS.toml` for coding-directives to follow -- ignoring the newer-version specifications; and pyproject.toml/uv references; for this older-architecture project.

- Create a plan at `bdr_indexer/PLAN__remove_a11y_ancestor_processing.md` -- to ensure that "image_accessibility_alt_text" should _NOT_ be parsed from an ancestor object -- I want it to only be parsed from it's direct-object. But other indexing can continue to refer to an ancestor object.

- Don't make any code-changes yet, just create the plan.

- If there are any decision-points -- clarify them.

- Add this prompt at the bottom under the heading "Original prompt". Thx!
