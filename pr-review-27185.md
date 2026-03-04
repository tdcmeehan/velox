# Code Review: prestodb/presto#27185 — Presto Lance Connector

**PR:** https://github.com/prestodb/presto/pull/27185
**Author:** jja725
**Branch:** `lance-connector`

This PR introduces a new connector enabling Presto to read from and write to LanceDB datasets, a columnar data format built on Apache Arrow optimized for ML/AI workloads. The implementation uses a split-per-fragment read model and fragment-based commit protocol for writes.

---

## HIGH Severity

### 1. `toPrestoType(ArrowType)` hardcodes array element type as `REAL`
**File:** `LanceColumnHandle.java`

The single-argument `toPrestoType(ArrowType)` overload unconditionally returns `ArrayType(RealType.REAL)` for any `ArrowType.List` or `ArrowType.FixedSizeList`, regardless of the actual element type. The two-argument `toPrestoType(Field)` correctly inspects `field.getChildren()`, but any code path calling the single-argument overload for a list column will silently return the wrong type. This is a data correctness bug waiting to happen — the two overloads are inconsistent and should be unified into a single `Field`-based method, since element type information is only available through `Field`.

### 2. `schemaName` parameter silently ignored in all path methods
**File:** `LanceNamespaceHolder.java`

`getTablePath()`, `tableExists()`, `dropTable()`, and all callers accept a `schemaName` parameter that is never used in path construction:

```java
public String getTablePath(String schemaName, String tableName) {
    return Paths.get(root, tableName + TABLE_PATH_SUFFIX).toUri().toString();
    // schemaName is ignored
}
```

Two tables with the same name in different schemas would collide on disk. The connector currently enforces a single "default" schema, so this isn't a live bug, but the signature is actively misleading. Either remove the parameter (making the single-schema assumption explicit) or use it correctly.

### 3. Static `RootAllocator` with no lifecycle management
**File:** `LanceNamespaceHolder.java`

```java
private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
```

This JVM-wide singleton allocator is never closed. Arrow's `RootAllocator` tracks memory and expects `close()` for proper cleanup. Because it's `static`:
- It survives connector teardown; reloading the connector creates a second `RootAllocator`
- It cannot be mocked in tests
- `Long.MAX_VALUE` cap provides no memory back-pressure at all

**Recommendation:** Inject the allocator via Guice, implement `Closeable`, close during connector shutdown, and set a configurable memory cap.

### 4. Java object serialization for cross-node commit data
**File:** `LancePageSink.java`

```java
public static String serializeFragment(FragmentMetadata fragment) {
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(fragment);
    return Base64.getEncoder().encodeToString(baos.toByteArray());
}
```

Issues:
- **Version brittleness:** Any change to `FragmentMetadata`'s internal fields breaks deserialization during rolling upgrades
- **Security:** `ObjectInputStream.readObject()` is a known deserialization attack vector
- **Naming lie:** The field `LanceCommitTaskData.fragmentsJson` contains Base64-encoded Java serialization blobs, not JSON
- **Performance:** Java `ObjectOutputStream` is one of the slowest serialization mechanisms on the JVM

**Recommendation:** Use Lance's native serialization (protobuf/JSON if available) or define a stable intermediate DTO with Jackson.

### 5. N+1 dataset opens per query
**Files:** `LanceSplitManager.java`, `LanceFragmentPageSource.java`

Split planning opens the dataset to enumerate fragments, then closes it. Each split execution re-opens the dataset. For N fragments: 1 open during planning + N opens during execution = N+1 total opens. `Dataset.open` is not free — it reads the table manifest from storage.

**Recommendation:** Consider a dataset pool keyed by `(schemaName, tableName, version)`, or batch multiple fragments per split to reduce opens.

---

## MEDIUM Severity

### 6. Three divergent type-switch implementations
**Files:** `LanceArrowToPageScanner.java`, `LancePageToArrowConverter.java`, `LanceColumnHandle.java`

All three files contain type-dispatch over the same Presto type set with diverging coverage:
- `LanceArrowToPageScanner` handles `ArrayType` and `TimeStampMicroTZVector`
- `LancePageToArrowConverter` handles neither — writing an array column via INSERT throws `LANCE_TYPE_NOT_SUPPORTED` even though reading works
- `LanceColumnHandle.toArrowType` handles `RowType → ArrowType.Struct`, but neither converter supports Struct

**Recommendation:** Consolidate into a `LanceTypeConverter` utility class so adding a new type requires a single change.

### 7. `commitAppend()` race condition
**File:** `LanceNamespaceHolder.java`

```java
try (Dataset dataset = Dataset.open(...)) {
    Dataset.commit(allocator, tablePath, appendOp, Optional.of(dataset.version()), ...);
}
```

The version is read from a snapshot, then passed to `commit`. Two concurrent workers can read the same version. Whether Lance's `commit` is CAS-with-retry or last-writer-wins is undocumented. This needs clarification and likely retry logic.

### 8. `LanceWritableTableHandle.equals/hashCode` exclude semantically significant fields
**File:** `LanceWritableTableHandle.java`

`equals()` and `hashCode()` only compare `schemaName` and `tableName`, ignoring `schemaJson` and `inputColumns`. Two handles for the same table but different schemas/columns compare as equal, risking wrong-schema commits if Presto caches or deduplicates handles.

### 9. `LancePageSink` buffers all pages in memory; `writeBatchSize` is dead config
**File:** `LancePageSink.java`

All pages accumulate in `List<Page> bufferedPages` until `finish()`. The `writeBatchSize` config property (default 10,000 rows) is declared and documented but never used at runtime — the sink never flushes when the threshold is reached. For large INSERTs, peak memory is doubled (Presto pages + Arrow vectors alive simultaneously).

**Recommendation:** Flush pages to Lance fragments incrementally in `appendPage()` when `writeBatchSize` is reached.

### 10. `LanceSplit` accepts `List<Integer>` but is always constructed with a singleton
**Files:** `LanceSplitManager.java`, `LanceSplit.java`

Every split wraps exactly one fragment ID in a list. If fragment-batching is intended for future use, document it. If not, use `int fragmentId` to avoid misleading the API.

### 11. Row-by-row `instanceof` type dispatch in hot path
**Files:** `LancePageToArrowConverter.java`, `LanceArrowToPageScanner.java`

Both conversion methods resolve the Presto type via an `instanceof` chain on every row. The type is loop-invariant. Hoisting the type resolution outside the loop and using type-specific inner loops eliminates N-1 redundant checks per row and allows tighter JIT code generation.

### 12. Multiple redundant dataset opens in metadata path
**File:** `LanceNamespaceHolder.java`

`getTableMetadata()` calls `tableExists()` (filesystem stat) then `describeTable()` (full dataset open + schema read). `commitAppend()` opens the dataset for version, then `Dataset.commit` likely opens it again internally.

---

## LOW Severity

### 13. `tableExists()` uses redundant double-stat
**File:** `LanceNamespaceHolder.java`

`Files.exists(path) && Files.isDirectory(path)` — two syscalls where `Files.isDirectory(path)` alone suffices (returns `false` for non-existent paths). Same pattern in `listTables()`.

### 14. `Schema.fromJSON()` re-parsed on every `createPageSink()` call
**File:** `LancePageSinkProvider.java`

For queries with many tasks, the same schema JSON string is parsed repeatedly. Consider a `LoadingCache<String, Schema>`.

### 15. `LanceConfig.impl` is stringly-typed dead configuration
**File:** `LanceConfig.java`

The `impl` field accepts `"dir"` as a magic string, but no code reads it to dispatch to alternative implementations. Either remove it or add an enum with validation.

### 16. Hand-rolled `deleteRecursively` with swallowed errors
**File:** `LanceNamespaceHolder.java`

`listFiles()` returns `null` on I/O error (silently skipping children), deletion failures are only logged as warnings. Use `com.google.common.io.MoreFiles.deleteRecursively()` or `Files.walkFileTree` with a `SimpleFileVisitor`.

### 17. `LanceArrowToPageScanner.close()` silently swallows `IOException`
**File:** `LanceArrowToPageScanner.java`

The `// ignore` comment on the catch block provides no rationale. At minimum log a warning, consistent with other close methods in the PR.

### 18. Child allocators named non-uniquely with unbounded ceiling
**Files:** `LanceBasePageSource.java`, `LancePageSink.java`

Multiple concurrent splits create children named after the table name, making Arrow's allocator tree hard to interpret. Both use `Long.MAX_VALUE` ceiling, providing no per-task memory isolation.

---

## Summary

| # | Severity | Location | Issue |
|---|----------|----------|-------|
| 1 | HIGH | `LanceColumnHandle` | Array type hardcoded as `REAL` in single-arg overload |
| 2 | HIGH | `LanceNamespaceHolder` | `schemaName` parameter silently ignored |
| 3 | HIGH | `LanceNamespaceHolder` | Static `RootAllocator` never closed, unbounded |
| 4 | HIGH | `LancePageSink` | Java serialization for commit data; `fragmentsJson` misnomer |
| 5 | HIGH | `LanceSplitManager` + `FragmentScannerFactory` | N+1 dataset opens per query |
| 6 | MEDIUM | Multiple files | Three divergent type-switch implementations |
| 7 | MEDIUM | `LanceNamespaceHolder` | Race condition in `commitAppend` |
| 8 | MEDIUM | `LanceWritableTableHandle` | `equals`/`hashCode` exclude payload fields |
| 9 | MEDIUM | `LancePageSink` | All pages buffered; `writeBatchSize` config is dead |
| 10 | MEDIUM | `LanceSplit` | `List<Integer>` always size 1 |
| 11 | MEDIUM | Converters | Row-by-row `instanceof` in inner loop |
| 12 | MEDIUM | `LanceNamespaceHolder` | Redundant dataset opens in metadata path |
| 13 | LOW | `LanceNamespaceHolder` | Double-stat in `tableExists()` |
| 14 | LOW | `LancePageSinkProvider` | Schema JSON re-parsed per sink |
| 15 | LOW | `LanceConfig` | Dead `impl` configuration |
| 16 | LOW | `LanceNamespaceHolder` | Hand-rolled `deleteRecursively` |
| 17 | LOW | `LanceArrowToPageScanner` | Swallowed `IOException` in `close()` |
| 18 | LOW | Allocators | Non-unique names, unbounded ceiling |
