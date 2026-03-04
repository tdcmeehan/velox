# Code Review: prestodb/presto#27185 — Presto Lance Connector

**PR:** https://github.com/prestodb/presto/pull/27185
**Author:** jja725
**Branch:** `lance-connector`

This PR introduces a new connector enabling Presto to read from and write to LanceDB datasets, a columnar data format built on Apache Arrow optimized for ML/AI workloads. The implementation uses a split-per-fragment read model and fragment-based commit protocol for writes.

---

## CRITICAL — Code Reuse

### C1. `LanceErrorCode` base offset `0x0510_0000` COLLIDES with `ArrowErrorCode`
**File:** `LanceErrorCode.java`

```java
errorCode = new ErrorCode(code + 0x0510_0000, name(), type);
```

`presto-base-arrow-flight`'s `ArrowErrorCode` uses the **exact same base offset** (`0x0510_0000`). `ArrowErrorCode` defines 7 constants (codes 0–6) and `LanceErrorCode` defines 4 constants (codes 0–3), so codes 0–3 collide numerically. Lance must choose a distinct, officially registered range (e.g., `0x0520_0000`), and both connectors should register their ranges in the [Error Codes wiki](https://github.com/prestodb/presto/wiki/Error-Codes).

### C2. Arrow-Presto type mapping duplicates `presto-base-arrow-flight`'s `ArrowBlockBuilder`
**Files:** `LanceColumnHandle.java`, `LanceArrowToPageScanner.java`

`LanceColumnHandle.toPrestoType(ArrowType/Field)` and `LanceArrowToPageScanner`'s vector-to-block conversion duplicate the exact same work already done in [`ArrowBlockBuilder.java`](https://github.com/prestodb/presto/blob/master/presto-base-arrow-flight/src/main/java/com/facebook/plugin/arrow/ArrowBlockBuilder.java) in `presto-base-arrow-flight`:
- `ArrowBlockBuilder.getPrestoTypeFromArrowField(Field)` — Arrow→Presto type mapping with richer coverage (Decimal, Map, Struct, Time, Duration, dictionary encoding)
- `ArrowBlockBuilder.buildBlockFromFieldVector(FieldVector, Type, DictionaryProvider)` — vector→block conversion

Every type handler in the Lance scanner (`BitVector`, `TinyIntVector`, `SmallIntVector`, `IntVector`, `BigIntVector`, `Float4Vector`, `Float8Vector`, `VarCharVector`, `VarBinaryVector`, `DateDayVector`, `TimeStampMicroVector`, `ListVector`, `FixedSizeListVector`) is duplicated.

**Recommendation:** Do NOT depend on `presto-base-arrow-flight` directly — it transitively pulls in `flight-core` (gRPC 1.75.0 + Netty 4.1.130), which Lance has no use for. Instead, extract a new `presto-arrow-toolkit` module (see Appendix A below for the complete extraction plan).

### C3. `LanceFragmentPageSource`/`LanceBasePageSource` duplicate `ArrowPageSource` structure
**Files:** `LanceBasePageSource.java`, `LanceFragmentPageSource.java`

[`ArrowPageSource.java`](https://github.com/prestodb/presto/blob/master/presto-base-arrow-flight/src/main/java/com/facebook/plugin/arrow/ArrowPageSource.java) in `presto-base-arrow-flight` already implements the Arrow-batch-to-Presto-page loop (iterate batches, extract field vectors, call `ArrowBlockBuilder`, assemble Page, track completion). The difference is the data source (Lance Scanner vs. Flight stream), but the column-extraction and block-assembly loop is nearly identical. The ideal factoring is to extract the shared loop into a base class in the new `presto-arrow-toolkit` module behind an `ArrowBatchStream` interface.

### C4. Java `ObjectOutputStream` serialization violates Presto development guidelines
**File:** `LancePageSink.java`

The [Presto Development Guidelines](https://github.com/prestodb/presto/wiki/Presto-Development-Guidelines) explicitly state: **"Feel free to skip the section on Java serialization, as this is not used in Presto."** Every other connector uses Airlift's `JsonCodec<T>` for commit task data serialization. The Hive connector's `PartitionUpdate` pattern (`@JsonCreator`/`@JsonProperty` → `JsonCodec.toBytes()` → `Slices.wrappedBuffer()`) is the canonical example. `LanceCommitTaskData` already uses Jackson annotations — but then stores Base64-encoded Java-serialized `FragmentMetadata` objects inside it, violating the convention.

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
| C1 | CRITICAL | `LanceErrorCode` | Error code base `0x0510_0000` collides with `ArrowErrorCode` |
| C2 | CRITICAL | `LanceColumnHandle`, `LanceArrowToPageScanner` | Arrow↔Presto type mapping duplicates `ArrowBlockBuilder`; extract `presto-arrow-toolkit` (Appendix A) |
| C3 | CRITICAL | `LanceBasePageSource`, `LanceFragmentPageSource` | Page source duplicates `ArrowPageSource` structure; ~610 lines eliminable via toolkit (Appendix A) |
| C4 | CRITICAL | `LancePageSink` | Java `ObjectOutputStream` serialization violates Presto development guidelines |
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

---

## Appendix A: `presto-arrow-toolkit` Extraction Plan

The Lance connector duplicates significant Arrow↔Presto conversion code from `presto-base-arrow-flight` (see C2, C3). However, depending on `presto-base-arrow-flight` directly would drag in `flight-core`, which transitively pulls in **gRPC 1.75.0 + Netty 4.1.130.Final** — a massive dependency footprint that Lance has no use for. Analysis confirms `presto-base-arrow-flight` is a leaf module (no other module in the monorepo depends on it), and the Arrow↔Presto conversion code has **zero `flight-core` imports**.

The solution is to extract a lightweight `presto-arrow-toolkit` module that both `presto-base-arrow-flight` and `presto-lance` depend on.

### Dependency structure

```
presto-arrow-toolkit (NEW — lightweight)
  deps: arrow-vector, arrow-memory-core, presto-spi, presto-common,
        airlift (configuration, bootstrap), guice, jackson, slice

presto-base-arrow-flight (EXISTING — slimmed)
  deps: presto-arrow-toolkit, flight-core (gRPC + Netty)

presto-lance (NEW connector from this PR)
  deps: presto-arrow-toolkit, lance-core
        (NO flight-core, NO gRPC, NO Netty)
```

### New interfaces to create in `presto-arrow-toolkit` (3)

```java
/**
 * Abstracts any source of Arrow record batches (Flight stream, Lance scanner,
 * Parquet reader, etc.) so ArrowPageSource can consume them uniformly.
 */
public interface ArrowBatchStream extends AutoCloseable {
    boolean next();
    VectorSchemaRoot getRoot();
    DictionaryProvider getDictionaryProvider();
    void close();
}

/**
 * Provides schema metadata for the connector's catalog.
 * Decouples ArrowMetadata from BaseArrowFlightClientHandler.
 */
public interface ArrowMetadataProvider {
    List<String> listSchemaNames(ConnectorSession session);
    List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName);
    Schema getSchemaForTable(ConnectorSession session, String schema, String table);
}

/**
 * Factory for creating ArrowBatchStream instances from splits.
 * Decouples ArrowPageSourceProvider from Flight client handler.
 */
public interface ArrowStreamProvider {
    ArrowBatchStream getStream(ConnectorSession session, ArrowSplit split);
}
```

### Files extracted from `presto-base-arrow-flight` → `presto-arrow-toolkit`

#### Move as-is (10 files — zero Flight dependency)

| File | What it does | Dependencies |
|------|-------------|-------------|
| `ArrowBlockBuilder.java` | Arrow FieldVector → Presto Block conversion + type mapping | `arrow-vector`, `presto-common`, `presto-spi`, `slice` |
| `ArrowColumnHandle.java` | SPI `ColumnHandle` data class (columnName + columnType) | `presto-spi`, Jackson |
| `ArrowTableHandle.java` | SPI `ConnectorTableHandle` (schema + table strings) | `presto-spi`, Jackson |
| `ArrowTableLayoutHandle.java` | SPI `ConnectorTableLayoutHandle` with TupleDomain | `presto-spi`, `presto-common` |
| `ArrowTransactionHandle.java` | Singleton enum `ConnectorTransactionHandle` | `presto-spi` |
| `ArrowConnectorId.java` | Value class wrapping connector catalog name | None |
| `ArrowErrorCode.java` | Error code enum (constant names say "FLIGHT" but no code dep) | `presto-common`, `presto-spi` |
| `ArrowException.java` | `PrestoException` subclass using `ArrowErrorCode` | `presto-spi` |
| `ArrowHandleResolver.java` | Returns `.class` for each handle type | `presto-spi` |
| `ArrowPlugin.java` | `Plugin` impl, returns `ArrowConnectorFactory` | `presto-spi`, Guice |

#### Move with refactoring (7 files — replace Flight types with new interfaces)

| File | Change needed |
|------|--------------|
| `ArrowPageSource.java` | Replace `ClientClosingFlightStream` field → `ArrowBatchStream` interface. The `getNextPage()` loop already only calls `next()`, `getRoot()`, `getDictionaryProvider()` — all return `arrow-vector` types. |
| `ArrowPageSourceProvider.java` | Replace `BaseArrowFlightClientHandler clientHandler` → `ArrowStreamProvider streamProvider`. The `createPageSource()` method calls `streamProvider.getStream(session, split)` instead of `clientHandler.getFlightStream(...)`. |
| `ArrowMetadata.java` | Replace `BaseArrowFlightClientHandler clientHandler` → `ArrowMetadataProvider metadataProvider`. Arrow imports are only `org.apache.arrow.vector.types.pojo.{Field, Schema}` (from `arrow-vector`). |
| `ArrowConnector.java` | Already programs to SPI interfaces (`ConnectorMetadata`, `ConnectorSplitManager`, etc.). Moves as-is. |
| `ArrowConnectorFactory.java` | References `ArrowModule` — moves once the module is split. |
| `ArrowModule.java` | Split into base `ArrowModule` (toolkit: binds `BufferAllocator`, `ArrowConnectorId`, `ArrowConnector`, `ArrowHandleResolver`, `ArrowBlockBuilder`, `ArrowMetadata`, `ArrowPageSourceProvider`) and `ArrowFlightModule` (flight: binds `ArrowFlightConfig`, `ArrowSplitManager`, `BaseArrowFlightClientHandler`). |
| `ArrowSplit.java` | Rename `flightEndpointBytes` → `splitPayload` (generic opaque bytes). Flight module interprets bytes as `FlightEndpoint`; Lance module interprets them as fragment IDs. |

#### Extract config base class (1 file)

| File | Change needed |
|------|--------------|
| `ArrowFlightConfig.java` | Extract `ArrowConfig` base class into toolkit with transport-agnostic settings (`case-sensitive-name-matching`). `ArrowFlightConfig extends ArrowConfig` stays in flight module with server/port/SSL properties. |

### Files that stay in `presto-base-arrow-flight` (3 + 2 new)

| File | Why it stays |
|------|-------------|
| `BaseArrowFlightClientHandler.java` | Saturated with Flight imports (`FlightClient`, `FlightInfo`, `FlightDescriptor`, `Location`, TLS config via `grpcTls`). After extraction, implements `ArrowMetadataProvider` + `ArrowStreamProvider`. |
| `ArrowSplitManager.java` | Calls `clientHandler.getFlightInfoForTableScan()` → `FlightInfo`, iterates `flightInfo.getEndpoints()` → `List<FlightEndpoint>`, serializes endpoints. Irreducibly Flight-specific. |
| `ClientClosingFlightStream.java` | Wraps `org.apache.arrow.flight.FlightStream`. After extraction, implements `ArrowBatchStream`. |
| `ArrowFlightConfig.java` (modified) | Extends toolkit's `ArrowConfig`, adds Flight server/port/SSL properties. |
| `ArrowFlightModule.java` (new) | Extends toolkit's `ArrowModule`, binds Flight-specific implementations. |

**Final tally:** 17 classes move to toolkit + 4 new abstractions created; 3 original files + 2 new files remain in flight module.

### What the PR's Lance code should extract into `presto-arrow-toolkit`

In addition to the `presto-base-arrow-flight` extractions above, three pieces of Lance-specific code in this PR contain **generic Arrow↔Presto logic** that should live in the toolkit (and replace the duplicated implementations):

| PR file | Extractable piece | Toolkit destination | What stays in `presto-lance` |
|---------|------------------|--------------------|-----------------------------|
| `LanceColumnHandle.java` | Static methods `toPrestoType(ArrowType)`, `toPrestoType(Field)`, `toArrowType(Type)` — bidirectional Arrow↔Presto type mapping | Merge into `ArrowBlockBuilder`'s existing `getPrestoTypeFromArrowField()` (which already has richer coverage: Decimal, Map, Struct, Time, Duration, dictionary encoding) and add a new `getArrowTypeFromPrestoType(Type)` for the reverse direction | The `ColumnHandle` class itself (Lance-specific SPI boilerplate) |
| `LancePageToArrowConverter.java` | Entire file — static utility methods `toArrowSchema(List<ColumnMetadata>)`, `writeBlockToVector(Block, FieldVector, Type, int)`, `writeBlockToVectorAtOffset(...)`. Handles boolean, tinyint, smallint, integer, bigint, real, double, varchar, varbinary, date, timestamp. **Zero Lance imports.** | New toolkit class `PrestoBlockToArrowWriter` (or add to `ArrowBlockBuilder` as the write direction). This is the cleanest extraction — the file has no Lance or Flight references at all. | Nothing — entire file moves |
| `LanceArrowToPageScanner.java` | Methods `writeVectorToBlock(FieldVector, BlockBuilder, Type)` and `writeValue(...)` — per-row Arrow vector → Presto block writing for all supported types | Merge into `ArrowBlockBuilder`'s existing `buildBlockFromFieldVector()` which already does this. The existing implementation has broader type coverage. | The scanner lifecycle (`read()` method that calls `LanceScanner`), the `ScannerFactory` interface, and the `PageBuilder` orchestration |

### What Lance should use from the toolkit instead of its own code

After extraction, the Lance connector's read path becomes:

```java
// LanceFragmentPageSource — Lance-specific
public class LanceFragmentPageSource extends ArrowPageSource {
    // ArrowPageSource (from toolkit) handles the batch→page loop
    // LanceBatchStream (Lance-specific) implements ArrowBatchStream
}

// LanceBatchStream — Lance-specific, implements toolkit interface
public class LanceBatchStream implements ArrowBatchStream {
    private final LanceScanner scanner;
    private VectorSchemaRoot currentBatch;

    public boolean next() { currentBatch = scanner.next(); return currentBatch != null; }
    public VectorSchemaRoot getRoot() { return currentBatch; }
    public DictionaryProvider getDictionaryProvider() { return /* ... */; }
}
```

And the write path uses toolkit's `PrestoBlockToArrowWriter` directly instead of the duplicate `LancePageToArrowConverter`:

```java
// In LancePageSink.finish()
Schema schema = PrestoBlockToArrowWriter.toArrowSchema(columns);
PrestoBlockToArrowWriter.writeBlockToVector(block, vector, type, rowCount);
// then Lance-specific: Fragment.create(allocator, tablePath, root);
```

### Summary of code elimination in this PR

If `presto-arrow-toolkit` existed, the Lance PR could delete or significantly reduce:

| File | Lines saved | Reason |
|------|------------|--------|
| `LanceArrowToPageScanner.java` | ~200 lines | Vector→Block conversion replaced by `ArrowBlockBuilder` |
| `LancePageToArrowConverter.java` | ~180 lines (entire file) | Moves wholesale to toolkit |
| `LanceColumnHandle.java` | ~100 lines | Type mapping methods replaced by toolkit's `ArrowBlockBuilder` |
| `LanceBasePageSource.java` | ~80 lines | Page source loop replaced by `ArrowPageSource` from toolkit |
| Type-dispatch in `LancePageSink.java` | ~50 lines | `writeBlockToVector` calls delegated to toolkit |

**Total: ~610 lines of duplicated code eliminated**, replaced by dependency on shared, better-tested implementations with broader type coverage (Decimal, Map, Struct, Time, Duration, dictionary encoding — all missing from the Lance implementations).
