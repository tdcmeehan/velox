# cuDF Backend vs. Wave Backend: GPU Acceleration in Velox

## Executive Summary

Velox pursues GPU acceleration through **two independent, experimental backends** that occupy the same directory tree (`velox/experimental/`) but embody fundamentally different design philosophies:

| | **cuDF Backend** | **Wave Backend** |
|---|---|---|
| **Location** | `velox/experimental/cudf/` | `velox/experimental/wave/` |
| **Led by** | IBM + NVIDIA collaboration | Meta (originally by Orri Erling / Jimmy Lu) |
| **Approach** | **Library-based** — calls pre-built cuDF APIs | **Code-generation** — emits & compiles CUDA C++ at runtime |
| **Key differentiator** | Maturity, breadth of cuDF API surface | Operator fusion across an entire pipeline |
| **Hardware** | NVIDIA GPUs only (CUDA) | Multi-vendor via Breeze (CUDA, HIP, SYCL, OpenCL, Metal) |
| **CUDA expertise needed** | No — pure C++ against cuDF's C++ API | Yes — generates and compiles `.cu` kernels via NVRTC |

---

## 1. cuDF Backend — Library-Based GPU Acceleration

### 1.1 Integration Architecture

The cuDF backend plugs into Velox through the **`DriverAdapter`** interface (defined in `velox/exec/Driver.h:744-748`). At query compile time, a `CudfDriverAdapter` inspects the pipeline and **replaces CPU operators one-to-one** with GPU equivalents:

```
Velox Plan  →  DriverAdapter::adapt()  →  GPU Pipeline
  TableScan         →  CudfTableScan
  FilterProject     →  CudfFilterProject
  HashAggregation   →  CudfHashAggregation
  HashJoin          →  CudfHashJoin
  ...
```

The replacement is managed by an **`OperatorAdapterRegistry`** singleton (`velox/experimental/cudf/exec/OperatorAdapters.h`). Each registered `OperatorAdapter` answers three questions:
- `canHandle(op)` — is this my operator type?
- `canRunOnGPU(op, planNode, ctx)` — is this specific instance GPU-eligible?
- `createReplacements(...)` — produce the GPU operator(s).

When an operator lacks a GPU implementation, a **`CudfConversion`** operator is inserted to transfer data GPU→CPU, run the CPU operator, then transfer back. This is controlled by the `cudf.allow_cpu_fallback` config flag.

### 1.2 Data Model: CudfVector

```cpp
// velox/experimental/cudf/vector/CudfVector.h
class CudfVector : public RowVector {
  using TableStorage = std::variant<
      std::unique_ptr<cudf::table>,
      std::unique_ptr<cudf::packed_table>>;
  TableStorage tableStorage_;
  cudf::table_view tabView_;
  rmm::cuda_stream_view stream_;
};
```

Key design decisions:
- **Inherits from `RowVector`** — every Velox operator interface works unchanged, consuming/producing `RowVector` pointers. The GPU data is opaque underneath.
- **Arrow-compatible** columnar layout shared with libcudf.
- **Stream-ordered** — the `rmm::cuda_stream_view` ensures asynchronous GPU work is properly sequenced.

### 1.3 GPU Operators Implemented

| Operator | Source |
|---|---|
| TableScan (Hive) | `cudf/connectors/hive/CudfHiveDataSource.h` |
| FilterProject | `cudf/exec/CudfFilterProject.h` |
| HashAggregation | `cudf/exec/CudfHashAggregation.h` |
| HashJoin | `cudf/exec/CudfHashJoin.h` |
| OrderBy | `cudf/exec/CudfOrderBy.h` |
| TopN | `cudf/exec/CudfTopN.h` |
| Limit | `cudf/exec/CudfLimit.h` |
| LocalPartition | `cudf/exec/CudfLocalPartition.h` |
| AssignUniqueId | `cudf/exec/CudfAssignUniqueId.h` |

### 1.4 Expression Evaluation

cuDF offers **three strategies** for evaluating expressions, selected by priority:

| Strategy | Priority (default) | Description |
|---|---|---|
| JIT expressions | 101 (highest) | Jitify-compiled CUDA from Velox expr trees |
| AST expressions | 100 | cuDF's built-in AST evaluator (`cudf/expression/AstExpression.h`) |
| Standalone functions | 50 | Individual cuDF API calls per function |

The `cudf.jit_expression_enabled`, `cudf.ast_expression_enabled`, and their priority knobs control selection.

### 1.5 Memory Management

Uses **RMM** (RAPIDS Memory Manager) with configurable allocator strategy:

```
cudf.memory_resource = {cuda, pool, async, arena, managed, managed_pool, prefetch}
cudf.memory_percent = 50  (% of GPU memory to reserve)
```

### 1.6 Execution Tuning

From the official blog post and VeloxCon 2025 talk:

| Parameter | CPU Velox | cuDF GPU |
|---|---|---|
| Batch size | ~1K rows | ~1 GiB |
| Driver count | = physical CPU cores | 2-8 per GPU |

Each cuDF operator launches **device-wide kernels**. Multiple drivers pipeline GPU work to hide latency of host-to-device copies.

### 1.7 Published Benchmarks

TPC-H at Scale Factor 1,000 (from NVIDIA developer blog, Oct 2025):

| Configuration | Total Runtime |
|---|---|
| Presto C++ on AMD 7965WX (CPU) | 1,246 s |
| Presto + cuDF on NVIDIA RTX PRO 6000 Blackwell | 133.8 s |
| Presto + cuDF on NVIDIA GH200 Grace Hopper | 99.9 s (~**12x** speedup) |

---

## 2. Wave Backend — Code-Generation GPU Acceleration

### 2.1 Integration Architecture

Wave also uses a `DriverAdapter` (via `registerWave()` in `velox/experimental/wave/exec/ToWave.h`), but its `CompileState` does something very different: it **translates entire operator pipelines into CUDA C++ source code**, compiles it with NVRTC at runtime, and launches the resulting kernels.

```
Velox Plan  →  CompileState::compile()  →  CUDA Source  →  NVRTC  →  cubin  →  Launch
  [Scan → Filter → Project → Aggregate]  →  ONE fused kernel
```

The compilation pipeline (`velox/experimental/wave/common/Compile.cu`):

```cpp
nvrtcCreateProgram(&prog, spec.code.c_str(), ...);
nvrtcCompileProgram(prog, ...);        // NVRTC compiles to PTX/cubin
cuModuleLoadDataEx(&module, code, ...); // Load compiled GPU code
cuLaunchKernel(kernel, ...);            // Execute
```

Jitify is used to gather system headers, cached to `/tmp/wavesystemheaders.txt` for faster reuse. A `KernelCache` avoids recompilation of identical kernels.

### 2.2 The Key Concept: Operator Fusion

This is **the defining difference** from cuDF. Wave's `CompileState` partitions the operator pipeline into **`Segment`s** (one per cardinality change), then plans how to **fuse multiple segments into a single kernel**:

```cpp
// velox/experimental/wave/exec/ToWave.h
struct KernelBox {
  std::vector<KernelStep*> steps;  // Multiple ops in ONE kernel
  int32_t numWraps{0};             // Cardinality changes tracked
  std::vector<std::unique_ptr<AbstractInstruction>> instructions;
};

struct PipelineCandidate {
  std::vector<std::vector<KernelBox>> steps;  // The fusion plan
  std::vector<LevelParams> levelParams;       // I/O for each level
};
```

A pipeline like `TableScan -> Filter -> Project -> AggregateProbe -> AggregateUpdate` can become a **single GPU kernel** where thread blocks execute all steps in sequence. Intermediate results stay in **registers and shared memory** rather than being materialized to global GPU memory.

### 2.3 Execution Model: WaveDriver, WaveStream, Pipeline

```
WaveDriver (extends exec::SourceOperator)
  |-- Pipeline[]
       |-- WaveOperator[]     (GPU operators: TableScan, Project, Aggregation, HashJoin, ...)
       |-- running[]          (WaveStream instances in flight)
       |-- arrived[]          (completed, ready to yield results)
       |-- finished[]         (recyclable)
```

**WaveDriver** (`velox/experimental/wave/exec/WaveDriver.h:164`) replaces a consecutive sequence of CPU operators. It manages multiple **`Pipeline`** objects, each containing:
- GPU operators (`WaveOperator` subclasses)
- Multiple **`WaveStream`** instances for concurrent GPU work

**WaveStream** (`velox/experimental/wave/exec/Wave.h`) represents a chain of data-dependent kernel launches with:
- GPU memory arenas (unified + device)
- CUDA streams for async execution
- `LaunchControl` structures for kernel parameters
- Event-based synchronization between streams

**WaveBarrier** synchronizes across multiple `WaveDriver` instances (e.g., all drivers must finish the build phase of a hash join before any can start the probe phase).

### 2.4 Thread-Block-Wide Instruction Set

Wave kernels execute a **thread-block-wide instruction set** where each thread processes one row:

```cuda
// velox/experimental/wave/exec/WaveCore.cuh
struct WaveShared {
  BlockStatus* status;      // Per-lane status (active, error, continue)
  Operand** operands;       // Column data pointers
  void** states;            // Operator states (hash tables, accumulators)
  int32_t blockBase;        // Starting row for this block
  int32_t numRows;          // Total rows
  int32_t numBlocks;        // Grid size
  int16_t programIdx;       // Which fused program this block runs
  int16_t startLabel;       // Resume label (for continuable ops)
  bool isContinue;          // Resuming partial execution
};
```

Multiple `Program`s can share a single kernel launch, each assigned to different thread blocks via `programIdx`. This enables heterogeneous work within one launch.

### 2.5 Supported Operations

From the `StepKind` enum in `ToWave.h:52-65`:

| Step | Description |
|---|---|
| `kTableScan` | Data source reading |
| `kFilter` | Predicate evaluation + wrap (cardinality change) |
| `kOperand` / `kNullCheck` / `kEndNullCheck` | Expression compute with null propagation |
| `kValues` | Constant generation |
| `kAggregateProbe` | Hash table lookup for group-by |
| `kAggregateUpdate` | Accumulator update (fused or separate kernel) |
| `kReadAggregation` | Final aggregation readout |
| `kJoinBuild` | Build side of hash join |
| `kJoinProbe` | Probe side of hash join |
| `kJoinExpand` | 1:N expansion for join matches |

### 2.6 GPU Function & Aggregate Registries

Wave maintains its own **`WaveRegistry`** mapping Velox function names to `inline __device__` GPU code generators, and an **`AggregateRegistry`** mapping aggregate names to code generators for accumulator init, update (with atomics where possible), and extract.

Registered functions (`velox/experimental/wave/exec/RegisterFunctions.cpp`):
- Binary arithmetic: `plus`, `minus`, `multiply`, `divide`, `mod`
- Comparisons: `lt`, `lte`, `eq`, `neq`, `gt`, `gte`
- For all numeric types: `TINYINT` through `DOUBLE`

### 2.7 GPU Memory Management

Wave manages its own memory via **`GpuArena`** (`velox/experimental/wave/common/GpuArena.h`):
- Allocates large **`GpuSlab`** regions (minimum 128 MB)
- Maintains free-list with coalescing
- Supports both **device memory** (fast, GPU-only) and **unified memory** (CPU+GPU accessible)
- Separate from RMM — Wave owns its entire memory stack

### 2.8 GPU I/O: DWIO Decoding

Wave has its own GPU-accelerated I/O layer (`velox/experimental/wave/dwio/`) with:
- **GpuDecoder** (`dwio/decode/GpuDecoder.cu`) — parallel decompression/decoding on GPU
- Support for multiple encodings (originally 8 encodings for Meta's Alpha format)
- **Nimble** format support
- Integrated with the `TableScan` operator for end-to-end GPU data reading

### 2.9 Hardware Portability via Breeze

Wave uses the **Breeze** library (`velox/experimental/breeze/`) — a header-only library for portable data-parallel algorithms supporting:
- **CUDA** (NVIDIA)
- **HIP** (AMD ROCm)
- **SYCL** (Intel)
- **OpenCL**
- **Metal** (Apple)
- **OpenMP** (CPU fallback)

This enables Wave's algorithms to target non-NVIDIA accelerators. Rivos has been working on extending Wave support to their custom accelerator hardware via Breeze.

---

## 3. Head-to-Head Comparison

### 3.1 Architectural Philosophy

| Dimension | cuDF | Wave |
|---|---|---|
| **Abstraction level** | High — calls library APIs | Low — generates CUDA C++ source |
| **Operator boundary** | Each operator = separate cuDF API call(s) | Multiple operators fuse into one kernel |
| **Intermediate data** | Materialized as `cudf::table` between operators | Stays in registers/shared memory within fused kernel |
| **Compilation** | None (pre-compiled library) | NVRTC at runtime (cached) |
| **Startup latency** | Minimal | Higher (kernel compilation, header gathering) |

### 3.2 Operator Fusion — The Central Tradeoff

**cuDF**: A pipeline `Scan -> Filter -> Project -> Aggregate` becomes 4+ separate kernel launches. Each produces a materialized columnar table in GPU memory that the next operator consumes. This means:
- Each kernel launch incurs overhead (~5-15 us)
- Intermediate results consume GPU memory bandwidth (read + write for each stage)
- But each individual kernel is **highly optimized** by RAPIDS engineers

**Wave**: The same pipeline can become **1 kernel** where thread blocks execute all steps in sequence:
```
Thread block:
  1. Read rows from table scan staging
  2. Evaluate filter predicate -> compact passing rows (wrap)
  3. Evaluate projection expressions
  4. Hash group keys -> probe/update aggregation state
  All in registers/shared memory — no global memory round-trip
```

This eliminates inter-operator materialization overhead, which is especially significant for:
- Many chained expressions
- High-selectivity filters (most rows eliminated early)
- Aggregations where the scan->filter->aggregate pipeline is tight

### 3.3 Data Representation

| | cuDF | Wave |
|---|---|---|
| **Vector type** | `CudfVector` (wraps `cudf::table`) | `WaveVector` (wraps `WaveBufferPtr`) |
| **Inherits from** | `RowVector` | Custom, with `Operand` for device access |
| **Memory manager** | RMM (configurable: pool, arena, async, etc.) | `GpuArena` (custom slab allocator) |
| **Host<->Device** | Via `CudfConversion` operator | Via `Transfer` structs + CUDA streams |

### 3.4 Expression Evaluation

| | cuDF | Wave |
|---|---|---|
| **Strategy** | 3 modes: JIT, AST, standalone functions | Code generation into kernel body |
| **Granularity** | Per-expression or per-operator | Fused with surrounding operators |
| **Registration** | cuDF function adapters | `WaveRegistry` with inline `__device__` code generators |
| **Null handling** | cuDF built-in | `NullCheck`/`EndNullCheck` steps in generated code |

### 3.5 Hardware and Ecosystem

| | cuDF | Wave |
|---|---|---|
| **GPU vendors** | NVIDIA only | NVIDIA + AMD + Intel + custom (via Breeze) |
| **Ecosystem** | RAPIDS (RMM, KvikIO, cuDF 26.04) | Standalone (Breeze, custom DWIO) |
| **External users** | Presto (via Prestissimo), Spark (via Gluten) | Meta internal workloads |
| **Multi-node** | UCX-based GPU exchange (PR #15014) | Not yet documented |

### 3.6 Maturity and Production Readiness

| | cuDF | Wave |
|---|---|---|
| **Published benchmarks** | TPC-H SF1000 (12x on GH200) | Early results only (VLDB 2023 workshop) |
| **CI integration** | Yes (adapters-cuda build) | Limited (cuda_driver label for excluding) |
| **Documentation** | README, blog post, NVIDIA blog, VeloxCon talks | Sparse (GitHub discussions, 1 workshop paper) |
| **Test coverage** | Operator tests, function tests, Spark SQL tests | Unit tests for scan, filter/project, aggregation, join |
| **CUDA expertise** | Not required | Required for extending |

---

## 4. When to Use Which

**cuDF is the right choice when:**
- You need GPU acceleration **today** with production-grade confidence
- Your workload has large, independent operators (big joins, large aggregations)
- You're in the NVIDIA RAPIDS ecosystem and want a single GPU data stack
- Your team doesn't have CUDA expertise
- You're running Presto or Spark and want to add GPU support

**Wave is the right choice when (at maturity):**
- Your workload has **tight pipelines** (scan -> filter -> project -> aggregate) where fusion eliminates materialization overhead
- You need to target **non-NVIDIA hardware** (AMD, Intel, Rivos, custom accelerators)
- You need GPU-accelerated **I/O decoding** for Meta's Alpha or Nimble formats
- You're willing to invest in CUDA/GPU kernel development
- You want maximal theoretical throughput at the cost of development complexity

---

## 5. How They Coexist

Both backends are **independent, non-conflicting modules** under `velox/experimental/`:
- They use the same `DriverAdapter` hook in Velox's `Driver` infrastructure
- They are enabled by separate CMake flags: `VELOX_ENABLE_CUDF` and `VELOX_ENABLE_WAVE`
- They cannot both be active for the same pipeline simultaneously — the adapter that runs first wins
- The cuDF backend explicitly says it "does not require CUDA programming knowledge," while Wave is deeply CUDA-native

The Velox project appears to be pursuing both simultaneously, recognizing that they serve different points on the **ease-of-integration vs. theoretical performance ceiling** spectrum. cuDF gets you GPU acceleration quickly with battle-tested RAPIDS libraries; Wave aims for the highest possible throughput through deep fusion and multi-vendor portability, at the cost of more development investment.

---

## References

- [NVIDIA Developer Blog: Accelerating Large-Scale Data Analytics with GPU-Native Velox and cuDF (Oct 2025)](https://developer.nvidia.com/blog/accelerating-large-scale-data-analytics-with-gpu-native-velox-and-nvidia-cudf/)
- [Velox Blog: Extending Velox - GPU Acceleration with cuDF (Jul 2025)](https://velox-lib.io/blog/extending-velox-with-cudf/)
- [CEUR CDMS16: Techniques in Accelerating Query Processing on GPU — Jimmy Lu (VLDB 2023)](https://ceur-ws.org/Vol-3462/CDMS16.pdf)
- [GitHub Discussion #6290: What does wave execution mean?](https://github.com/facebookincubator/velox/discussions/6290)
- [GitHub Discussion #11985: Any more information about Velox Wave?](https://github.com/facebookincubator/velox/discussions/11985)
- [VeloxCon 2025: Accelerating Velox with RAPIDS cuDF](https://www.youtube.com/watch?v=l1JEo-mTNlw)
- [Velox-cuDF README](https://github.com/facebookincubator/velox/blob/main/velox/experimental/cudf/README.md)
