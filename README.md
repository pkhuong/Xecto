Xecto: 80% of xectors
=====================

This is a library for regular array parallelism in CL.  It's also an
"80%" library: the goal isn't to extract all the performance there is,
but to allow programmers to easily express computations with a decent
overhead compared to hand-written loops, in common cases.  Uncommon
cases should either be handled specially, or rewritten to better fit
Xecto's simplistic (and simple) approach.

Overview:

 * Structured data
 * Typed arrays
 * Views (later)
 * Reshaping
 * Bulk operations
  - Copy on write
  - Parallelism
   * SIMD
   * Thread-level

Structured data
---------------

_Very lazily sketched out; this looks more like an interface design
issue_

Xecto provides homogeneously, but dynamically-typed bulk structured
data.  Arrays of structures are actually always structures of
specialised, homogeneous arrays.

[more stuff, when it's better thought out]

Think data frames: it's not just a bunch of vectors, there's a level
of semantic typing.

Upgrading rules??

Typed arrays
------------

_Prototyped; just more of the same_

Arrays in Xecto are typed: currently, we only have arrays of double
floats, but we'll also have arrays of single floats, machine integers,
etc.  What's the stance on T arrays? No clue.

Arrays can be of arbitrary rank and size; plans include allocating
them from the foreign heap (or directly via `mmap`).

Reshaping
---------

_Prototyped_

As with many similar libraries, each array is represented as a flat
data vector and shape information; shape information includes the
dimensions, and an affine transformation from the coordinates to an
index in the data vector.

Data vectors are immutable, but reference counted to help with
copy-on-write.  However, arrays themselves are mutable: the copying
(if any) is transparent, and references to data vectors are updated as
needed.

This means that operations like slicing or transposition are nearly
free and do not directly entail copying or allocation.

Bulk operations
---------------

_Half prototyped_

Usual stuff: map, reduce, scan.

Some amount of recycling rule: single element is replicated as needed,
but nothing more.

Reduce and scan work on the first dimension of the single input; the
reduced/scanned function is then applied, map-like on the remaining
dimensions.


## Copy on write

_Not yet, but it's a SMOP_

Mention ! variants (foo-into).  Expresses partial writing, but also
storage reuse.

`mmap` and `tmpfs` for TLB-level copying.

## Parallelism

The library is "just" a minimally smart interpreter; the upside is
that the primitives are chosen so they execute efficiently.

Rather than working with scalars, the primitives compute on strided
spans of vectors (for inputs and outputs), much like level 1 BLAS
operations.  This allows for SIMD-level parallelism

This also means that each operation boils down to a large number of
specialized primitive calls; that's where thread-level parallelism
comes in.

### SIMD

_Hard part done_

Each map operation boils down to a perfect nest of for-loops.  The
nesting is reordered for locality: we attempt to get monotonous
address sequences as much as possible, especially in the result
vector.  Loops are also merged when possible to increase trip counts
and reduce nesting depth.  Finally, we attempt to ensure a trip count
in the innermost loop, to better exploit the primitives.

Primitives are pre-compiled and specialised for some key trip count
and stride values.  That's how we get SIMD.

Execution then proceeds by first finding the primitive corresponding
to the operation and the inner loop's stride and trip count, and
interprets the remainder of the loop nest.

### TLP

_Half-designed_

As shown earlier, each operation is executed as a perfect loop nest
that gives rise to a number of primitive operations.  The key to
exploiting threads is that operations are implemented as futures
(with a thread pool and task stealing).

Each outer loop is executed a couple times to yield a small number of
tasks.  Tasks then note dependencies, which gives us pipelining.

 -> Note: probably want early dealloc.

Futures are triggered via a stack and task stealing, so we get
locality for free.

NUMA awareness via hashing on middle bits of written addresses.

