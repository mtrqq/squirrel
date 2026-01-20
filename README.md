* Allocator evicts existing slots and replace it with new data (run main.go repro, look at #2 item)
* Concurrency model is not figured out AT ALL, concurrency primitives are used somewhat arbitrary, not all the places which have data are protected neither naturally nor by semaphores/mutexes
* Table Controller (rename BinaryEngine? as practically - name sucks)
* Finalize the implementation for the Insert, draft Select without any filters, just get all the data
* Alignments within the allocator, storing unaligned data may slow down CPU significantly
* Table metadata is far from being good UX, user may even select data pages, there's no clear separation between internal table representation and something what user provides as an input.
* Metadata max name size doesn't account for extra amount of data needed for varchar prefix
* Interface for tables and column names is absolutely terrifying, we need to patch it
* Watermark optimization, from Gemini:

Currently, you calculate dataOffset by looking at the header of the highest index. But if you've deallocated that specific slot, your allocator "forgets" where the rest of the data starts.

By adding a lowestOffset to your struct, you create a "high-water mark" for the data section.


Questions:
1. What's the difference between value.(CastedType) vs (CastedType)value? Interface / Pointer cast?
2. 