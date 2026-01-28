# jsonl_pipeline

A Python library that handles JSONL data processing boilerplates for you.

## Installation

```bash
pip install git+https://github.com/bluerose73/jsonl_pipeline.git
```

## Quick Start

```python
from jsonl_pipeline import process_jsonl

# Define your async processor function
async def my_processor(obj, input_file_path, line_num):
    # Process your JSON object here
    obj['processed'] = True
    obj['line_number'] = line_num
    return obj

# Process JSONL files
process_jsonl(
    input_folder_or_file="input.jsonl",  # or "input_folder/"
    output_file="output.jsonl",
    processor=my_processor,
    n_jobs=10  # concurrent processing limit
)
```

Async mode

```python
import asyncio
from jsonl_pipeline import async_process_jsonl

# Define your async processor function
async def my_processor(obj, input_file_path, line_num):
    # Process your JSON object here
    obj['processed'] = True
    obj['line_number'] = line_num
    return obj

# Process JSONL files
async def main():
    await async_process_jsonl(
        input_folder_or_file="input.jsonl",  # or "input_folder/"
        output_file="output.jsonl",
        processor=my_processor,
        n_jobs=10  # concurrent processing limit
    )

asyncio.run(main())
```

## Usage

### Processor Interface

The processor is an async function with the following signature:

```python
async def processor(obj: dict, input_file_abs_path: str, line_num: int) -> dict | None
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `obj` | `dict` | The JSON object from the current line being processed |
| `input_file_abs_path` | `str` | Absolute path to the input file being processed |
| `line_num` | `int` | The line number (1-indexed) of the current object in the file |
| **Returns** | `dict \| None` | The processed JSON object, or `None` to filter out the line |

### Arguments

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_folder_or_file` | `str` | required | Path to a single JSONL file or folder containing JSONL files |
| `output_file_or_folder` | `str` | required | Output file path (when `combine_output=True`) or output folder (when `combine_output=False`) |
| `processor` | `ProcessorType` | required | Async function to process each JSON object |
| `n_jobs` | `int` | `10` | Maximum number of concurrent tasks |
| `extension_names` | `list[str] \| None` | `None` | File extensions to process when input is a folder. If `None`, searches for both `.jsonl` and `.jsonl.gz` files |
| `decoding` | `str` | `"utf-8"` | File encoding format |
| `on_error` | `Literal["skip", "empty-object", "fail"]` | `"skip"` | Error handling strategy (see below) |
| `combine_output` | `bool` | `True` | If `True`, combine all results into a single output file. If `False`, create separate output files preserving folder structure |

### Error Handling

The `on_error` parameter controls how errors are handled:

- `"skip"` - Skip the problematic line and continue processing
- `"empty-object"` - Write an empty `{}` to output for problematic lines
- `"fail"` - Stop processing and raise an exception on any error

### Compressed Files (gzip)

The library automatically handles gzip-compressed JSONL files based on file extension:

```python
# Read compressed input (single file)
process_jsonl("input.jsonl.gz", "output.jsonl", processor)

# Write compressed output
process_jsonl("input.jsonl", "output.jsonl.gz", processor)

# Process folder - both .jsonl and .jsonl.gz files are included by default
process_jsonl("input_folder/", "output.jsonl", processor)

# Process only compressed files in folder
process_jsonl("input_folder/", "output.jsonl", processor, extension_names=[".jsonl.gz"])
```

Compression uses streaming to avoid loading entire files into memory.

### Preserving Folder Structure

When processing a folder, set `combine_output=False` to create separate output files that mirror the input structure:

```python
# Input:
#   input_folder/
#     file1.jsonl
#     subdir/file2.jsonl
#
# Output:
#   output_folder/
#     file1.jsonl
#     subdir/file2.jsonl

process_jsonl(
    "input_folder/",
    "output_folder/",
    processor,
    combine_output=False
)
```

## Features

- Process single JSONL files or entire directories
- Async processing with configurable concurrency
- Automatic error handling and logging
- Preserves file paths and line numbers for debugging
