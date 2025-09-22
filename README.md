# jsonl_pipeline

A Python library that handles JSONL data processing boilerplates for you.

## Installation

```bash
pip install jsonl_pipeline
```

## Usage

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

## Features

- Process single JSONL files or entire directories
- Async processing with configurable concurrency
- Automatic error handling and logging
- Preserves file paths and line numbers for debugging
