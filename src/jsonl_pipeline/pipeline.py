from typing import Callable, Awaitable, Any, List, Literal
import json
import asyncio
import os
import gzip
from pathlib import Path
from tqdm.asyncio import tqdm


# Define the processor type as an async function
# Args:
#   obj (dict): The JSON object from the current line being processed
#   input_file_abs_path (str): Absolute path to the input file being processed
#   line_num (int): The line number (1-indexed) of the current object in the file
# Returns:
#   Awaitable[dict | None]: A coroutine that resolves to the processed JSON object or None if filtered out
ProcessorType = Callable[[dict, str, int], Awaitable[dict | None]]


def _get_processor_name(processor: ProcessorType) -> str:
    """
    Get the name of the processor function using reflection.
    
    Args:
        processor: The processor function
        
    Returns:
        The name of the processor function
    """
    if hasattr(processor, '__name__'):
        return processor.__name__
    elif hasattr(processor, '__class__'):
        return processor.__class__.__name__
    else:
        return "Unknown Processor"


async def async_process_jsonl(input_folder_or_file: str, output_file_or_folder: str, processor: ProcessorType,
                        n_jobs: int = 10, extension_names: list[str] | None = None, decoding: str = "utf-8",
                        on_error: Literal["skip", "empty-object", "fail"] = "skip",
                        combine_output: bool = True):
    """
    Process JSONL files using the provided async processor.
    
    Args:
        input_folder_or_file: Path to a single JSONL file or folder containing JSONL files
        output_file_or_folder: Path to the output file (when combine_output=True) or output folder 
                               (when combine_output=False) where processed results will be saved
        processor: Async function to process each JSON object
        n_jobs: Maximum number of concurrent tasks (default: 10)
        extension_names: List of file extensions to process when input is a folder.
                         If None (default), searches for both .jsonl and .jsonl.gz files.
        decoding: File decoding format (default: "utf-8")
        on_error: Error handling strategy - "skip", "empty-object", or "fail" (default: "skip")
                 - "skip": Skip the problematic line and continue processing
                 - "empty-object": Write an empty dict {} to output for problematic lines
                 - "fail": Stop processing and raise an exception on any error
        combine_output: If True (default), combine all results into a single output file.
                       If False, create separate output files preserving the input folder structure.
    """
    input_path = Path(input_folder_or_file).resolve()
    processor_name = _get_processor_name(processor)
    
    # Default extension_names to both plain and compressed
    if extension_names is None:
        extension_names = [".jsonl", ".jsonl.gz"]
    
    # Validate on_error parameter
    valid_on_error_options = ["skip", "empty-object", "fail"]
    if on_error not in valid_on_error_options:
        raise ValueError(f"Invalid on_error option '{on_error}'. Must be one of: {valid_on_error_options}")
    
    if input_path.is_file():
        # Process single file
        results = await _process_file(input_path, processor, n_jobs, processor_name, decoding, on_error)
        # Save results to output file
        await _save_results(results, output_file_or_folder)
    elif input_path.is_dir():
        # Process all JSONL files in directory recursively
        jsonl_files = _find_jsonl_files(input_path, extension_names)
        
        if combine_output:
            # Combine all results into a single output file
            all_results = []
            
            # Add progress bar for file processing
            with tqdm(jsonl_files, desc=f"Processing files with {processor_name}", unit="file") as file_pbar:
                for file_path in file_pbar:
                    file_pbar.set_postfix(file=file_path.name)
                    file_results = await _process_file(file_path, processor, n_jobs, processor_name, decoding, on_error)
                    all_results.extend(file_results)
            
            # Save combined results to output file
            await _save_results(all_results, output_file_or_folder)
        else:
            # Create separate output files preserving folder structure
            output_base = Path(output_file_or_folder).resolve()
            
            with tqdm(jsonl_files, desc=f"Processing files with {processor_name}", unit="file") as file_pbar:
                for file_path in file_pbar:
                    file_pbar.set_postfix(file=file_path.name)
                    file_results = await _process_file(file_path, processor, n_jobs, processor_name, decoding, on_error)
                    
                    # Calculate relative path and create corresponding output path
                    relative_path = file_path.relative_to(input_path)
                    output_file_path = output_base / relative_path
                    
                    # Save results for this file
                    await _save_results(file_results, str(output_file_path))
    else:
        raise ValueError(f"Input path does not exist: {input_folder_or_file}")


def process_jsonl(input_folder_or_file: str, output_file_or_folder: str, processor: ProcessorType,
                        n_jobs: int = 10, extension_names: list[str] | None = None, decoding: str = "utf-8",
                        on_error: Literal["skip", "empty-object", "fail"] = "skip",
                        combine_output: bool = True):
    asyncio.run(async_process_jsonl(input_folder_or_file, output_file_or_folder, processor, n_jobs, extension_names, decoding, on_error, combine_output))


def _is_gzip_file(file_path: Path) -> bool:
    """
    Check if a file is a gzip compressed file based on extension.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if the file has a .gz extension
    """
    return file_path.name.lower().endswith('.gz')


def _open_file_for_reading(file_path: Path, decoding: str = "utf-8"):
    """
    Open a file for reading, handling both plain and gzip compressed files.
    Uses streaming for gzip files to avoid loading entire file into memory.
    
    Args:
        file_path: Path to the file
        decoding: File decoding format
        
    Returns:
        A file-like object for reading lines
    """
    if _is_gzip_file(file_path):
        # Use gzip.open with 'rt' mode for text reading with streaming
        return gzip.open(file_path, 'rt', encoding=decoding)
    else:
        return open(file_path, 'r', encoding=decoding)


def _open_file_for_writing(file_path: Path, decoding: str = "utf-8"):
    """
    Open a file for writing, handling both plain and gzip compressed files.
    Uses streaming for gzip files to avoid loading entire file into memory.
    
    Args:
        file_path: Path to the file
        decoding: File decoding format
        
    Returns:
        A file-like object for writing
    """
    if _is_gzip_file(file_path):
        # Use gzip.open with 'wt' mode for text writing with streaming
        return gzip.open(file_path, 'wt', encoding=decoding)
    else:
        return open(file_path, 'w', encoding=decoding)


async def _process_file(file_path: Path, processor: ProcessorType, n_jobs: int, processor_name: str, decoding: str = "utf-8", on_error: Literal["skip", "empty-object", "fail"] = "skip") -> List[dict]:
    """
    Process a single JSONL file using the async processor.
    
    Args:
        file_path: Path to the JSONL file
        processor: Async function to process each JSON object
        n_jobs: Maximum number of concurrent tasks
        processor_name: Name of the processor function for progress display
        decoding: File decoding format (default: "utf-8")
        on_error: Error handling strategy - "skip", "empty-object", or "fail"
        
    Returns:
        List of processed JSON objects
    """
    # Read the file first, with limited exception handling
    # Uses streaming for gzip files
    try:
        with _open_file_for_reading(file_path, decoding) as file:
            # Read all lines for progress bar and processing
            lines = file.readlines()
    except (IOError, OSError) as e:
        print(f"Error reading file {file_path}: {e}")
        return []
    
    # Now process the lines - any IOError/OSError here should propagate from user code
    results = []
    
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(n_jobs)
    tasks = []
    
    async def process_with_semaphore(json_obj, file_path_str, line_num, pbar):
        async with semaphore:
            try:
                result = await processor(json_obj, file_path_str, line_num)
                pbar.update(1)
                return result
            except Exception as e:
                pbar.update(1)
                if on_error == "fail":
                    raise e
                elif on_error == "empty-object":
                    print(f"Warning: Error processing line {line_num} in {file_path}: {e}")
                    return {}
                else:  # on_error == "skip"
                    print(f"Warning: Error processing line {line_num} in {file_path}: {e}")
                    return None
    
    # Create progress bar for line processing
    with tqdm(total=len(lines), desc=f"{processor_name} - {file_path.name}", unit="lines") as pbar:
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                pbar.update(1)
                continue
            
            try:
                json_obj = json.loads(line)
                task = process_with_semaphore(json_obj, str(file_path.resolve()), line_num, pbar)
                tasks.append(task)
            except json.JSONDecodeError as e:
                if on_error == "fail":
                    raise e
                elif on_error == "empty-object":
                    print(f"Warning: Invalid JSON on line {line_num} in {file_path}: {e}")
                    # Create a task that returns an empty object
                    async def make_empty_task(pbar=pbar):
                        pbar.update(1)
                        return {}
                    tasks.append(make_empty_task())
                else:  # on_error == "skip"
                    print(f"Warning: Invalid JSON on line {line_num} in {file_path}: {e}")
                    pbar.update(1)
                    continue
        
        # Process all objects concurrently
        if tasks:
            if on_error == "fail":
                # Don't use return_exceptions=True for fail mode to let exceptions propagate
                results = await asyncio.gather(*tasks)
                # Filter out None results
                results = [result for result in results if result is not None]
            else:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle results based on on_error strategy
                valid_results = []
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        if on_error == "empty-object":
                            valid_results.append({})
                        # For "skip", we don't add anything to valid_results
                    elif result is not None:
                        valid_results.append(result)
                    # Note: None results are filtered out when on_error is "skip"
                
                results = valid_results
    
    return results


def _find_jsonl_files(directory: Path, extension_names: list[str] = [".jsonl"]) -> List[Path]:
    """
    Recursively find all files with specified extensions in the given directory.
    
    Args:
        directory: Directory to search
        extension_names: List of file extensions to search for (default: [".jsonl"])
        
    Returns:
        List of paths to files with the specified extensions
    """
    matching_files = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_lower = file.lower()
            for ext in extension_names:
                if file_lower.endswith(ext.lower()):
                    matching_files.append(Path(root) / file)
                    break  # Found a match, no need to check other extensions
    
    return sorted(matching_files)  # Sort for consistent ordering


async def _save_results(results: List[dict], output_file: str):
    """
    Save processed results to a JSONL output file.
    Supports both plain and gzip compressed output based on file extension.
    Uses streaming for gzip files to avoid loading entire file into memory.
    
    Args:
        results: List of processed JSON objects
        output_file: Path to the output file (use .jsonl.gz extension for compressed output)
    """
    output_path = Path(output_file)
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with _open_file_for_writing(output_path) as file:
            for result in results:
                json.dump(result, file, ensure_ascii=False)
                file.write('\n')
        
        print(f"Successfully saved {len(results)} processed objects to {output_file}")
        
    except Exception as e:
        print(f"Error saving results to {output_file}: {e}")
        raise