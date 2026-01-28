import pytest
import asyncio
import json
import gzip
import tempfile
import os
from pathlib import Path
from jsonl_pipeline import async_process_jsonl, process_jsonl, ProcessorType


class TestProcessJsonl:
    """Test class for process_jsonl function."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_dir_path = Path(self.temp_dir)
    
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_jsonl_file(self, filename: str, data: list) -> Path:
        """Helper method to create a test JSONL file."""
        file_path = self.temp_dir_path / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            for item in data:
                json.dump(item, f, ensure_ascii=False)
                f.write('\n')
        return file_path
    
    def create_test_gzip_jsonl_file(self, filename: str, data: list) -> Path:
        """Helper method to create a gzip compressed test JSONL file."""
        file_path = self.temp_dir_path / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            for item in data:
                json.dump(item, f, ensure_ascii=False)
                f.write('\n')
        return file_path
    
    def create_output_path(self, filename: str) -> Path:
        """Helper method to create an output file path."""
        return self.temp_dir_path / filename
    
    async def simple_processor(self, obj: dict, input_file_abs_path: str, line_num: int) -> dict:
        """Simple processor that adds metadata to each object."""
        result = obj.copy()
        result['processed'] = True
        result['line_number'] = line_num
        return result
    
    async def filter_processor(self, obj: dict, input_file_abs_path: str, line_num: int) -> dict | None:
        """Processor that filters out objects based on a condition."""
        if obj.get('keep', True):
            return obj
        return None
    
    async def error_processor(self, obj: dict, input_file_abs_path: str, line_num: int) -> dict:
        """Processor that raises an error for testing error handling."""
        if obj.get('trigger_error', False):
            raise ValueError("Test error")
        return obj
    
    def test_process_single_file_basic(self):
        """Test basic processing of a single JSONL file."""
        # Create test data
        test_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            {'id': 3, 'name': 'Charlie'}
        ]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor
        ))
        
        # Verify output
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 3
        for i, result in enumerate(results):
            assert result['id'] == i + 1
            assert result['processed'] is True
            assert result['line_number'] == i + 1
    
    def test_process_empty_file(self):
        """Test processing an empty JSONL file."""
        # Create empty test file
        input_file = self.create_test_jsonl_file('empty.jsonl', [])
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor
        ))
        
        # Verify output
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        assert content == ''
    
    def test_process_with_filtering(self):
        """Test processing with a filter processor that returns None for some items."""
        # Create test data with some items to be filtered out
        test_data = [
            {'id': 1, 'keep': True},
            {'id': 2, 'keep': False},
            {'id': 3, 'keep': True}
        ]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.filter_processor
        ))
        
        # Verify output
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 2
        assert results[0]['id'] == 1
        assert results[1]['id'] == 3
    
    def test_process_folder_with_multiple_files(self):
        """Test processing a folder containing multiple JSONL files."""
        # Create multiple test files
        test_data1 = [{'file': 1, 'id': 1}, {'file': 1, 'id': 2}]
        test_data2 = [{'file': 2, 'id': 1}, {'file': 2, 'id': 2}]
        
        file1 = self.create_test_jsonl_file('file1.jsonl', test_data1)
        file2 = self.create_test_jsonl_file('file2.jsonl', test_data2)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor on the folder
        asyncio.run(async_process_jsonl(
            str(self.temp_dir_path), 
            str(output_file), 
            self.simple_processor
        ))
        
        # Verify output
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 4
        # Verify all items were processed
        for result in results:
            assert result['processed'] is True
            assert 'line_number' in result
    
    def test_error_handling_skip(self):
        """Test error handling with 'skip' strategy."""
        # Create test data with one item that will trigger an error
        test_data = [
            {'id': 1, 'trigger_error': False},
            {'id': 2, 'trigger_error': True},
            {'id': 3, 'trigger_error': False}
        ]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor with skip error handling
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.error_processor,
            on_error="skip"
        ))
        
        # Verify output - should have 2 items (error item skipped)
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 2
        assert results[0]['id'] == 1
        assert results[1]['id'] == 3
    
    def test_error_handling_empty_object(self):
        """Test error handling with 'empty-object' strategy."""
        # Create test data with one item that will trigger an error
        test_data = [
            {'id': 1, 'trigger_error': False},
            {'id': 2, 'trigger_error': True},
            {'id': 3, 'trigger_error': False}
        ]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor with empty-object error handling
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.error_processor,
            on_error="empty-object"
        ))
        
        # Verify output - should have 3 items (error item becomes empty object)
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 3
        assert results[0]['id'] == 1
        assert results[1] == {}  # Empty object for error case
        assert results[2]['id'] == 3
    
    def test_error_handling_fail(self):
        """Test error handling with 'fail' strategy."""
        # Create test data with one item that will trigger an error
        test_data = [
            {'id': 1, 'trigger_error': False},
            {'id': 2, 'trigger_error': True},
            {'id': 3, 'trigger_error': False}
        ]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor with fail error handling - should raise exception
        with pytest.raises(ValueError, match="Test error"):
            asyncio.run(async_process_jsonl(
                str(input_file), 
                str(output_file), 
                self.error_processor,
                on_error="fail"
            ))
    
    def test_invalid_json_handling_skip(self):
        """Test handling of invalid JSON lines with 'skip' strategy."""
        # Create file with invalid JSON
        input_file = self.temp_dir_path / 'invalid.jsonl'
        with open(input_file, 'w', encoding='utf-8') as f:
            f.write('{"id": 1, "name": "Alice"}\n')  # Valid JSON
            f.write('{"id": 2, "name": "Bob",}\n')   # Invalid JSON (trailing comma)
            f.write('{"id": 3, "name": "Charlie"}\n') # Valid JSON
        
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor with skip error handling
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor,
            on_error="skip"
        ))
        
        # Verify output - should have 2 items (invalid JSON line skipped)
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 2
        assert results[0]['id'] == 1
        assert results[1]['id'] == 3
    
    def test_custom_concurrency(self):
        """Test processing with custom concurrency settings."""
        # Create test data
        test_data = [{'id': i} for i in range(5)]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor with custom concurrency
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor,
            n_jobs=2
        ))
        
        # Verify output
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 5
        for i, result in enumerate(results):
            assert result['id'] == i
            assert result['processed'] is True
    
    def test_nonexistent_input_file(self):
        """Test behavior when input file doesn't exist."""
        nonexistent_file = self.temp_dir_path / 'nonexistent.jsonl'
        output_file = self.create_output_path('output.jsonl')
        
        # Should raise ValueError for nonexistent input
        with pytest.raises(ValueError, match="Input path does not exist"):
            asyncio.run(async_process_jsonl(
                str(nonexistent_file), 
                str(output_file), 
                self.simple_processor
            ))
    
    def test_invalid_on_error_parameter(self):
        """Test that invalid on_error parameter raises ValueError."""
        test_data = [{'id': 1}]
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Should raise ValueError for invalid on_error parameter
        with pytest.raises(ValueError, match="Invalid on_error option"):
            asyncio.run(async_process_jsonl(
                str(input_file), 
                str(output_file), 
                self.simple_processor,
                on_error="invalid_option"
            ))
    
    def test_process_jsonl_sync_wrapper(self):
        """Test the synchronous process_jsonl wrapper function."""
        # Create test data
        test_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the synchronous processor
        process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor
        )
        
        # Verify output
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 2
        for i, result in enumerate(results):
            assert result['id'] == i + 1
            assert result['processed'] is True
            assert result['line_number'] == i + 1
    
    def test_combine_output_false_preserves_folder_structure(self):
        """Test that combine_output=False creates separate output files preserving folder structure."""
        # Create nested directory structure with test files
        subdir1 = self.temp_dir_path / 'input' / 'subdir1'
        subdir2 = self.temp_dir_path / 'input' / 'subdir2'
        subdir1.mkdir(parents=True)
        subdir2.mkdir(parents=True)
        
        test_data1 = [{'file': 'file1', 'id': 1}, {'file': 'file1', 'id': 2}]
        test_data2 = [{'file': 'file2', 'id': 1}]
        test_data3 = [{'file': 'file3', 'id': 1}, {'file': 'file3', 'id': 2}, {'file': 'file3', 'id': 3}]
        
        self.create_test_jsonl_file('input/subdir1/file1.jsonl', test_data1)
        self.create_test_jsonl_file('input/subdir1/file2.jsonl', test_data2)
        self.create_test_jsonl_file('input/subdir2/file3.jsonl', test_data3)
        
        input_folder = self.temp_dir_path / 'input'
        output_folder = self.temp_dir_path / 'output'
        
        # Run the processor with combine_output=False
        asyncio.run(async_process_jsonl(
            str(input_folder), 
            str(output_folder), 
            self.simple_processor,
            combine_output=False
        ))
        
        # Verify output files exist with correct structure
        output_file1 = output_folder / 'subdir1' / 'file1.jsonl'
        output_file2 = output_folder / 'subdir1' / 'file2.jsonl'
        output_file3 = output_folder / 'subdir2' / 'file3.jsonl'
        
        assert output_file1.exists()
        assert output_file2.exists()
        assert output_file3.exists()
        
        # Verify content of each output file
        with open(output_file1, 'r', encoding='utf-8') as f:
            results1 = [json.loads(line) for line in f]
        assert len(results1) == 2
        assert all(r['file'] == 'file1' for r in results1)
        
        with open(output_file2, 'r', encoding='utf-8') as f:
            results2 = [json.loads(line) for line in f]
        assert len(results2) == 1
        assert results2[0]['file'] == 'file2'
        
        with open(output_file3, 'r', encoding='utf-8') as f:
            results3 = [json.loads(line) for line in f]
        assert len(results3) == 3
        assert all(r['file'] == 'file3' for r in results3)
    
    def test_read_gzip_compressed_jsonl(self):
        """Test reading gzip compressed .jsonl.gz files."""
        # Create compressed test data
        test_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            {'id': 3, 'name': 'Charlie'}
        ]
        
        # Create compressed test file
        input_file = self.create_test_gzip_jsonl_file('input.jsonl.gz', test_data)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor,
            extension_names=['.jsonl.gz']
        ))
        
        # Verify output
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 3
        for i, result in enumerate(results):
            assert result['id'] == i + 1
            assert result['processed'] is True
    
    def test_write_gzip_compressed_jsonl(self):
        """Test writing gzip compressed .jsonl.gz output files."""
        # Create test data
        test_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        # Create test file
        input_file = self.create_test_jsonl_file('input.jsonl', test_data)
        output_file = self.create_output_path('output.jsonl.gz')
        
        # Run the processor with compressed output
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor
        ))
        
        # Verify output is a valid gzip file
        assert output_file.exists()
        
        with gzip.open(output_file, 'rt', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 2
        for i, result in enumerate(results):
            assert result['id'] == i + 1
            assert result['processed'] is True
    
    def test_read_and_write_gzip_compressed_jsonl(self):
        """Test reading gzip input and writing gzip output."""
        # Create compressed test data
        test_data = [
            {'id': 1, 'value': 'test1'},
            {'id': 2, 'value': 'test2'}
        ]
        
        # Create compressed test file
        input_file = self.create_test_gzip_jsonl_file('input.jsonl.gz', test_data)
        output_file = self.create_output_path('output.jsonl.gz')
        
        # Run the processor with both compressed input and output
        asyncio.run(async_process_jsonl(
            str(input_file), 
            str(output_file), 
            self.simple_processor,
            extension_names=['.jsonl.gz']
        ))
        
        # Verify output is a valid gzip file
        assert output_file.exists()
        
        with gzip.open(output_file, 'rt', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 2
        for i, result in enumerate(results):
            assert result['id'] == i + 1
            assert result['processed'] is True
    
    def test_folder_with_mixed_plain_and_gzip_files(self):
        """Test processing a folder with both plain and gzip compressed files."""
        # Create mixed test files
        test_data_plain = [{'type': 'plain', 'id': 1}]
        test_data_gzip = [{'type': 'gzip', 'id': 2}]
        
        self.create_test_jsonl_file('plain.jsonl', test_data_plain)
        self.create_test_gzip_jsonl_file('compressed.jsonl.gz', test_data_gzip)
        output_file = self.create_output_path('output.jsonl')
        
        # Run the processor on the folder with both extensions
        asyncio.run(async_process_jsonl(
            str(self.temp_dir_path), 
            str(output_file), 
            self.simple_processor,
            extension_names=['.jsonl', '.jsonl.gz']
        ))
        
        # Verify output contains data from both files
        assert output_file.exists()
        
        with open(output_file, 'r', encoding='utf-8') as f:
            results = [json.loads(line) for line in f]
        
        assert len(results) == 2
        types = {r['type'] for r in results}
        assert types == {'plain', 'gzip'}
    
    def test_combine_output_false_with_gzip_files(self):
        """Test combine_output=False with gzip compressed files preserves structure."""
        # Create nested directory structure with compressed test files
        subdir = self.temp_dir_path / 'input' / 'subdir'
        subdir.mkdir(parents=True)
        
        test_data1 = [{'file': 'file1', 'id': 1}]
        test_data2 = [{'file': 'file2', 'id': 2}]
        
        self.create_test_gzip_jsonl_file('input/file1.jsonl.gz', test_data1)
        self.create_test_gzip_jsonl_file('input/subdir/file2.jsonl.gz', test_data2)
        
        input_folder = self.temp_dir_path / 'input'
        output_folder = self.temp_dir_path / 'output'
        
        # Run the processor with combine_output=False
        asyncio.run(async_process_jsonl(
            str(input_folder), 
            str(output_folder), 
            self.simple_processor,
            extension_names=['.jsonl.gz'],
            combine_output=False
        ))
        
        # Verify output files exist with correct structure (compressed)
        output_file1 = output_folder / 'file1.jsonl.gz'
        output_file2 = output_folder / 'subdir' / 'file2.jsonl.gz'
        
        assert output_file1.exists()
        assert output_file2.exists()
        
        # Verify content is correctly compressed
        with gzip.open(output_file1, 'rt', encoding='utf-8') as f:
            results1 = [json.loads(line) for line in f]
        assert len(results1) == 1
        assert results1[0]['file'] == 'file1'
        
        with gzip.open(output_file2, 'rt', encoding='utf-8') as f:
            results2 = [json.loads(line) for line in f]
        assert len(results2) == 1
        assert results2[0]['file'] == 'file2'