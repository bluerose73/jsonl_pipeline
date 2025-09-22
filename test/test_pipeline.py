import pytest
import asyncio
import json
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
        with open(file_path, 'w', encoding='utf-8') as f:
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