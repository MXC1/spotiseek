# Code Quality and Linting

This project uses multiple linting and static analysis tools to maintain code quality, detect code smells, and ensure best practices.

## Available Tools

### Ruff (Primary Linter)
A fast, modern Python linter that combines the functionality of multiple tools (flake8, isort, pyupgrade, etc.).

- **Configuration**: `pyproject.toml` under `[tool.ruff]`
- **What it checks**: Import sorting, code style, common bugs, complexity, deprecated patterns
- **Auto-fix capability**: Yes (fixes 80%+ of issues automatically)

### Pylint (Advanced Static Analysis)
More comprehensive static analysis with focus on code quality and maintainability.

- **Configuration**: `pyproject.toml` under `[tool.pylint]`
- **What it checks**: Code quality, design patterns, potential bugs, naming conventions
- **Complexity threshold**: Max 15 (configurable)

### Flake8 (Code Style)
Traditional Python style guide enforcement.

- **Configuration**: `.flake8`
- **What it checks**: PEP 8 compliance, logical errors, complexity
- **Max complexity**: 15

### Radon (Complexity Analysis)
Code metrics tool for analyzing cyclomatic complexity and maintainability.

- **What it provides**: Cyclomatic complexity scores, maintainability index
- **Thresholds**: Functions with complexity > 15 should be refactored

## Usage

### Run all linters
```bash
invoke lint
```

### Run specific linter
```bash
invoke lint --tool=ruff
invoke lint --tool=flake8
invoke lint --tool=pylint
```

### Auto-fix issues with Ruff
```bash
invoke lint-fix
```

### Check code complexity
```bash
invoke complexity
```

## Complexity Ratings

### Cyclomatic Complexity (per function)
- **A (1-5)**: Simple, easy to understand
- **B (6-10)**: Moderate, acceptable
- **C (11-20)**: Complex, consider refactoring
- **D (21-30)**: Very complex, should be refactored
- **F (>30)**: Extremely complex, requires immediate refactoring

### Maintainability Index (per file)
- **A (85-100)**: Highly maintainable
- **B (65-84)**: Moderately maintainable
- **C (20-64)**: Low maintainability
- **D (<20)**: Very low maintainability

## Current Status

### Summary
- **Initial state**: 999 linting issues detected
- **After auto-fix**: 125 remaining issues
- **Auto-fixed**: 874 issues (87.5%)
- **Maintainability**: All files rated "A"

### Remaining Issues
Most remaining issues are recommendations rather than errors:
- Suggestions to use `pathlib` instead of `os.path` (modernization)
- Import organization preferences
- Minor complexity warnings (functions rated C)

### Functions with High Complexity (C rating)
The following functions have complexity scores of 11-20 and could benefit from refactoring in the future:
- `extract_file_metadata` (scripts/xml_exporter.py) - Rated F, needs refactoring
- `_add_track_to_xml` (scripts/xml_exporter.py)
- `analyze_workflow_run` (scripts/logs_utils.py)
- `get_tracks_from_playlist` (scripts/spotify_scraper.py)
- `_handle_completed_download` (scripts/workflow.py)
- `process_playlist` (scripts/workflow.py)
- `select_best_file` (scripts/soulseek_client.py)
- `is_better_quality` (scripts/soulseek_client.py)
- `enqueue_download` (scripts/soulseek_client.py)
- `import_track` (observability/combined_dashboard.py)

## CI/CD Integration

To integrate linting into your CI/CD pipeline, add:

```yaml
- name: Run linters
  run: |
    pip install -r requirements.txt
    ruff check scripts/ tasks.py observability/
    pylint scripts/ tasks.py observability/ --exit-zero
    radon cc scripts/ tasks.py observability/ -a -nb
```

## Best Practices

1. **Run linters before committing**: Use `invoke lint-fix` to auto-fix issues
2. **Check complexity regularly**: Use `invoke complexity` to identify overly complex functions
3. **Address F-rated functions**: Functions with complexity > 30 should be split into smaller functions
4. **Keep maintainability high**: Aim for A or B ratings on all files
5. **Follow the boy scout rule**: Leave code cleaner than you found it

## Configuration Files

- `pyproject.toml` - Ruff and Pylint configuration
- `.flake8` - Flake8 configuration
- `tasks.py` - Invoke tasks for running linters

## Ignoring False Positives

To ignore specific warnings in code:

```python
# For Ruff
# ruff: noqa: E501
long_line = "This is a very long line that exceeds the character limit..."

# For Pylint
# pylint: disable=too-many-arguments
def complex_function(arg1, arg2, arg3, arg4, arg5, arg6):
    pass
```

## Future Improvements

Consider adding:
- **mypy**: Static type checking
- **bandit**: Security vulnerability detection
- **pre-commit hooks**: Automatic linting before commits
- **coverage**: Code coverage analysis
