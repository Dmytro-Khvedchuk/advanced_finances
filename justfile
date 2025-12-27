# Set PowerShell as the default shell on Windows for better compatibility.
set windows-powershell := true

# List all available commands. This is the default recipe.
default:
    @just --list

run: 
    python main.py

add package:
    uv add {{package}}

# ==============================================================================
# 📖 Documentation
# ==============================================================================
# Commands for building and serving the project documentation.

# Start a live-reloading local server for documentation.
serve:
    mkdocs serve

# ==============================================================================
# ✅ Git Hooks Management
# ==============================================================================
# Commands for setting up and tearing down Git pre-commit hooks.

# Install pre-commit hooks into the .git/ directory.
install-hooks:
    pre-commit install

# Uninstall pre-commit hooks from the .git/ directory.
uninstall-hooks:
    pre-commit uninstall

# Run all pre-commit hooks against all files.
lint:
    pre-commit run --all-files