"""
Utility Functions Module

This module provides utility functions used across the fraud analytics pipeline.
"""

import os
import time
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown

console = Console()

def log_step(step_name, description=None):
    """
    Log a pipeline step with a formatted title and description.
    
    Args:
        step_name (str): Name of the step
        description (str, optional): Description of the step
    """
    console.print("\n")
    if description:
        console.print(Panel(
            f"[bold cyan]{step_name}[/bold cyan]\n\n[white]{description}[/white]",
            expand=False
        ))
    else:
        console.print(Panel(f"[bold cyan]{step_name}[/bold cyan]", expand=False))
    console.print("\n")

def log_success(message):
    """
    Log a success message.
    
    Args:
        message (str): Success message to log
    """
    console.print(f"[bold green]✓ {message}[/bold green]")

def log_error(message, exception=None):
    """
    Log an error message with optional exception details.
    
    Args:
        message (str): Error message to log
        exception (Exception, optional): Exception to log
    """
    error_text = f"[bold red]✗ {message}[/bold red]"
    if exception:
        error_text += f"\n[red]{type(exception).__name__}: {str(exception)}[/red]"
    console.print(error_text)

def log_warning(message):
    """
    Log a warning message.
    
    Args:
        message (str): Warning message to log
    """
    console.print(f"[bold yellow]⚠ {message}[/bold yellow]")

def time_execution(func):
    """
    Decorator to time the execution of a function.
    
    Args:
        func (callable): Function to time
    
    Returns:
        callable: Wrapped function that logs execution time
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        console.print(f"[cyan]Starting {func.__name__}...[/cyan]")
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        console.print(f"[cyan]{func.__name__} completed in {elapsed_time:.2f} seconds[/cyan]")
        return result
    return wrapper

def generate_timestamp_str():
    """
    Generate a timestamp string for use in filenames.
    
    Returns:
        str: Timestamp string in YYYYMMDD_HHMMSS format
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(directory):
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory (str): Directory path to ensure exists
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        console.print(f"[green]Created directory: {directory}[/green]")

def display_markdown(markdown_text):
    """
    Display formatted markdown text in the console.
    
    Args:
        markdown_text (str): Markdown text to display
    """
    md = Markdown(markdown_text)
    console.print(md)

def format_currency(amount):
    """
    Format a number as currency.
    
    Args:
        amount (float): Amount to format
    
    Returns:
        str: Formatted currency string
    """
    return f"${amount:,.2f}"

def format_percent(value):
    """
    Format a decimal as a percentage.
    
    Args:
        value (float): Value to format (e.g., 0.1234)
    
    Returns:
        str: Formatted percentage string (e.g., "12.34%")
    """
    return f"{value * 100:.2f}%"

def is_database_ready(db_path):
    """
    Check if a DuckDB database exists and is ready for use.
    
    Args:
        db_path (str): Path to the DuckDB database file
    
    Returns:
        bool: True if the database exists and is ready
    """
    return os.path.exists(db_path)

def format_duration(seconds):
    """
    Format seconds into a human-readable duration.
    
    Args:
        seconds (float): Duration in seconds
    
    Returns:
        str: Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)} hours {int(minutes)} minutes"