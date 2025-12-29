"""
Dashboard tab modules.

Each module contains the render functions for a specific tab.
"""

from .overall_stats import render_overall_stats_tab
from .tasks import render_tasks_tab
from .execution_inspection import render_execution_inspection_tab
from .manual_import import render_manual_import_tab
from .auto_import import render_auto_import_tab
from .docs import render_docs_tab

__all__ = [
    "render_overall_stats_tab",
    "render_tasks_tab",
    "render_execution_inspection_tab",
    "render_manual_import_tab",
    "render_auto_import_tab",
    "render_docs_tab",
]
