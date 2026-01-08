import argparse
import socket
from typing import Any, List

from yr.datasystem.cli.benchmark.common import (
    BaseCommand,
    BenchSuite,
)

from yr.datasystem.cli.benchmark.common import BaseCommand
from yr.datasystem.cli.benchmark.system_info import SystemInfoCollector

from yr.datasystem.cli.benchmark.task import BenchArgs


class ShowCommand(BaseCommand):
    """Command to show node information."""
    name = "show"
    description = "Show node system information."
    def __init__(self, *args, **kwargs):
        """Initialize ShowCommand."""
        super().__init__(*args, **kwargs)
        self.command_name = "show"

    def add_arguments(self, parser: argparse.ArgumentParser):
        pass

    def validate(self, args: Any) -> bool:
        """Validate command-line arguments. Return False if invalid."""
        pass

    def initialize(self, args: Any) -> bool:
        """Initializes the benchmark runner with the provided arguments."""
        pass

    def pre_run(self) -> bool:
        """Performs pre-execution checks and setup tasks."""
        pass

    def build_suite(self, bench_args: BenchArgs) -> BenchSuite:
        """Constructs and returns a benchmark suite object."""
        pass

    def _build_parser(self) -> argparse.ArgumentParser:
        """Build argument parser for show command."""
        parser = argparse.ArgumentParser(
            prog="dsbench show",
            description="Show node system information.",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        return parser

    def _get_local_node_info(self) -> dict[str, Any]:
        """Get local node information."""
        local_ip = socket.gethostbyname(socket.gethostname())
        return SystemInfoCollector.get_node_info(local_ip)

    def print_node_info(self, node_info: dict[str, Any]):
        """Print node information in key: value format."""
        self.logger.info(SystemInfoCollector.format_node_info_for_display(node_info))

    def run(self, args: Any) -> int:
        """Run the show command."""
        # Get local node information
        node_info = self._get_local_node_info()
        
        # Print the information
        self.print_node_info(node_info)
        
        return 0