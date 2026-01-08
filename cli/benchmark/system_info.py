import json
import re
from typing import Any, Dict, List

from yr.datasystem.cli.benchmark.executor import executor


class SystemInfoCollector:
    """Class for collecting system information from nodes."""

    @staticmethod
    def get_dscli_version(worker_address: str) -> tuple[str, str]:
        """Get version and commit id from dscli."""
        result = executor.execute('bash -l -c "source ~/.bashrc && dscli --version"', worker_address)
        version = ""
        commit_id = ""
        
        if hasattr(result, 'stdout'):
            stdout = result.stdout.strip()
            if stdout.startswith("dscli "):
                stdout = stdout[len("dscli "):]
            if stdout.endswith(")"):
                stdout = stdout[:-1]

            parts = stdout.split(" (commit: ")
            if len(parts) == 2:
                version = parts[0].strip()
                commit_id = parts[1].strip()
            else:
                for line in stdout.split('\n'):
                    if 'version' in line.lower():
                        version = line.split(':', 1)[1].strip() if ':' in line else line.strip()
                    if 'commit' in line.lower():
                        commit_id = line.split(':', 1)[1].strip() if ':' in line else line.strip()
        return version, commit_id

    @staticmethod
    def _parse_mem_info(mem_result: Any) -> tuple[str, str]:
        """Parse memory information from free -h output."""
        total_mem = ""
        free_mem = ""
        
        if hasattr(mem_result, 'stdout'):
            for line in mem_result.stdout.strip().split('\n'):
                if line.startswith('Mem:'):
                    parts = line.split()
                    if len(parts) >= 4:
                        total_mem = parts[1]
                        free_mem = parts[3]
                    break
        
        return total_mem, free_mem

    @staticmethod
    def _parse_cpu_info(cpu_result: Any) -> str:
        """Parse CPU MHz from lscpu output"""
        cpu_mhz = ""
        
        if hasattr(cpu_result, 'stdout'):
            for line in cpu_result.stdout.strip().split('\n'):
                if 'CPU MHz' in line:
                    try:
                        freq_str = line.split(':', 1)[1].strip()
                        freq_int = int(float(freq_str))
                        cpu_mhz = f"{freq_int}MHz"
                        break
                    except (ValueError, TypeError):
                        cpu_mhz = "Unknown"
        
        return cpu_mhz

    @staticmethod
    def _parse_thp_status(thp_result: Any) -> str:
        """Parse THP status."""
        thp_status = "unknown"

        if hasattr(thp_result, 'stdout'):
            output = thp_result.stdout.strip()
            current_status = [s.strip() for s in output.split() if '[' in s]
            if current_status:
                thp_status = current_status[0].strip('[]')
            else:
                if 'always' in output:
                    thp_status = "enabled"
                elif 'madvise' in output:
                    thp_status = "madvise"
                elif 'never' in output:
                    thp_status = "disabled"
        return thp_status

    @staticmethod
    def _parse_hugepages_status(huge_result: Any) -> str:
        """Parse HugePages status."""
        try:
            total_pages = 0
            free_pages = 0
            page_size_kb = 0
            if hasattr(huge_result, 'stdout'):
                stdout = str(huge_result.stdout).strip()
                for line in stdout.split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    if 'HugePages_Total' in line:
                        total_pages = int(line.split(':', 1)[1].strip())
                    elif 'HugePages_Free' in line:
                        free_pages = int(line.split(':', 1)[1].strip())
                    elif 'Hugepagesize' in line:
                        page_size_str = line.split(':', 1)[1].strip()
                        page_size_kb = int(''.join(filter(str.isdigit, page_size_str)))
                if page_size_kb > 0:
                    free_gb = int(free_pages * page_size_kb / (1024 ** 2))
                    total_gb = int(total_pages * page_size_kb / (1024 ** 2))
                else:
                    return "0G/0G"
                return f"{total_gb}G/{free_gb}G"
            else:
                return "0G/0G"
        except Exception as e:
            return "0G/0G"

    @staticmethod
    def get_node_info(ip: str) -> dict[str, Any]:
        """Get node information for a given IP."""
        # Get version and commit id
        version, commit_id = SystemInfoCollector.get_dscli_version(ip)
        
        # Get memory information
        mem_result = executor.execute("free -h", ip)
        total_mem, free_mem = SystemInfoCollector._parse_mem_info(mem_result)
        
        # Get CPU information
        cpu_result = executor.execute("lscpu", ip)
        cpu_mhz = SystemInfoCollector._parse_cpu_info(cpu_result)
        
        # Get THP status
        thp_result = executor.execute("cat /sys/kernel/mm/transparent_hugepage/enabled", ip)
        thp = SystemInfoCollector._parse_thp_status(thp_result)
        
        # Get HugePages status
        huge_result = executor.execute("grep Huge /proc/meminfo", ip)
        hugepages = SystemInfoCollector._parse_hugepages_status(huge_result)
        
        return {
            "IP": ip,
            "Version": version,
            "Commit ID": commit_id,
            "Total Memory": total_mem,
            "Free Memory": free_mem,
            "THP": thp,
            "HugePages": hugepages,
            "CPU MHz": cpu_mhz
        }

    @staticmethod
    def format_node_info_for_display(node_info: dict[str, Any]) -> str:
        """Format node info as key: value lines."""
        output = []
        for key, value in node_info.items():
            output.append(f"{key}: {value}")
        return '\n'.join(output)