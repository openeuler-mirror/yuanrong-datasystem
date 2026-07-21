#!/usr/bin/env python3
import argparse
import logging
from pathlib import Path
from xml.etree import ElementTree as ET


LOG = logging.getLogger(__name__)

CMAKE_WORKSPACE_COMPONENTS = {
    "CMakePresetLoader",
    "CMakeProjectFlavorService",
    "CMakeRunConfigurationManager",
    "CMakeSettings",
    "VCPKGProject",
}

EXCLUDED_PROJECT_PATHS = (
    ".bazel-cache",
    ".clion-remote",
    ".codegraph",
    "bazel-bin",
    "bazel-out",
    "bazel-testlogs",
    "cmake-build-debug",
)


def write_xml(path, root):
    ET.indent(root, space="  ")
    tree = ET.ElementTree(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(path, encoding="UTF-8", xml_declaration=True)


def write_compdb_misc(idea_dir):
    project = ET.Element("project", {"version": "4"})
    settings = ET.SubElement(project, "component", {"name": "CompDBSettings"})
    linked = ET.SubElement(settings, "option", {"name": "linkedExternalProjectsSettings"})
    project_settings = ET.SubElement(linked, "CompDBProjectSettings")
    ET.SubElement(project_settings, "option", {"name": "externalProjectPath", "value": "$PROJECT_DIR$"})
    modules_option = ET.SubElement(project_settings, "option", {"name": "modules"})
    modules_set = ET.SubElement(modules_option, "set")
    ET.SubElement(modules_set, "option", {"value": "$PROJECT_DIR$"})
    ET.SubElement(project, "component", {"name": "CompDBWorkspace", "PROJECT_DIR": "$PROJECT_DIR$"})
    ET.SubElement(project, "component", {"name": "ExternalStorageConfigurationManager", "enabled": "true"})
    write_xml(idea_dir / "misc.xml", project)


def bazel_execroot_symlink(project_name):
    return f"bazel-{project_name}"


def write_compdb_module_files(idea_dir, project_name):
    iml_name = f"{project_name}.iml"

    modules_project = ET.Element("project", {"version": "4"})
    manager = ET.SubElement(modules_project, "component", {"name": "ProjectModuleManager"})
    modules = ET.SubElement(manager, "modules")
    ET.SubElement(
        modules,
        "module",
        {
            "fileurl": f"file://$PROJECT_DIR$/.idea/{iml_name}",
            "filepath": f"$PROJECT_DIR$/.idea/{iml_name}",
        },
    )
    write_xml(idea_dir / "modules.xml", modules_project)

    iml = ET.Element(
        "module",
        {
            "classpath": "CIDR",
            "external.linked.project.id": project_name,
            "external.linked.project.path": "$MODULE_DIR$/..",
            "external.root.project.path": "$MODULE_DIR$/..",
            "external.system.id": "CompDB",
            "type": "CPP_MODULE",
            "version": "4",
        },
    )
    component = ET.SubElement(iml, "component", {"name": "NewModuleRootManager"})
    content = ET.SubElement(component, "content", {"url": "file://$MODULE_DIR$/.."})
    for excluded in (*EXCLUDED_PROJECT_PATHS, bazel_execroot_symlink(project_name)):
        ET.SubElement(content, "excludeFolder", {"url": f"file://$MODULE_DIR$/../{excluded}"})
    write_xml(idea_dir / iml_name, iml)


def ensure_workspace(idea_dir):
    workspace_path = idea_dir / "workspace.xml"
    if workspace_path.exists():
        root = ET.parse(workspace_path).getroot()
    else:
        root = ET.Element("project", {"version": "4"})

    for component in list(root):
        if component.tag != "component":
            continue
        name = component.get("name")
        if name in CMAKE_WORKSPACE_COMPONENTS:
            root.remove(component)
            continue
        if name == "RunManager":
            for config in list(component):
                if config.tag == "configuration" and config.get("type") == "CMakeRunConfiguration":
                    component.remove(config)

    write_xml(workspace_path, root)


def ensure_clion_compdb_project(project_dir):
    project_dir = Path(project_dir).resolve()
    idea_dir = project_dir / ".idea"
    idea_dir.mkdir(parents=True, exist_ok=True)
    write_compdb_misc(idea_dir)
    write_compdb_module_files(idea_dir, project_dir.name)
    ensure_workspace(idea_dir)


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Keep CLion project files in CompDB mode.")
    parser.add_argument("project_dir", nargs="?", default=".", help="CLion project root")
    args = parser.parse_args()
    ensure_clion_compdb_project(Path(args.project_dir))
    LOG.info("CLion CompDB project files are ready.")


if __name__ == "__main__":
    main()
