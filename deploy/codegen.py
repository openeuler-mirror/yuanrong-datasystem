#!/usr/bin/env python
# coding=utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module manager
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import with_statement

import errno
import getopt
import json
import re
import os
import stat
import sys
from shutil import copyfile
from shutil import rmtree


class Conf:
    """
    Configuration item structure.

    Attributes:
        name: gflags name.
        env: environment variables for conf files.
        default: environment variables default value.
        meaning: environment variables description.
    """

    def __init__(self, prefix, name, default, meaning, flag_prefix, flag_hide):
        self.prefix = prefix
        self.name = name
        self.default = default
        self.meaning = meaning
        self.env = self.name.upper()
        if not self.env.startswith(prefix.upper()):
            self.env = prefix.upper() + '_' + self.env
        self.flag_prefix = flag_prefix
        self.flag_hide = flag_hide

    def __str__(self):
        return "Conf name: [{}], default: [{}], meaning: [{}], env: [{}]".format(self.name, self.default, self.meaning,
                                                                                 self.env)


class ConfParser:
    """
    Parse component configuration file and format to Conf list.

    Attributes:
        component: worker.
        dir_path: configuration files saved directory.
    """

    def __init__(self, component, dir_path):
        self.component = component
        self.dir_path = dir_path

    @staticmethod
    def load(json_file):
        """
        Load json file.

        Args:
            json_file: Json file path.

        Returns:
            A json object

        Raise:
            IOError: json_file not exist.
        """
        with open(json_file, 'r') as file:
            json_obj = json.load(file)
            return json_obj

    def parse(self):
        """
        Parse deploy configuration json file.

        Returns:
            A conf list that contains configuration items.

        Raise:
            IOError: json_file not exist.
        """
        component_json = ConfParser.load(os.path.join(self.dir_path, '{}-env.json'.format(self.component)))
        flag_items = component_json['common']
        conf_list = []
        for flag_item in flag_items:
            flag_item.setdefault('prefix', '')
            flag_item.setdefault('hide_flag', '')
            conf = Conf(self.component, flag_item['flag'], flag_item['default'], flag_item['description'],
                        flag_item['prefix'], flag_item['hide_flag'])
            conf_list.append(conf)
        return conf_list


def gen_conf_file(conf_list, output_file):
    """
    Generate the configure file via configure item list.

    Args:
        conf_list: Configuration item list, used for generate the output file.
        output_file: Output file path.

    Returns:
        None

    Raise:
        IOError: output_file not exist.
        OSError: create directory failed.
    """
    # if output_file parent not exist, we need to create it first.
    if not os.path.exists(os.path.dirname(output_file)):
        try:
            os.makedirs(os.path.dirname(output_file))
        # Guard against race condition
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                print('Create directory {} failed: {}'.format(os.path.dirname(output_file), exc))
                raise

    with os.fdopen(os.open(output_file, os.O_RDWR | os.O_CREAT, stat.S_IRUSR | stat.S_IWUSR), "w") as conf_file:
        # 1. write the header
        header = '#!/bin/bash\n' \
                 '# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.\n' \
                 '#\n' \
                 '# Licensed under the Apache License, Version 2.0 (the "License");\n' \
                 '# you may not use this file except in compliance with the License.\n' \
                 '# You may obtain a copy of the License at\n' \
                 '#\n' \
                 '# http://www.apache.org/licenses/LICENSE-2.0\n' \
                 '#\n' \
                 '# Unless required by applicable law or agreed to in writing, software\n' \
                 '# distributed under the License is distributed on an "AS IS" BASIS,\n' \
                 '# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n' \
                 '# See the License for the specific language governing permissions and\n' \
                 '# limitations under the License.\n' \
                 '#\n' \
                 '# Edit this file to configure startup parameters, it is sourced to launch components.\n'
        conf_file.write(header)

        for conf in conf_list:
            # 2. write the description.
            conf_file.write('\n')
            conf_file.write('# {} (Default: "{}")\n'.format(conf.meaning, conf.default))
            # 3. write the environment variables.
            conf_file.write('# {}="{}"\n'.format(conf.env, conf.default))


def gen_launcher_file(comp, template_file, output_file):
    """
    Generate component-launcher.sh for component launch to local or remote hosts.

    Args:
        comp: component
        template_file: template file to generate the component-launcher.sh
        output_file: output file

    Returns:
        None.

    Raise:
        IOError: output_file not exist.
        OSError: create directory failed.
    """
    # if output_file parent not exist, we need to create it first.
    if not os.path.exists(os.path.dirname(output_file)):
        try:
            os.makedirs(os.path.dirname(output_file))
        # Guard against race condition
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                print('Create directory {} failed: {}'.format(os.path.dirname(output_file), exc))
                raise

    if comp == 'clusterfs':
        comp_upper = 'CLUSTERFS_WORKER'
    else:
        comp_upper = comp.upper()

    launcher_conf = '${BASE_DIR}/conf'

    subs = {
        'component': comp,
        'COMPONENT': comp_upper,
        'launch-conf': launcher_conf
    }

    with open(template_file, "r") as fin:
        with os.fdopen(os.open(output_file, os.O_RDWR | os.O_CREAT, stat.S_IRUSR | stat.S_IWUSR), "w") as fout:
            for line in fin:
                fout.write(re.sub('@([^\\s]*?)@', from_dict(subs), line))


def from_dict(dct):
    """
    Look up the match key and return its value.

    Args:
        dct: Dict

    Returns:
        The match value.
    """

    def lookup(match):
        key = match.group(1)
        return dct.get(key, '')

    return lookup

STOP_FUNC_STR = 'function stop_one_{0}()\n' \
                '{{\n' \
                '  is_array "{1}_ADDRESS" && {0}_address="${{{1}_ADDRESS[$COMPONENT_NUM]}}" ||' \
                ' {0}_address="${{{1}_ADDRESS}}"\n' \
                '  local pid="$(ps ux | grep /datasystem_worker | grep {0}_address=${{{0}_address}} | grep -v grep' \
                ' | awk \'{{print $2}}\')"\n' \
                '  if ! is_num "${{pid}}"; then\n' \
                '    echo -e "Cannot found the {0} we want: ${{{0}_address}}" >&2\n' \
                '    exit 0\n' \
                '  fi\n' \
                '  kill -15 "$pid"\n' \
                '  local timeout=1800\n' \
                '  local start_time=$(date +%s)\n' \
                '  while kill -0 $pid; do \n' \
                '    if [[ $(($(date +%s) - start_time)) -ge $timeout ]]; then\n' \
                '      echo "Termination timeout (30 minutes) reached - killing process."\n' \
                '      kill -9 $pid\n' \
                '      break\n' \
                '    fi\n' \
                '    sleep 0.5\n' \
                '  done\n' \
                '}}\n'

DEPLOY_FUNC_STR = 'function deploy_{0}()\n' \
                  '{{\n' \
                  '  if [ x"${{ACTION}}" = "xstart" ]; then\n' \
                  '    "${{LAUNCHER[@]}}" "${{BASE_DIR}}/{0}-launcher.sh" "${{BASE_DIR}}/deploy-datasystem.sh"' \
                  ' "-a" "start" "-c" "{0}" "-p" "${{CONF_DIR}}"\n' \
                  '  else\n' \
                  '    "${{LAUNCHER[@]}}" "${{BASE_DIR}}/{0}-launcher.sh" "${{BASE_DIR}}/deploy-datasystem.sh"' \
                  ' "-a" "stop" "-c" "{0}" "-p" "${{CONF_DIR}}"\n' \
                  '  fi\n' \
                  '}}\n'

DEPLOY_ONE_FUNC_STR = 'function deploy_one_{0}()\n' \
                      '{{\n' \
                      '  . "${{CONF_DIR}}/{0}-env.sh"\n' \
                      '  if [ x"${{ACTION}}" = "xstart" ]; then\n' \
                      '    start_one_{0}\n' \
                      '  else\n' \
                      '    stop_one_{0}\n' \
                      '  fi\n' \
                      '}}\n'

STOP_CLUSTER_STR = 'function stop_one_clusterfs()\n' \
                   '{\n' \
                   '  is_array "CLUSTERFS_WORKER_ADDRESS" &&' \
                   ' cluster_address="${CLUSTERFS_WORKER_ADDRESS[$COMPONENT_NUM]}" ||' \
                   ' cluster_address="${CLUSTERFS_WORKER_ADDRESS}"\n' \
                   '  pid="$(ps ux | grep bin/clusterfs | grep ${cluster_address} ' \
                   '| grep -v grep | awk \'{print $2}\')"\n' \
                   '  ps ux | grep bin/clusterfs | grep worker_address=${cluster_address} ' \
                   '| grep -v grep | awk \'{print $NF}\' | xargs fusermount3 -u\n' \
                   '  while [[ -n $(ps -p "$pid" | grep "$pid") ]]; do sleep 0.5; done\n' \
                   '}\n'


def gen_start_function(comp, conf_list, func_list):
    """
    Generate start_one_component function for component

    Args:
        comp: component
        conf_list: configuration items list
        func_list: function list to be append

    Returns:
        None.
    """
    indent = ' ' * 2
    func_list.append('')
    func_list.append('function start_one_{}()'.format(comp))
    func_list.append('{')
    func_list.append('{}local argv_list'.format(indent))
    gen_arg_list(comp, conf_list, func_list)
    func_list.append('{}export LD_LIBRARY_PATH="${{BIN_DIR}}/lib:$LD_LIBRARY_PATH"'.format(indent))
    func_list.append(
        f'{indent}(nohup "${{BIN_DIR}}/datasystem_worker" "${{argv_list[@]}}" '
        f'>${{BASE_DIR}}/{comp}.out 2>&1) &'
    )
    func_list.append('{}local pid=$!'.format(indent))
    func_list.append('{}sleep 5'.format(indent))
    func_list.append('{}[[ -n $(ps -p "$pid" | grep "$pid") ]] && ps -p "$pid" -o args || ret_code=1'.format(indent))
    func_list.append('{}if [[ $ret_code -ne 0 ]]; then'.format(indent))
    func_list.append('{}cat ${{BASE_DIR}}/{}.out'.format(indent * 2, comp))
    func_list.append('{}fi'.format(indent))
    func_list.append('{}return $ret_code'.format(indent))
    func_list.append('}')
    func_list.append('')
    if comp == 'clusterfs':
        func_list.append(STOP_CLUSTER_STR)
    else:
        func_list.append(STOP_FUNC_STR.format(comp, comp.upper()))
    func_list.append(DEPLOY_ONE_FUNC_STR.format(comp))
    func_list.append(DEPLOY_FUNC_STR.format(comp))


def gen_arg_list(comp, conf_list, func_list):
    """
    Generate argument list for component

    Args:
        comp: component
        conf_list: configuration items list
        func_list: function list to be append

    Returns:
        None.
    """
    indent = ' ' * 2
    for conf in conf_list:
        func_list.append('{0}is_array "{1}" && {2}="${{{1}[$COMPONENT_NUM]}}" '
                         '|| {2}="${{{1}}}"'.format(indent, conf.env, conf.name))
        if comp != "clusterfs":
            flag = '-{}'.format(conf.name)
            func_list.append(
                '{0}[[ -n "${{{1}}}" ]] && argv_list+=("{2}=${{{1}}}")'.format(indent, conf.name, flag))
            continue

        if conf.flag_hide:
            func_list.append(
                '{0}[[ -n "${{{1}}}" ]] && argv_list+=("${{{1}}}")'.format(indent, conf.name))
            continue

        if conf.flag_prefix == '':
            flag = '--{}'.format(conf.name)
            func_list.append(
                '{0}[[ -n "${{{1}}}" ]] && argv_list+=("{2}=${{{1}}}")'.format(indent, conf.name, flag))
            continue

        func_list.append(
            '{0}[[ -n "${{{1}}}" ]] && argv_list+=("{2}" "{3}=${{{1}}}")'.format(indent, conf.name,
                                                                                        conf.flag_prefix,
                                                                                        conf.name))
    if comp == 'clusterfs':
        func_list.append('  mkdir -p "${CLUSTERFS_MOUNT_DIR}"')
        func_list.append('  chmod -R 700 "${CLUSTERFS_MOUNT_DIR}"')


def gen_shell_script(comp_dict, template_dir, output_dir):
    """
    Generate shell scripts via configure item list.

    Args:
        comp_dict: master, worker or gcs
        template_dir: Configuration item list, used for generate the output file.
        output_dir: template file for generate output files.

    Returns:
        None

    Raise:
        IOError: output_file not exist.
        OSError: create directory failed.
    """

    func_list = []
    deploy_list = []
    path_list = []
    for comp, conf_list in comp_dict.items():
        gen_start_function(comp, conf_list, func_list)
        if comp not in ['master', 'worker']:
            if comp == 'clusterfs':
                deploy_list.append('    if [ "x${HANDLE_CLUSTERFS}" = "xYes" ]; then')
                deploy_list.append('      deploy_{}'.format(comp))
                deploy_list.append('    fi')
            else:
                deploy_list.append('    deploy_{}'.format(comp))

    path_list.append('export BIN_DIR="$(realpath "${BASE_DIR}/..")"')
    path_list.append('export CONF_DIR=$(realpath "${BASE_DIR}/conf")')

    subs = {
        'function': '\n'.join(func_list),
        'deploy': '\n'.join(deploy_list),
        'path': '\n  '.join(path_list)
    }

    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        # Guard against race condition
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                print('Create directory {} failed: {}'.format(output_dir, exc))
                raise

    with open(os.path.join(template_dir, "deploy-datasystem.sh.template"), "r") as fin, \
            os.fdopen(os.open(os.path.join(output_dir, "deploy-datasystem.sh"), os.O_RDWR | os.O_CREAT,
                              stat.S_IRUSR | stat.S_IWUSR), "w") as fout:
        for line in fin:
            if line == "    deploy_master\n":
                continue
            fout.write(re.sub('@([^\\s]*?)@', from_dict(subs), line))
    copyfile(os.path.join(template_dir, 'deploy-common.sh.template'), os.path.join(output_dir, 'deploy-common.sh'))


def main(argv):
    """conf_list
    Main to start the deploy script.

    Args:
        argv: external input arguments.

    Returns:
        0 on success, otherwise return 1.
    """
    try:
        opts, _ = getopt.getopt(argv, 'h:t:i:o:s:c:',
                                ['help', 'temp_in=', 'conf_in=', 'conf_out=', 'script_out=', 'comps='])
    except getopt.GetoptError as exception:
        print('Parse input arguments error: {}'.format(exception))
        sys.exit(1)

    temp_in = ''
    conf_in = ''
    conf_out = ''
    script_out = ''
    comps = []
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print("helpful usage")
        elif opt in ('-t', '--temp_in'):
            temp_in = arg
        elif opt in ('-i', '--conf_in'):
            conf_in = arg
        elif opt in ('-o', '--conf_out'):
            conf_out = arg
        elif opt in ('-s', '--script_out'):
            script_out = arg
        elif opt in ('-c', '--comps'):
            comps = arg.split(',')

    comp_dict = {}

    if os.path.isdir(conf_out):
        rmtree(conf_out)

    if os.path.isdir(script_out):
        rmtree(script_out)

    for component in comps:
        # 1. parse json files.
        conf_list = ConfParser(component, conf_in).parse()
        comp_dict[component] = conf_list
        # 2. generate conf files.
        gen_conf_file(conf_list, os.path.join(conf_out, '{}-env.sh'.format(component)))
        # 3. generate launcher files.
        gen_launcher_file(component, os.path.join(temp_in, "component-launcher.sh.template"),
                          os.path.join(script_out, '{}-launcher.sh'.format(component)))

    # 4. generate deploy-datasystem.sh
    gen_shell_script(comp_dict, temp_in, script_out)


if __name__ == "__main__":
    main(sys.argv[1:])
