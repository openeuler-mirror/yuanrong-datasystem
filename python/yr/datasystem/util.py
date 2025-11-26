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
The status of the return result.
"""
from yr.datasystem.lib import libds_client_py as ds


class Validator:
    """
    Features: Parse arguments
    """

    INT32_MAX_SIZE = int("0x7FFFFFFF", 16)  # Int32.MAX_VALUE
    UINT64_MAX_SIZE = 2**64 - 1 # UInt64.MAX_VALUE

    @staticmethod
    def check_args_types(args):
        """ Check the types of the input arguments

        Args:
            args(list): The input arguments, which is a list of lists. Each list inside contains an argument name,
            the argument value and its expected valid types.
            Example: args = [["value", value, bytes, memoryview], ["timeout", timeout, int]]. Which means the argument
            value should have the type of bytes or memoryview and the timeout argument should be an integer.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
        """
        if not isinstance(args, list):
            raise TypeError(r"The input of args should be a list, error type: {err}".format(err=type(args)))
        for arguments in args:
            if not isinstance(arguments, list):
                raise TypeError(r"Each element of the input of args should be a list, error type: {err}".format(
                    err=type(arguments)))
            if len(arguments) < 3:
                raise TypeError(r"Each element of the input of args should have the length at least 3, which "
                                r"contains an argument name, the argument value and its expected valid types.")
            arg_name = arguments[0]
            arg_value = arguments[1]
            arg_types = arguments[2:]
            valid = False
            for arg_type in arg_types:
                # Use strict type checking with type() is instead of isinstance()
                if type(arg_value) is arg_type:
                    valid = True
                    break
            if valid is False:
                raise TypeError(r"The input of {name} has invalid type, valid type: {type}".format(name=arg_name,
                                                                                                   type=arg_types))

    @staticmethod
    def check_key_exists(args, keys):
        """ Check the types of the input arguments

        Args:
            args(dict): The input arguments.
            keys(list): a list of strings

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.

        Returns:
            res: A list of the values of the given keys
        """
        if not isinstance(args, dict):
            raise TypeError(r"The input of args should be dict, error type: {err}".format(err=type(args)))
        if not isinstance(keys, list):
            raise TypeError(r"The input of keys should be list, error type: {err}".format(err=type(keys)))
        res = []
        for key in keys:
            k = args.get(key)
            if k is None:
                raise TypeError(r"The key '{k_val}' of the input param does not exist".format(k_val=key))
            res.append(k)
        return res

    @staticmethod
    def check_param_range(param_name, param_value, min_limit, max_limit):
        """ Check the range of the input arguments

        Args:
            param_name(str): The name of param
            param_value(str): The value of param
            min_limit(int): The Maximum limit
            max_limit(int): The Minimum limit

        Raises:
            RuntimeError: Raise a RuntimeError if the range of the input parameter is invalid.
        """
        if not min_limit <= param_value <= max_limit:
            raise RuntimeError(r"Invalid {} size {} is set, which should be between [{},{}]".format(
                param_name, param_value, min_limit, max_limit))


class Status:
    """
    Features: The status of the return result
    """

    def __init__(self, status):
        """ Wrap the status of result.

        Args:
            status: libds_client_py.Status
        """
        self._status = status

    def is_ok(self):
        """ Whether the result is ok.

        Returns:
            Return True if is ok.
        """
        return self._status.is_ok()

    def is_error(self):
        """ Whether the result is error.

        Returns:
            Return True if is error.
        """
        return self._status.is_error()

    def to_string(self):
        """ Get the message of status.

        Returns:
            Return the message of status.
        """
        return self._status.to_string()


class Context:
    """
    Features: Data system Context for python.
    """
    @staticmethod
    def set_trace_id(trace_id: str):
        """ Set trace id for all API calls of the current thread.

        Raises:
            RuntimeError: Raise a runtime error if the trace_is is invalid.
        """
        status = ds.Context.set_trace_id(trace_id)
        if status.is_error():
            raise RuntimeError(status.to_string())
