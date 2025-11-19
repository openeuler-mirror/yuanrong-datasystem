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

import csv
import argparse
import os

stream_headers = [
    "Time",
    "Stream Name",
    "NumLocalProducers",
    "NumRemoteProducers",
    "NumLocalConsumers",
    "NumRemoteConsumers",
    "SharedMemoryUsed",
    "LocalMemoryUsed",
    "NumTotalElementsSent",
    "NumTotalElementsReceived",
    "NumTotalElementsAcked",
    "NumSendRequests",
    "NumReceiveRequests",
    "NumPagesCreated",
    "NumPagesReleased",
    "NumPagesCached",
    "NumBigPagesCreated",
    "NumBigPagesReleased",
    "NumLocalProducersBlocked",
    "NumRemoteProducersBlocked",
    "NumRemoteConsumersBlocking",
    "RetainDataState",
    "StreamState",
]

worker_headers = [
    "Time",
    "TotalNumberStreams",
    "TotalNumberInActiveStreams",
    "TotalStreamMemoryUsed",
    "TotalStreamMemoryLimit",
]

retain_data_state = ["NONE", "INIT", "RETAIN", "NOT_RETAIN"]

stream_manager_state = [
    "ACTIVE",
    "RESET_IN_PROGRESS",
    "RESET_COMPLETE",
    "DELETE_IN_PROGRESS",
]

METRIC_LOG_ENTRY_INDEX = 7


def parse_args():
    """
    Parse arguments
    """
    parser = argparse.ArgumentParser(
        description="""Parses worker and stream metrics in sc_metrics.log file.
        \rOutputs metrics to sc_worker_metrics.csv and sc_stream_metrics.csv respectively""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Example: Get worker and stream metrics from sc_metrics.log file, filtering on stream1,stream2
                                     \r $ python parse_sc_metrics.py sc_metrics.log -t sw -f stream1,stream2""",
    )
    parser.add_argument("filename", help="path to the sc_metrics.log file")
    parser.add_argument(
        "-t",
        "--type",
        help="metric types to parse from the file. (s = stream, w = worker, sw or ws = stream and worker)",
        choices=["s", "w", "sw", "ws"],
        default="sw",
    )
    parser.add_argument(
        "-f",
        "--filter",
        help='filter stream metrics by stream names. Separate stream names with ","',
    )
    parser.add_argument(
        "-o", "--output", help="path of the output folder, default: ./", default="./"
    )
    args = parser.parse_args()

    parse_worker = "w" in args.type
    parse_stream = "s" in args.type

    if args.filter is None:
        stream_filter = None
    else:
        stream_filter = args.filter.split(",")

    return (
        args.filename,
        parse_worker,
        parse_stream,
        stream_filter,
        args.output,
    )


def print_info_message(
    filename, parse_worker, parse_stream, stream_filter, output
):
    """
    Prints info message
    """
    if parse_worker and parse_stream:
        print("Parsing worker, stream metrics from", filename)
    elif parse_worker:
        print("Parsing worker metrics from", filename)
    elif parse_stream:
        print("Parsing stream metrics from", filename)
    else:
        print("error")
    if stream_filter:
        print("Filtering on stream names:", stream_filter)
    print("Outputing to directory:", output)


def parse_sc_metrics(filename, parse_worker, parse_stream, stream_filter, output):
    """
    Parse metrics from log file
    """
    # Start parsing log file
    file = open(filename, "r")
    # create output directory
    if not os.path.exists(output):
        os.makedirs(output)
    if parse_stream:
        stream_csv = open(os.path.join(output, "sc_stream_metrics.csv"), "w")
        stream_wr = csv.writer(stream_csv)
        stream_wr.writerow(stream_headers)
    if parse_worker:
        worker_csv = open(os.path.join(output, "sc_worker_metrics.csv"), "w")
        worker_wr = csv.writer(worker_csv)
        worker_wr.writerow(worker_headers)

    for line in file:
        categories = line.split("|")
        # actual log line located at index 7
        metric_log = categories[METRIC_LOG_ENTRY_INDEX]
        time = categories[0].rstrip()
        if parse_worker and "Worker metrics" in metric_log:
            metrics = metric_log.split("/")
            metrics[len(metrics) - 1] = metrics[len(metrics) - 1].rstrip()
            metrics.pop(0)
            metrics.insert(0, time)
            worker_wr.writerow(metrics)
        elif parse_stream and "Worker metrics" not in metric_log and "master " not in metric_log:
            metrics = metric_log.split("/")
            metrics[0] = metrics[0].lstrip()
            # filter based on stream name
            if stream_filter is None or metrics[0] in stream_filter:
                metrics[len(metrics) - 1] = metrics[len(metrics) - 1].rstrip()
                metrics.insert(0, time)
                # convert enums to string
                metrics[
                    stream_headers.index("RetainDataState")
                ] = retain_data_state[
                    int(metrics[stream_headers.index("RetainDataState")])
                ]
                metrics[stream_headers.index("StreamState")] = stream_manager_state[
                    int(metrics[stream_headers.index("StreamState")])
                ]
                stream_wr.writerow(metrics)

    # close files
    if parse_stream:
        stream_csv.close()
    if parse_worker:
        worker_csv.close()
    file.close()


def main():
    """
    Main execution
    """
    (
        filename,
        parse_worker,
        parse_stream,
        stream_filter,
        output,
    ) = parse_args()

    print_info_message(
        filename, parse_worker, parse_stream, stream_filter, output
    )

    parse_sc_metrics(filename, parse_worker, parse_stream, stream_filter, output)

    print("Parse metrics success")


if __name__ == "__main__":
    main()
