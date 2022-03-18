import argparse
import datetime
import json
import re
import subprocess
import time
from asyncio.subprocess import PIPE
from configparser import ConfigParser
from typing import Dict, List


def execute_subcommand(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out = process.communicate()[0].strip()
    return out.decode("UTF-8")


def stream_subcommand(command: List, args: List, other_flags: Dict = {"stdout": PIPE, "stderr": subprocess.DEVNULL}):
    commands = []
    commands += command
    commands += args
    with subprocess.Popen(commands, **other_flags) as proc:
        for line in iter(proc.stdout.readline, b""):
            line = line.decode()
            yield line


def check_kcat_exists(kcat_path: str = "kcat"):
    resp = execute_subcommand(f"{kcat_path} -V")
    if resp.startswith("kcat - Apache Kafka producer and consumer tool"):
        return True
    else:
        return False


def parse_time_switch(value: str):
    pattern_value = "(\d+)([DHMSdhms])"
    is_failed = re.match(pattern_value, value)
    if not is_failed:
        return False
    resp = re.split(pattern_value, value)
    output = []
    for item in resp:
        if item:
            output.append(item.lower())
    if output[1] == "d":
        return {"days": int(output[0])}
    elif output[1] == "h":
        return {"hours": int(output[0])}
    elif output[1] == "m":
        return {"minutes": int(output[0])}
    elif output[1] == "s":
        return {"seconds": int(output[0])}


def generate_timestamp(baseline=datetime.datetime.today(), **kwargs):
    ts_value = int((baseline - datetime.timedelta(**kwargs)).timestamp() * 1000)
    return ts_value


def read_config_file(filepath: str = "config.props"):
    from configparser import ConfigParser

    file_parser = ConfigParser()
    file_parser.read(filepath)
    return file_parser


def get_config_props(configs: ConfigParser, section: str, option: str):
    return configs.get(section, option)


def pattern_regex_creator(switch: List, chain_op: str = "~~"):
    token_pattern = []
    for item in switch:
        if chain_op in item:
            tokens = str(item).split(chain_op)
            token_pattern += ["^" + "".join([f"(?=.*{piece})" for piece in tokens]) + ".*$"]
        else:
            token_pattern += [item]
    final_pattern = "(?:" + "|".join(token_pattern) + ")"
    return final_pattern


def prepare_switches(configs: ConfigParser, begin_time: Dict, end_time: Dict, topics, section="kcat"):
    bs = configs.get(section, "bootstrap.servers")
    switches = []
    switches += ["-b", bs]
    configs[section].pop("bootstrap.servers")
    for key, value in configs[section].items():
        switches += ["-X", f"{key}={value}"]
    switches += ["-m", "10"]
    switches += ["-e"]
    switches += ["-C"]
    switches += ["-J"]
    begin_ts = generate_timestamp(datetime.datetime.today(), **begin_time)
    end_ts = generate_timestamp(datetime.datetime.today(), **end_time)
    switches += ["-t", " ".join(topics)]
    switches += ["-o", f"s@{begin_ts}"]
    switches += ["-o", f"e@{end_ts}"]
    return switches


def search_and_display(input_line: str, match_pattern, match_switches):
    if re.search(match_pattern, input_line, *match_switches):
        data = json.loads(input_line)
        print(
            "{:<50} {:<4} {:<10} {:<30} {:<200}".format(
                data["topic"], data["partition"], data["offset"], time.ctime(data["ts"]), str(data["payload"])[:200]
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application",
        add_help=True,
    )

    global_args = parser.add_argument_group("application-args", "switches available for the application")

    global_args.add_argument(
        "-k",
        type=str,
        default="kcat",
        metavar="file path for locating kcat.",
        help="Override location for findding kcat executable if kcat is not available in path.",
    )

    global_args.add_argument(
        "-c",
        type=str,
        default=argparse.SUPPRESS,
        required=True,
        metavar="<file path of the config props file>",
        help="This is the config file that contains the property values to execute the script.",
    )

    global_args.add_argument(
        "-t",
        nargs="+",
        default=argparse.SUPPRESS,
        required=True,
        metavar="topicA, topicB, topicC",
        help="This is the config file that contains the property values to execute the script.",
    )

    global_args.add_argument(
        "-b",
        type=str,
        default="1d",
        metavar="1d",
        help="Begin reading the data from x (d)ays/(h)ours/(m)onths/(s)econds ago.",
    )

    global_args.add_argument(
        "-e",
        type=str,
        default="0s",
        metavar="1d",
        help="End reading the data from x (d)ays/(h)ours/(m)onths/(s)econds ago. Leave out the switch for reading till now.",
    )

    global_args.add_argument(
        "-s",
        nargs="+",
        default="",
        # required=False,
        metavar="searchterm1, searchterm2, searchterm3",
        help="Search string to be used for locating the message. The search string could be in the body, header, key, offset. It is a brute force search across all of them.",
    )

    global_args.add_argument(
        "-i",
        default=False,
        action="store_true",
        help="Make the search string case insensitive",
    )

    args = parser.parse_args()

    # Check kcat availability
    kcat_path = [args.k]
    if not check_kcat_exists(kcat_path[0]):
        raise Exception("cannot find kcat. Please check if it exists in path")

    # Read configurations that will be streamed to kcat as -X switches.
    configs = read_config_file(args.c)

    starttime = generate_timestamp()
    begin_time_switch = parse_time_switch(args.b)
    if not begin_time_switch:
        raise Exception("Begin switch is not the proper formatting. <numbers>D/M/H/S are the only acceptable values.")
    end_time_switch = parse_time_switch(args.e)
    if not end_time_switch:
        raise Exception("End switch is not the proper formatting. <numbers>D/M/H/S are the only acceptable values.")

    kcat_switches = prepare_switches(configs, begin_time_switch, end_time_switch, args.t)
    kcat_pattern = pattern_regex_creator(args.s)
    kcat_match_switches = []
    if args.i:
        kcat_match_switches += [re.IGNORECASE]

    print("{:<50} {:<4} {:<10} {:<30} {:<200}".format("Topic Name", "P#", "Offset", "Timestamp", "Payload"))
    print("=" * 300)
    for line in stream_subcommand(kcat_path, kcat_switches):
        search_and_display(line, kcat_pattern, kcat_match_switches)
