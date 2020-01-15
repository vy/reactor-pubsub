#!/usr/bin/env python
# coding=utf-8


import logging
import os
import re
import subprocess
import sys
import time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)7s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
LOGGER = logging.getLogger(__name__)


BENCHMARK_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(BENCHMARK_DIR, ".."))
CPU_COUNT = 3


MVN_ENV = os.environ.copy()
MVN_ENV["MAVEN_OPTS"] = "-XX:+TieredCompilation -Xmx1g --illegal-access=deny"


MESSAGE_COUNTS = list([50, 100, 500, 1000])
PAYLOAD_LENGTHS = [(byte_count * 1024) for byte_count in [1, 4, 8, 16]]
CONCURRENCIES = list([1, 2, 3, 4, 5, 6])
TOTAL_PAYLOAD_LENGTH = max(MESSAGE_COUNTS) * max(PAYLOAD_LENGTHS) * 50


def ensure_cpu_count():

    LOGGER.info("ensuring CPU count")

    # Execute "lscpu" command.
    popen = subprocess.Popen(
        ["lscpu", "-p=cpu"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    (stdout, ignored_stderr) = popen.communicate()
    return_code = popen.returncode
    if return_code != 0:
        LOGGER.error("unexpected return code (return_code=%d)", return_code)
        sys.stderr.write(stdout)
        sys.exit(return_code)

    # Verify "lscpu" output.
    cpu_count = int(stdout.strip().split('\n')[-1])
    if cpu_count < CPU_COUNT:
        LOGGER.error("CPU count mismatch (expected=%d, actual=%d)", CPU_COUNT, cpu_count)
        sys.exit(1)


def run_benchmarks():
    LOGGER.info("starting benchmarks...")
    start_instant_seconds = time.time()
    for message_count in MESSAGE_COUNTS:
        for payload_length in PAYLOAD_LENGTHS:
            server_ctx = start_server(message_count, payload_length)
            try:
                for concurrency in CONCURRENCIES:
                    run_client(message_count, payload_length, concurrency)
            finally:
                stop_server(server_ctx)
    stop_instant_seconds = time.time()
    duration_seconds = stop_instant_seconds - start_instant_seconds
    LOGGER.info("benchmarks completed (duration_seconds=%.1f)", duration_seconds)


def get_server_output_filepath(message_count, payload_length):
    return os.path.join(
        BENCHMARK_DIR,
        "out",
        "server-m{}-l{}.out".format(message_count, payload_length))


def start_server(message_count, payload_length):

    # Execute "mvn" command.
    LOGGER.info(
        "starting server (message_count=%d, payload_length=%d)",
        message_count, payload_length)
    output_filepath = get_server_output_filepath(message_count, payload_length)
    output_stream = open(output_filepath, "w")
    popen = subprocess.Popen(
        ["taskset",
         "-c", "0",
         "mvn", "exec:java",
         "-Dexec.classpathScope=test",
         "-Dexec.mainClass=com.vlkan.pubsub.BenchmarkServer",
         "-Dbenchmark.messageCount={}".format(message_count),
         "-Dbenchmark.payloadLength={}".format(payload_length)],
        cwd=PROJECT_DIR,
        env=MVN_ENV,
        stdout=output_stream,
        stderr=subprocess.STDOUT)

    # Create the context.
    ctx = {
        "output_stream": output_stream,
        "popen": popen
    }

    # Wait for the server start.
    verified_start = wait_file_lines_for_pattern(
        output_filepath,
        "^.*started server.*$",
        2 * 60,
        5)
    if not verified_start:
        LOGGER.error("failed to verify server start")
        stop_server(ctx)
        sys.exit(1)

    # Return the context.
    LOGGER.info("server is started")
    return ctx


def wait_file_lines_for_pattern(filepath, pattern, timeout_millis, retry_millis):
    start_instant_millis = time.time()
    while (time.time() - start_instant_millis) < timeout_millis:
        for line in open(filepath):
            if re.match(pattern, line.strip()):
                return True
        time.sleep(retry_millis)
    return False


def stop_server(server_ctx):
    LOGGER.info("stopping server...")
    server_ctx["popen"].terminate()
    server_ctx["popen"].communicate()
    server_ctx["output_stream"].close()


def get_client_output_filepath(message_count, payload_length, concurrency):
    return os.path.join(
        BENCHMARK_DIR,
        "out",
        "client-m{}-l{}-c{}.out".format(
            message_count, payload_length, concurrency))


def run_client(message_count, payload_length, concurrency):

    # Execute "mvn" command.
    pull_count = TOTAL_PAYLOAD_LENGTH / (message_count * payload_length)
    LOGGER.info(
        "starting client (concurrency=%d, pull_count=%d)",
        concurrency, pull_count)
    output_filepath = get_client_output_filepath(
        message_count, payload_length, concurrency)
    with open(output_filepath, "w") as output_stream:
        taskset_cpus = "1-2" if concurrency > 1 else "1"
        popen = subprocess.Popen(
            ["taskset",
             "-c", taskset_cpus,
             "mvn", "exec:java",
             "-Dexec.classpathScope=test",
             "-Dexec.mainClass=com.vlkan.pubsub.BenchmarkClient",
             "-Dbenchmark.concurrency={}".format(concurrency),
             "-Dbenchmark.pullCount={}".format(pull_count)],
            cwd=PROJECT_DIR,
            env=MVN_ENV,
            stdout=output_stream,
            stderr=subprocess.STDOUT)

        # Verify completion.
        LOGGER.info("waiting client completion")
        popen.communicate()
        return_code = popen.returncode
        if return_code != 0:
            LOGGER.error("unexpected return code (return_code=%d)", return_code)
            sys.exit(return_code)

    # Extract the benchmark time.
    LOGGER.info("extracting benchmark time")
    benchmark_time = read_client_benchmark_time(output_filepath)
    if not benchmark_time:
        LOGGER.error("failed to extract benchmark time, client output:")
        sys.exit(1)


def read_client_benchmark_time(output_filepath):
    for line in open(output_filepath):
        match = re.match(
            "^.*\[benchmark\] pulled and ack'ed [0-9]+ messages in ([0-9]+) ms$",
            line.strip())
        if match:
            return long(match.groups()[0])


def load_results():
    LOGGER.info("loading results...")
    benchmark_time_by_concurrency_by_payload_length_by_message_count = {}
    for message_count in MESSAGE_COUNTS:
        benchmark_time_by_concurrency_by_payload_length = \
            benchmark_time_by_concurrency_by_payload_length_by_message_count[message_count] = \
            {}
        for payload_length in PAYLOAD_LENGTHS:
            benchmark_time_by_concurrency = \
                benchmark_time_by_concurrency_by_payload_length[payload_length] = \
                {}
            for concurrency in CONCURRENCIES:
                client_output_file = get_client_output_filepath(message_count, payload_length, concurrency)
                benchmark_time_by_concurrency[concurrency] = read_client_benchmark_time(client_output_file)
    return benchmark_time_by_concurrency_by_payload_length_by_message_count


def report_results(benchmark_time_by_concurrency_by_payload_length_by_message_count):

    LOGGER.info("plotting results...")

    # Determine min/max benchmark times.
    min_benchmark_time = 99999999   # Too big to be true.
    max_benchmark_time = 00000000   # Too small to be true.
    for message_count in MESSAGE_COUNTS:
        for payload_length in PAYLOAD_LENGTHS:
            for concurrency in CONCURRENCIES:
                benchmark_time = \
                    benchmark_time_by_concurrency_by_payload_length_by_message_count[message_count][payload_length][concurrency]
                if benchmark_time < min_benchmark_time:
                    min_benchmark_time = benchmark_time
                min_benchmark_time = min(min_benchmark_time, benchmark_time)
                max_benchmark_time = max(max_benchmark_time, benchmark_time)

    gnuplot_img_file = report_results_in_gnuplot(
        benchmark_time_by_concurrency_by_payload_length_by_message_count,
        min_benchmark_time,
        max_benchmark_time)

    report_results_in_html(
        benchmark_time_by_concurrency_by_payload_length_by_message_count,
        min_benchmark_time,
        max_benchmark_time,
        gnuplot_img_file)


def report_results_in_gnuplot(
        benchmark_time_by_concurrency_by_payload_length_by_message_count,
        min_benchmark_time,
        max_benchmark_time):

    # Dump CSV file.
    LOGGER.info("reporting results via Gnuplot...")
    csv_file = os.path.join(BENCHMARK_DIR, "results.csv")
    with open(csv_file, "w") as csv_file_handle:
        csv_file_handle.write("""#message_count,payload_length,concurrency,benchmark_time
""")
        for message_count in MESSAGE_COUNTS:
            for payload_length in PAYLOAD_LENGTHS:
                for concurrency in CONCURRENCIES:
                    benchmark_time = \
                        benchmark_time_by_concurrency_by_payload_length_by_message_count[message_count][payload_length][concurrency]
                    csv_file_handle.write("""{},{},{},{}
""".format(message_count, payload_length, concurrency, benchmark_time))

    # Dump Gnuplot file.
    gnuplot_file = os.path.join(BENCHMARK_DIR, "results.gnuplot")
    gnuplot_img_file = os.path.join(BENCHMARK_DIR, "results.png")
    with open(gnuplot_file, "w") as gnuplot_file_handle:

        # Write header.
        gnuplot_file_handle.write("""
set terminal pngcairo size {},300 enhanced font 'Verdana,8';
set output '{}';
set datafile separator ',';
set grid;
set xtics;
set ytics;
set xlabel 'concurrency';
set ylabel 'time (sec)';
set origin 0,0;
set xrange [{}:{}];
set yrange [{}:{}];
set multiplot layout 1,{};
""".format(
            150 * len(PAYLOAD_LENGTHS),
            gnuplot_img_file,
            CONCURRENCIES[0] - 1,
            CONCURRENCIES[-1] + 1,
            ((min_benchmark_time / 1000.0) - 1),
            ((max_benchmark_time / 1000.0) + 1),
            len(PAYLOAD_LENGTHS)))

        # Write plots.
        for payload_length in PAYLOAD_LENGTHS:
            if payload_length != PAYLOAD_LENGTHS[0]:
                gnuplot_file_handle.write("""
set xlabel ' ';
unset ylabel;""")
            title = "{:,d} KiB".format(payload_length / 1024)
            gnuplot_file_handle.write("""
set title '{}';
plot '{}' using ($2=={}?$3:1/0):($4/1000.0):($1/4000.0) with points linestyle 6 notitle
""".format(title, os.path.basename(csv_file), payload_length))

    # Execute gnuplot.
    popen = subprocess.Popen(
        ["gnuplot", gnuplot_file],
        cwd=BENCHMARK_DIR)

    # Verify gnuplot completion.
    popen.communicate()
    return_code = popen.returncode
    if return_code != 0:
        LOGGER.error("unexpected return code (return_code=%d)", return_code)
        sys.exit(return_code)

    return gnuplot_img_file


def report_results_in_html(
        benchmark_time_by_concurrency_by_payload_length_by_message_count,
        min_benchmark_time,
        max_benchmark_time,
        gnuplot_img_file):
    LOGGER.info("reporting results in HTML...")

    # Determine the best configuration.
    best_message_count = best_payload_length = best_concurrency = None
    for message_count in MESSAGE_COUNTS:
        for payload_length in PAYLOAD_LENGTHS:
            for concurrency in CONCURRENCIES:
                benchmark_time = \
                    benchmark_time_by_concurrency_by_payload_length_by_message_count[message_count][payload_length][concurrency]
                if benchmark_time == min_benchmark_time:
                    best_message_count = message_count
                    best_payload_length = payload_length
                    best_concurrency = concurrency
                    break

    # Dump the HTML file.
    html_file = os.path.join(BENCHMARK_DIR, "results.html")
    with open(html_file, "w") as html_file_handle:
        total_payload_length_mib = TOTAL_PAYLOAD_LENGTH / float(1024**2)
        html_file_handle.write("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>reactor-pubsub Benchmark Results</title>
</head>
<body>
    <style>
        .results th, .results td { padding: 0.3em }
        .results td { text-align: right }
        .results td.benchmark_time_bar { text-align: left }
        .results th { background-color: #cfcfcf }
    </style>
""")
        best_payload_length_kib = best_payload_length / 1024
        html_file_handle.write("""
    <p>
        Timing results for retrieving (pull &amp; ack) a total payload of
        {:,.0f} MiB using different pull batch size, pull concurrency, and
        message payload lengths.
    </p>
    <p>
        Best result is {:,.0f} ms achieved by pull batch size {:,.0f}, pull
        concurrency {}, and message payload length {:,.0f} KiB.
    </p>
""".format(
            total_payload_length_mib,
            min_benchmark_time,
            best_message_count,
            best_concurrency,
            best_payload_length_kib))
        html_file_handle.write("""
    <dl>
        <dt>CPU</dt>
        <dd>Intel i7 2.70GHz (x86-64, confined <code>java</code> process to 2 cores
        using <a href="http://www.man7.org/linux/man-pages/man1/taskset.1.html"
        ><code>taskset -c 0</code></a>)</dd>
        <dt>JVM</dt>
        <dd>OpenJDK 64-Bit (AdoptOpenJDK, build 25.232-b09) with flags
        <code>{}</code></dd>
        <dt>OS</dt>
        <dd>Xubuntu 18.04.3 (4.15.0-70-generic, x86-64)</dd>
    </dl>
""".format(MVN_ENV["MAVEN_OPTS"]))
        html_file_handle.write("""
    <h1>Combined Results</h1>
    <img src="{}"/>
""".format(os.path.basename(gnuplot_img_file)))
        for payload_length in PAYLOAD_LENGTHS:
            payload_length_kib = payload_length / 1024
            html_file_handle.write("""
    <div class="results">
        <h1>Results for messages of size {} KiB</h1>
        <table>
            <thead>
                <tr>
                    <th>Batch Size</th>
                    <th>Concurrency</th>
                    <th colspan="2">Time (ms)</th>
                </tr>
            </thead>
            <tbody>""".format(payload_length_kib))
            for message_count in MESSAGE_COUNTS:
                first_row = True
                for concurrency in CONCURRENCIES:
                    benchmark_time = \
                        benchmark_time_by_concurrency_by_payload_length_by_message_count[message_count][payload_length][concurrency]
                    if first_row:
                        first_row = False
                        message_count_column = \
                            """<td class="message_count" rowspan={}>{}</td>""".format(
                                len(CONCURRENCIES),
                                "{:,.0f}".format(message_count))
                    else:
                        message_count_column = ""
                    normalized_benchmark_time = \
                        (benchmark_time - min_benchmark_time) / float(max_benchmark_time)
                    benchmark_time_bar = "▉" * (1 + int(19 * normalized_benchmark_time)) + " ({:.0f}%)".format(100 * normalized_benchmark_time)
                    html_file_handle.write("""
                <tr>
                    {}
                    <td class="concurrency">{}</td>
                    <td class="benchmark_time">{}</td>
                    <td class="benchmark_time_bar">{}</td>
                </tr>
""".format(
                        message_count_column,
                        concurrency,
                        "{:,.0f}".format(benchmark_time),
                        benchmark_time_bar))
            html_file_handle.write("""
            </tbody>
        </table>
""")
        html_file_handle.write("""
</body>
</html>
""")


def load_and_report_results():
    benchmark_time_by_concurrency_by_payload_length_by_message_count = load_results()
    report_results(benchmark_time_by_concurrency_by_payload_length_by_message_count)


def main():
    args = sys.argv
    if len(args) != 2:
        print >>sys.stderr, "usage: {} <run|report>".format(args[0])
        sys.exit(1)
    try:
        if args[1] == "run":
            ensure_cpu_count()
            run_benchmarks()
        elif args[1] == "report":
            load_and_report_results()
        else:
            print >>sys.stderr, "invalid argument: {}".format(args[1])
            sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    main()
