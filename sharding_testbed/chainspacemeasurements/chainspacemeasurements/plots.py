import json
import sys

from matplotlib import pyplot
from matplotlib import markers
from matplotlib.ticker import FuncFormatter

from chainspacemeasurements.results import parse_shard_results, parse_client_latency_results, parse_client_latency2_results


def plot_shard_scaling(results, outfile):
    parsed_results = parse_shard_results(results)
    pyplot.xlabel('Number of shards')
    pyplot.ylabel('Average transactions / second')
    pyplot.grid(True)

    pyplot.errorbar(
        range(2, len(parsed_results)+2),
        [i[0] for i in parsed_results],
        [i[1] for i in parsed_results],
        marker='o',
        #color='black',
    )

    pyplot.savefig(outfile)
    pyplot.close()


def plot_shard_scaling2(results1, results2, outfile):
    parsed_results1 = parse_shard_results(results1)
    parsed_results2 = parse_shard_results(results2)
    pyplot.xlabel('Number of shards')
    pyplot.ylabel('Average transactions / second')
    pyplot.grid(True)

    pyplot.errorbar(
        [i for i in range(2, len(parsed_results1)+2)],
        [i[0] for i in parsed_results1],
        [0 for i in parsed_results1],
        marker='o',
        color='C0',
        label='1 input'
    )

    pyplot.errorbar(
        [i-0.03 for i in range(2, len(parsed_results1)+2)],
        [i[0] for i in parsed_results1],
        [i[1] for i in parsed_results1],
        color='C0',
        fmt=''
    )

    pyplot.errorbar(
        [i for i in range(2, len(parsed_results2)+2)],
        [i[0] for i in parsed_results2],
        [0 for i in parsed_results2],
        marker='s',
        color='C1',
        label='2 inputs'
    )

    pyplot.errorbar(
        [i+0.03 for i in range(2, len(parsed_results2)+2)],
        [i[0] for i in parsed_results2],
        [i[1] for i in parsed_results2],
        color='C1',
        fmt=''
    )

    pyplot.legend(loc=4)
    pyplot.savefig(outfile)
    pyplot.close()


def plot_shard_scaling3(results1, results2, outfile):
    parsed_results1 = parse_shard_results(results1)
    parsed_results2 = parse_shard_results(results2)
    pyplot.xlabel('Number of shards')
    pyplot.ylabel('Transactions / second')
    pyplot.grid(True)

    pyplot.errorbar(
        [i for i in range(2, len(parsed_results1)+2)],
        [i[0] for i in parsed_results1],
        [0 for i in parsed_results1],
        marker='o',
        color='C0',
        label='Without defenses'
    )

    pyplot.errorbar(
        [i-0.01 for i in range(2, len(parsed_results1)+2)],
        [i[0] for i in parsed_results1],
        [i[1] for i in parsed_results1],
        color='C0',
        fmt=''
    )

    pyplot.errorbar(
        [i for i in range(2, len(parsed_results2)+2)],
        [i[0] for i in parsed_results2],
        [0 for i in parsed_results2],
        marker='s',
        color='C1',
        label='With defenses'
    )

    pyplot.errorbar(
        [i+0.01 for i in range(2, len(parsed_results2)+2)],
        [i[0] for i in parsed_results2],
        [i[1] for i in parsed_results2],
        color='C1',
        fmt=''
    )

    pyplot.locator_params(nbins=len(parsed_results1))
    pyplot.legend(loc=4)
    pyplot.savefig(outfile)
    pyplot.close()


def plot_input_scaling(results, outfile):
    parsed_results = parse_shard_results(results)
    pyplot.xlabel('Number of inputs per transaction')
    pyplot.ylabel('Transactions / second')
    pyplot.grid(True)

    pyplot.errorbar(
        range(1, len(parsed_results)+1),
        [i[0] for i in parsed_results],
        [i[1] for i in parsed_results],
        marker='o',
    )

    pyplot.savefig(outfile)
    pyplot.close()


def plot_bano(results, outfile):
    parsed_results = parse_shard_results(results)
    pyplot.xlabel('Number of dummy inputs per transaction')
    pyplot.ylabel('Transactions / second')
    pyplot.grid(True)

    pyplot.errorbar(
        range(1, len(parsed_results)+1),
        [i[0] for i in parsed_results],
        [i[1] for i in parsed_results],
        marker='o',
    )

    pyplot.locator_params(nbins=len(parsed_results))
    pyplot.savefig(outfile)
    pyplot.close()


def plot_input_scaling2(results1, results2, outfile):
    parsed_results1 = parse_shard_results(results1)
    parsed_results2 = parse_shard_results(results2)
    pyplot.xlabel('Number of inputs per transaction')
    pyplot.ylabel('Transactions / second')
    pyplot.grid(True)

    pyplot.errorbar(
        [i for i in range(1, len(parsed_results1)+1)],
        [i[0] for i in parsed_results1],
        [0 for i in parsed_results1],
        marker='o',
        color='C0',
        label='Without defenses'
    )

    pyplot.errorbar(
        [i-0.03 for i in range(1, len(parsed_results1)+1)],
        [i[0] for i in parsed_results1],
        [i[1] for i in parsed_results1],
        color='C0',
        fmt=''
    )

    pyplot.errorbar(
        [i for i in range(1, len(parsed_results2)+1)],
        [i[0] for i in parsed_results2],
        [0 for i in parsed_results2],
        marker='s',
        color='C1',
        label='With defenses'
    )

    pyplot.errorbar(
        [i+0.03 for i in range(1, len(parsed_results2)+1)],
        [i[0] for i in parsed_results2],
        [i[1] for i in parsed_results2],
        color='C1',
        fmt=''
    )

    pyplot.legend(loc=4)
    pyplot.savefig(outfile)
    pyplot.close()


def plot_node_scaling(results, outfile, step):
    parsed_results = parse_shard_results(results)
    pyplot.xlabel('Number of replicas per shard')
    pyplot.ylabel('Average transactions / second')
    pyplot.grid(True)

    pyplot.errorbar(
        range(4, 4+len(parsed_results)*step, step),
        [i[0] for i in parsed_results],
        [i[1] for i in parsed_results],
        marker='o',
    )

    pyplot.savefig(outfile)
    pyplot.close()


def plot_client_latency(results, outfile, start_tps, step):
    parsed_results = parse_client_latency_results(results)
    pyplot.xlabel('Client-perceived latency (ms)')
    pyplot.ylabel('Probability')
    pyplot.grid(True)

    for i, tps in enumerate(parsed_results):
        tps = [x*1000 for x in tps]
        pyplot.plot(
            tps,
            [j/float(len(tps)) for j in range(len(tps))],
            label=str(start_tps+i*step) + ' t/s',
            marker=markers.MarkerStyle.filled_markers[i],
            markevery=500,
        )

    pyplot.legend()
    pyplot.savefig(outfile)
    pyplot.close()


def plot_client_latency2(results1, results2, outfile, start_tps, step):
    parsed_results1 = parse_client_latency2_results(results1)
    parsed_results2 = parse_client_latency2_results(results2)
    pyplot.xlabel('Transactions / second')
    pyplot.ylabel('Client-perceived latency (ms)')
    pyplot.grid(True)

    pyplot.errorbar(
        [i for i in range(start_tps, len(parsed_results1)*step+start_tps, step)],
        [i[0] for i in parsed_results1],
        [0 for i in parsed_results1],
        marker='o',
        color='C0',
        label='Without defenses'
    )

    pyplot.errorbar(
        [i-0.5 for i in range(start_tps, len(parsed_results1)*step+start_tps, step)],
        [i[0] for i in parsed_results1],
        yerr=([i[1] for i in parsed_results1], [i[2] for i in parsed_results1]),
        color='C0',
        fmt=''
    )

    pyplot.errorbar(
        [i for i in range(start_tps, len(parsed_results1)*step+start_tps, step)],
        [i[0] for i in parsed_results2],
        [0 for i in parsed_results2],
        marker='s',
        color='C1',
        label='With defenses'
    )

    pyplot.errorbar(
        [i+0.5 for i in range(start_tps, len(parsed_results1)*step+start_tps, step)],
        [i[0] for i in parsed_results2],
        [i[1] for i in parsed_results2],
        color='C1',
        fmt=''
    )

    pyplot.legend(loc=4)
    pyplot.savefig(outfile)
    pyplot.close()


if __name__ == '__main__':
    if sys.argv[1] == 'shardscaling':
        results = json.loads(open(sys.argv[2]).read())
        plot_shard_scaling(results, sys.argv[3])
    elif sys.argv[1] == 'shardscaling2':
        results1 = json.loads(open(sys.argv[2]).read())
        results2 = json.loads(open(sys.argv[3]).read())
        plot_shard_scaling2(results1, results2, sys.argv[4])
    elif sys.argv[1] == 'shardscaling3':
        results1 = json.loads(open(sys.argv[2]).read())
        results2 = json.loads(open(sys.argv[3]).read())
        plot_shard_scaling3(results1, results2, sys.argv[4])
    elif sys.argv[1] == 'inputscaling':
        results = json.loads(open(sys.argv[2]).read())
        plot_input_scaling(results, sys.argv[3])
    elif sys.argv[1] == 'inputscaling2':
        results1 = json.loads(open(sys.argv[2]).read())
        results2 = json.loads(open(sys.argv[3]).read())
        plot_input_scaling2(results1, results2, sys.argv[4])
    elif sys.argv[1] == 'bano':
        results = json.loads(open(sys.argv[2]).read())
        plot_bano(results, sys.argv[3])
    elif sys.argv[1] == 'nodescaling':
        results = json.loads(open(sys.argv[2]).read())
        plot_node_scaling(results, sys.argv[3], int(sys.argv[4]))
    elif sys.argv[1] == 'clientlatency':
        results = json.loads(open(sys.argv[2]).read())
        plot_client_latency(results, sys.argv[3], int(sys.argv[4]), int(sys.argv[5]))
    elif sys.argv[1] == 'clientlatency2':
        results1 = json.loads(open(sys.argv[2]).read())
        results2 = json.loads(open(sys.argv[3]).read())
        plot_client_latency2(results1, results2, sys.argv[4], int(sys.argv[5]), int(sys.argv[6]))
