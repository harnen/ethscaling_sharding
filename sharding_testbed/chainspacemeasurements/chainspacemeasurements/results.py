import numpy


def parse_shard_results(results):
    final_result = []
    for shard in results:
        sum_set = []
        for tps_set in shard:
            sum_set.append(sum(tps_set))

        mean = numpy.mean(sum_set)
        sd = numpy.std(sum_set)

        #trimmed_sum_set = []
        #for s in sum_set:
        #    if mean + sd*2 > s and mean - sd*2 < s:
        #        trimmed_sum_set.append(s)
        #mean = numpy.mean(trimmed_sum_set)

        final_result.append((mean, sd))

    return final_result


def parse_client_latency_results(results):
    final_result = []
    for tps in results:
        latencies = []
        for latency_set in tps:
            latencies += latency_set
        latencies = sorted(latencies)
        final_result.append(latencies)

    return final_result


def parse_client_latency2_results(results):
    final_result = []
    for tps in results:
        latencies = []
        for latency_set in tps:
            latencies += latency_set

        median = numpy.median(latencies)
        sd = numpy.std(latencies)

        final_result.append((median, numpy.percentile(latencies, 26), numpy.percentile(latencies, 74)))

    return final_result
