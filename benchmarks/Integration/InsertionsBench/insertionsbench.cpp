#include "benchmark/benchmark.h"
#include "tioclient.h"
#include "tioclient.hpp"
#include <chrono>
#include <thread>
#include <iostream>


static void BM_InsertionsOnListWithOneClient(benchmark::State& state)
{
	tio::Connection cn("127.0.0.1");
	tio::containers::list<std::string> listContainer;

	listContainer.create(&cn, "test_insertions_1client", "volatile_list");
	listContainer.clear();

	// work loop
	for (auto _ : state) {
		for (uint32_t i = 0; i < state.range(0); ++i)
		{
			listContainer.push_back("v_" + std::to_string(i));
		}
	}

	auto insertedItems = int64_t(state.iterations()) * state.range(0);

	if (listContainer.size() != insertedItems)
	{
		state.SkipWithError("We lost records!");
	}

	//
	// Quantity of inserted items * iterations = e.g 8 * 1948
	//
	state.counters["inserted_items"] =
		benchmark::Counter(
			state.range(0),
			benchmark::Counter::kIsIterationInvariant);

	//
	// Quantity of inserted items per second
	//
	state.counters["inserted_items_per_second"] =
		benchmark::Counter(
			state.range(0),
			benchmark::Counter::kIsIterationInvariantRate);
	
	cn.Disconnect();
}
BENCHMARK(BM_InsertionsOnListWithOneClient)->Arg(1);
//->RangeMultiplier(2)
//->Range(8, 8 << 10);

static void BM_InsertionsOnListWithTwoClients(benchmark::State& state)
{
	tio::Connection cn("127.0.0.1");
	tio::containers::list<std::string> listContainer;

	listContainer.create(&cn, "test_insertions_2client", "volatile_list");

	if (state.thread_index == 0) {
		listContainer.clear();
	}

	// work loop
	for (auto _ : state) {
		for (uint32_t i = 0; i < state.range(0); ++i)
		{
			listContainer.push_back("v_" + std::to_string(i));
		}
	}

	//
	// Quantity of inserted items * iterations = e.g 8 * 1948
	//
	state.counters["inserted_items"] =
		benchmark::Counter(
			state.range(0),
			benchmark::Counter::kIsIterationInvariant);

	//
	// Quantity of inserted items per second
	//
	state.counters["inserted_items_per_second"] =
		benchmark::Counter(
			state.range(0),
			benchmark::Counter::kIsIterationInvariantRate);

	cn.Disconnect();
}

BENCHMARK(BM_InsertionsOnListWithTwoClients)
->Threads(2)
->RangeMultiplier(2)
->Range(8, 8 << 10);


BENCHMARK_MAIN();