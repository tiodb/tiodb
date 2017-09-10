#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>

using std::cout;
using std::endl;
using std::flush;
using std::vector;
using std::list;
using std::back_inserter;
using std::string;
using std::shared_ptr;
using std::make_shared;
using std::mutex;
using std::lock_guard;
using std::tuple;
using std::function;
using std::initializer_list;

using std::thread;
using std::atomic;
namespace chrono = std::chrono;

typedef char byte_t;

void* tio_alloc(size_t size)
{
    return nullptr;
}


template<template<class...> class ContainerT = std::vector, typename mutex_t = std::mutex>
class TioTransactionLog
{
public:
    typedef tuple<void*, size_t> value_type;
private:
    ContainerT<value_type> items_;
    mutex_t mutex_;
public:

     tuple<void*, size_t> Add(size_t itemSize)
     {
         auto item = value_type{tio_alloc(itemSize), itemSize};

         {
             lock_guard<mutex_t> lock(mutex_);
             items_.push_back(item);
         }

         return item;
     }

    void Clear()
    {
        items_.clear();
    }
};


class LocklessAppendOnlyList
{
private:
    struct node
    {
        int id;
        tuple<void*, size_t> data;
        std::atomic<node*> next;

        explicit node(int id)
            : id{id}
            , next{nullptr}
            , data{nullptr, 0}
        {}


        explicit node(int id, size_t dataSize)
            : id{id}
            , next{nullptr}
            , data{tio_alloc(dataSize), dataSize}
        { }
    };

    node head_{0};
    std::atomic<node*> tailHint_{&head_};

public:
    tuple<void*, size_t> Add(size_t dataSize)
    {
        node* tail = tailHint_;
        node* expected = nullptr;

        node* newNode = new node(0, dataSize);

        while (!tail->next.compare_exchange_weak(expected, newNode))
        {
            if (expected)
            {
                tail = expected;
                expected = nullptr;
            }
        }

        tailHint_ = newNode;

        return newNode->data;
    }

    void Clear()
    {
        head_.next = nullptr;
        tailHint_ = &head_;
    }
};
class StopWatch
{
    chrono::high_resolution_clock::time_point start_;
public:
    void Start()
    {
        start_ = chrono::high_resolution_clock::now();
    }

    long long int ElapsedInMilliseconds()
    {
        auto elapsed = chrono::high_resolution_clock::now() - start_;
        return chrono::duration_cast<chrono::milliseconds>(elapsed).count();
    }
};

class TioTestRunner
{
public:
    typedef function<void(void)> TestFunctionT;

private:
    vector<TestFunctionT> tests_;
    bool running_;

    vector<thread> threads_;
    atomic<unsigned> finishedThreads_;

public:

    TioTestRunner()
        : running_(false)
    {
        reset();
    }

    void reset()
    {
        tests_.clear();
        running_ = false;
        finishedThreads_ = 0;
    }

    void AddThread(const TestFunctionT &f)
    {
        tests_.push_back(f);
    }

    void AddThreads(const TestFunctionT &f, size_t count)
    {
        for(decltype(count) a = 0 ; a < count ; a++) {
            tests_.push_back(f);
        }
    }

    void AddThreads(const initializer_list<TestFunctionT> &f)
    {
        std::copy(cbegin(f), cend(f), back_inserter(tests_));
    }

    void join()
    {
        for (auto& t : threads_)
            t.join();

        reset();
    }

    void run()
    {
        start();
        join();
    }

    bool finished() const
    {
        return finishedThreads_ == threads_.size();
    }

    void start()
    {
        for (auto f : tests_)
        {
            threads_.emplace_back(
                thread(
                    [f, this]()
                    {
                        while (!running_)
                            std::this_thread::yield();

                        try
                        {
                            f();
                        }
                        catch (std::exception& ex)
                        {
#ifdef _WIN32
							//__debugbreak
#else
                            __builtin_debugtrap();
#endif

                        }

                        finishedThreads_++;
                    }
                ));
        }

        running_ = true;
    }
};

void RunFunctionAndReport(
    const string& testName,
    const function<void(size_t)>& testFunction,
    const function<void(void)>& resetFunction,
    unsigned totalItemCount,
    size_t minThread,
    size_t maxThreads)
{
    cout << "==== " << testName << " ====" << endl;

    for(size_t a = minThread ; a <= maxThreads ; a++)
    {
        StopWatch stopWatch;
        TioTestRunner testRunner;
        auto itemsPerThread = totalItemCount / a;

        testRunner.AddThreads(
            [&]()
            {
                testFunction(itemsPerThread);
            },
            a);

        cout << "threads=" << a << "," << flush;

        stopWatch.Start();
        testRunner.start();
        testRunner.join();

        auto elapsed = stopWatch.ElapsedInMilliseconds();
        auto persec = (totalItemCount * 1000) / elapsed;

        cout << "elapsed(ms)=" << elapsed << "ms, persec=" << persec << endl;

    }
}


struct not_a_mutex
{
    void lock(){}
    void unlock(){}

};

int main()
{
    StopWatch stopWatch;

    static const unsigned MAX_THREAD_COUNT = 4;
    static const unsigned TOTAL_ITEM_COUNT = 50 * 1000 * 1000;
    static const size_t ITEM_SIZE = 8;

    auto justAddItemsFunction = [](auto* collection, size_t itemCount)
    {
        for (auto a = 0; a < itemCount; a++) {
            auto[buffer, size] = collection->Add(ITEM_SIZE);
        }
    };

    //
    // Vector without lock
    //
    {
        TioTransactionLog<vector, not_a_mutex> vectorWithoutLock;

        RunFunctionAndReport(
            "Vector with no lock",
            [&](size_t itemCount)
            {
                justAddItemsFunction(&vectorWithoutLock, itemCount);
            },
            [&]()
            { vectorWithoutLock.Clear(); },
            TOTAL_ITEM_COUNT,
            1,
            1);
    }

    //
    // List without lock
    //
    {
        TioTransactionLog<list, not_a_mutex> listNoLock;

        RunFunctionAndReport(
            "List with no lock",
            [&](size_t itemCount)
            {
                justAddItemsFunction(&listNoLock, itemCount);
            },
            [&]()
            { listNoLock.Clear(); },
            TOTAL_ITEM_COUNT,
            1,
            1);
    }

    //
    // lockless
    //
    LocklessAppendOnlyList locklessAppendOnlyList;

    RunFunctionAndReport(
        "LocklessAppendOnlyList",
        [&](size_t itemCount)
        {
            justAddItemsFunction(&locklessAppendOnlyList, itemCount);
        },
        [&](){locklessAppendOnlyList.Clear();},
        TOTAL_ITEM_COUNT,
        1,
        MAX_THREAD_COUNT);

    //
    // Vector with mutex
    //
    TioTransactionLog<vector> vectorAndMutex;

    RunFunctionAndReport(
        "Vector and mutex",
        [&](size_t itemCount)
        {
            justAddItemsFunction(&vectorAndMutex, itemCount);
        },
        [&](){vectorAndMutex.Clear();},
        TOTAL_ITEM_COUNT,
        1,
        MAX_THREAD_COUNT);



    return 0;
}