#include <iostream>
#include <string.h>
#include <thread>
#include <chrono>
#include "tioclient.h"
#include "tioclient.hpp"
#include "gmock/gmock.h"

TEST(SubscriberTest, ReceiveCallbackFromSubscription)
{
	tio::Connection cn("127.0.0.1");
	tio::containers::list<std::string> listContainer;

	listContainer.create(&cn, "test_subscribe_container", "volatile_list");
	listContainer.clear();

	uint16_t eventsArrivedCount{};

	listContainer.subscribe(
	[&eventsArrivedCount](const std::string& containerName, const std::string& eventName, const int& key, const std::string& value)
	{
		eventsArrivedCount++;
	});

	const uint16_t expectedEventsArrivedCount = 5;

	for (uint16_t i = 1; i <= expectedEventsArrivedCount; ++i)
	{
		listContainer.push_back("v_" + std::to_string(i));
	}

	cn.WaitForNextEventAndDispatch(1);

	cn.Disconnect();

	ASSERT_THAT(eventsArrivedCount, testing::Eq(expectedEventsArrivedCount));
}

int main(int argc, char** argv)
{
	testing::InitGoogleMock(&argc, argv);

	return RUN_ALL_TESTS();
}